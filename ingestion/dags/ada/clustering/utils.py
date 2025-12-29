import numpy as np
import importlib
import pandas as pd
from copy import deepcopy
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import (
    silhouette_score,
    calinski_harabasz_score,
    davies_bouldin_score,
)
from pathlib import Path
import json

BASE_DIR = Path(__file__).resolve(strict=True).parent
JSONFILE = BASE_DIR / "clustering_algorithms.json"

# define the silhouette_score that can be passed to the grid as a error calc


def cv_silhouette_scorer(estimator, df):
    estimator.fit(df)
    cluster_labels = estimator.labels_
    num_labels = len(set(cluster_labels))
    num_samples = len(df.index)
    if num_labels == 1 or num_labels == num_samples:
        return -1
    else:
        return silhouette_score(df, cluster_labels)


# return list if arg is not None


def coerce_list(*args):
    lst = []
    for i in args:
        if i:
            lst = lst + i
    return lst


# extract the key and value from a dictionary


def get_dict_key_val(d):
    return list(d.keys())[0], list(d.values())[0]


# read all the available algorithms from the json file


def get_all_algorithms():
    try:
        return pd.read_json(JSONFILE, orient="index")
    except ValueError as err:
        print(f"Error reading the clustering algorithms json file. {err}")


# prepare the list of grid that needs to run based on the inputs


def get_all_grids(
    n_clusters=None, selected_algorithms=None, algorithms_df=get_all_algorithms(), parameters=None
):

    algorithm_grids = {}

    if selected_algorithms:
        algorithms_df = algorithms_df.loc[selected_algorithms, :]
        if parameters:
            algorithms_df["parameters"] = [
                json.dumps(parameters.get(algo, eval(algorithms_df.loc[algo, "parameters"])))
                for algo in algorithms_df.index
            ]
    print(algorithms_df)
    if n_clusters:
        algorithms_df = algorithms_df.loc[
            (algorithms_df["k_as_input"] == True)
            & (algorithms_df["default_method"] == True)
        ]

    default_algorithms = algorithms_df.loc[algorithms_df["default_method"] == True]
    custom_algorithms = algorithms_df.loc[algorithms_df["default_method"] == False]

    for index in default_algorithms.index.to_list():
        module_name, class_name = default_algorithms.loc[index, "class_path"].rsplit(
            ".", 1
        )
        algo_instance = getattr(importlib.import_module(module_name), class_name)()
        grid = GridSearchCV(
            estimator=algo_instance,
            param_grid=eval(default_algorithms.loc[index, "parameters"]),
            scoring=cv_silhouette_scorer,
            cv=[(slice(None), slice(None))],
            n_jobs=-1,
        )
        print(grid)
        if n_clusters:
            for k in n_clusters:
                grid.param_grid["n_clusters"] = [k]
                algorithm_grids[f"{grid_name(grid)}_{k}"] = deepcopy(grid)
        else:
            algorithm_grids[f"{grid_name(grid)}"] = deepcopy(grid)

    for idx, k_input, param in zip(
        custom_algorithms.index.to_list(),
        custom_algorithms.k_as_input.to_list(),
        custom_algorithms.parameters.to_list(),
    ):
        if k_input:
            cluster_list = eval(param).get("n_clusters")
            for k in cluster_list:
                algorithm_grids[f"{idx.lower()}_{k}"] = f"{idx.lower()}"
        else:
            algorithm_grids[f"{idx.lower()}"] = f"{idx.lower()}"

    return algorithm_grids


# get the name of each grid


def grid_name(grid):
    name = [c.lower() for c in str(grid.estimator) if c.isalnum()]
    name = "".join(name)
    return name


# function to remove a column if the variance of the column is 0


def remove_zero_variance_features_util(df):
    no_var_columns = df.var()[df.var() == 0].index.to_list()
    if no_var_columns:
        df.drop(no_var_columns, axis=1, inplace=True)
    return df


# best algorithm selection based on error for clustering
def select_best_algo_clustering(input_df: pd.DataFrame, labels_df: pd.DataFrame, best_algo_selection_weightage: dict):

    error_metrics_df = pd.DataFrame()

    for algo_index in labels_df.columns:
        # number of clustering lables should be atleast 2, else skip.
        if labels_df[algo_index].nunique() < 2:
            continue
        error_metrics_df.loc[algo_index, "silhouette_score"] = silhouette_score(
            input_df, labels_df[algo_index]
        )
        error_metrics_df.loc[algo_index, "davies_bouldin_score"] = davies_bouldin_score(
            input_df, labels_df[algo_index]
        )
        error_metrics_df.loc[
            algo_index, "calinski_harabasz_score"
        ] = calinski_harabasz_score(input_df, labels_df[algo_index])

    # if there is clustering results(all data is in same cluster), return random result
    if error_metrics_df.empty:
        return algo_index

    # rank the clustering results based on error metrics and choose the best one.
    error_metrics_df["sil_rank"] = pd.DataFrame(
        error_metrics_df["silhouette_score"].rank(method="first")
    )
    error_metrics_df["db_rank"] = pd.DataFrame(
        error_metrics_df["davies_bouldin_score"].rank(method="first", ascending=False)
    )
    error_metrics_df["ch_rank"] = pd.DataFrame(
        error_metrics_df["calinski_harabasz_score"].rank(method="first")
    )
    error_metrics_df["rank_sum"] = (
        error_metrics_df["sil_rank"] * best_algo_selection_weightage["silhouette_score"]
        + error_metrics_df["db_rank"] * best_algo_selection_weightage["davies_bouldin_score"]
        + error_metrics_df["ch_rank"] * best_algo_selection_weightage["calinski_harabasz_score"]
    )
    error_metrics_df["rank"] = pd.DataFrame(
        error_metrics_df["rank_sum"].rank(method="first", ascending=False)
    )

    best_algo = str(error_metrics_df[error_metrics_df["rank"] == 1].index[0])
    return best_algo
