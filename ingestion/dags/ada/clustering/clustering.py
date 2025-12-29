import random
from string import ascii_uppercase
from typing import List, Optional

import elasticapm
import pandas as pd
from ada.clustering.helpers import default_method, faiss_helper, meanshift
from ada.clustering.utils import (coerce_list, get_all_algorithms,
                                  get_all_grids,
                                  remove_zero_variance_features_util,
                                  select_best_algo_clustering)
from ada.preprocessing.preprocessing import preprocessing

from ada.clustering.constants import MIN_SAMPLES_CLUSTERING, mapper
from ada.clustering.models import clustering_input_model


@elasticapm.capture_span("clustering-code")
def clustering(
    inp: dict
):
    inp = clustering_input_model(**inp)

    """ 
    
    This function clusters the given data with either default settings or the with multiple data manipulation settings.

    df : pd.DataFrame
        This is mandatory input dataframe which is expected to be clustered.
        Shape = (samples, features)

    target : List[str]
        Target of the clustering. 
        Example : [product_code, store_code]

    n_clusters : Optional[List], default = None
        n_clustres is used to try the clustering with each k(for all k in n_clusters). When n_clusters is set to None, by default k loops from 2 to 10.
    
    algorithms : Optional[List], default = None
        algorithms expects a list of algorithms(must be one of the below algorithms). Whne algorithms is set to None, all the below algorithms are tried.
        ['kmeans', 'affinitypropagation', 'spectralclustering', 'agglomerativeclustering', 'dbscan', 'birch', 'minibatchkmeans', 'optics', 'kmedoids']
        Further more algorithms can be added by added it to the clustering_algorithms.json file with proper parameters and hyperparameters.

    parameters : Optional[dict]
        Accepts a dictionary with the key as the algorithm and the value as the parameters for the algorithm.
        Example : "parameters": {"kmeans" : {"n_init": [8, 9, 10, 11, 12, 13, 14],"max_iter": [100, 120, 140, 160, 180, 200, 220, 240, 260, 280, 300, 320, 340, 360, 380], "n_clusters": [3,4,5]}}

    output_algo_consistent : bool, default = False
        When this is set to True, the output for each "k" clusters in n_clusters will be consistent(same). This best algorithm 
        Example : When n_clusters = [2,3,4] and algorithms = ['kmeans', 'kmedoids], the total set of clusters available will be (3*2) = 6 as kmeans_2, kmeans_3 ......
        If out of all the 6 set of labels, if kmeans_4 is the best, then the output will be, kmeans_2, kmeans_3 and kmeans_4
        But when this is set to False, then the best algorithm is selected for each k clusters. as best(kmeans_2, kmedoids_2), best(kmeans_3,kmedoids_3), best(kmeans_4, kmedoids_4)
        Now, the output will not be algorithm consistent. As the output can be kmeans_2, kmedoids_3, kmedoids_4
    
    reassign_labels_feature : Optional[str], default = None
        Accepts either None or a numerical_feature. The cluster labels will then be reassigned based on the weight of sum of the given numerical feature.
        Example : reassign_labels_feature = 'total_revenue'. label 0/A will correpond to the products of a cluster whose sum(total_revenue) is greatest. And so on.

    retain_input_df : bool, default = True
        When set to true, the resultant dataframe will contain the labels of the cluster concatenated with the input dataframe. Returns only labels when set to False.

    numerical_columns : Optional[List[str]], default = None
        When set to None, the numerical features from the df are auto-detected. Otherwise, a list of numerical features is expected.
    
    categorical_columns : Optional[List[str]], default = None
        When set to None, the categorical features from the df are auto-detected. Otherwise, a list of numerical features is expected.

    ignore_features : Optional[List[str]], default = None
        The data such as target variable of the clustering or any other feature that should not be used for the clustering should be passed in a list.

    immune_columns : Optional[List[str]], default = None
        Accepts a list of features, which should be made immune to the feature_selection criteria.
        Example : say, immune_columns = ["promo"]. If the "promo" feature has zero variance and is expected to be removed, even then this feature will be persisted
                  in the dataframe and will not be dropped.
    
    numerical_imputation : dict, default = {'default' : 'zero'}
        Use this to impute 'nan' values in numerical features. Accepts a dictionary, with mandatory key 'default'.
        The keys of the dictionary are expected to be one or more numerical features and the values should be one of the imputation featuers which are given below.
        
        The imputations can be one of the below,
            -- "none"
            -- "zero"
            -- "sum"
            -- "mean"
            -- "median"
            -- "mode

        Example : 
            numerical_imputation = {
                'default' : 'zero',
                'promo' : 'mean',
                'revenue' : {'median':'color'}
            }
            From the above example,
            The nan values in promo will be replace with mean of the column promo.
            The nan values in revenue will be replace with the median of the revenue based on the grouping of the categorical feature 'color'. The median of revenue 
                for the same colors are imputed.
            The nan values in remaining numerical columns expect for promo and revenue will be imputed with 0s.


    categorical_imputation : Optional[List[str]], deafault = {'default' : 'NA'}
        Use this to impute 'nan' values in categorical features. Accepts a dictionary, with mandatory key 'default'.
        The keys of the dictionary are expected to be one or more categorical features and the values should be one of the imputation featuers which are given below.
        
        The imputations can be one of the below,
            -- "none"
            -- "NA"
            -- "mode"

        Example : 
            categorical_imputation = {
                'default' : 'none',
                'color' : 'NA',
                'size' : {'mode':'color'}
            }
            From the above example,
            The nan values in color will be replace with 'NA'.
            The nan values in size will be replace with the mode(highest frequecy value) of the size based on the grouping of the categorical feature 'color'. The most repeated
                value of size for a give color is imputed.
            The nan values in remaining categorical columns expect for color and size will be not be dealt. Make sure there are no nan values in these categorical columns if 
                dimentionality reduction is exptected

        
    high_cardinality_imputation : bool, default = False
        Set this to true, if high cardinal features are suspected in the data.
    high_cardinality_features : Optional[dict], default = None
        Ignored when high_cardinality_imputation is set to False. When high_cardinality_imputation is set to true, a dict is expected. The key of dict can either be set to 'frequency' or a numerical feature.
        The value of the dict is expected to be a list of high cardinal features. These features should be categorical features.
    high_cardinality_threshold : float, default = 0.05
        Ignored when high_cardinality_imputation is set to False. When high_cardinality_imputation is set to true, a float is expected within 0 < x < 1 

        Example : high_cardinality_imputation = True
                  high_cardinality_features = {'revenue':['color','size']}
                  high_cardinality_threshold = 0.05

                  The colors which contribute to less than 5% of the total revenue will be replaced with 'other'. Same for size.
                  If high_cardinality_features = {'frequency':['color','size']}, then, the colors that have relative_frequency less than 5% will be replaced by other.

    numerical_transformations : dict, default = {default = 'min-max-scaler'}
        Use this to transform numerical features. Accepts a dictionary, with mandatory key 'default'.
            The keys of the dictionary are expected to be one or more numerical features and the values should be one of the transformations which are given below.
            
            The transformations can be one of the below,
                -- "none"
                -- "min-max-scaler"
                -- "standard-scaler"
                -- "max-abs-scaler"
                -- "robust-scaler" 
                -- "log"
                -- "log2"
                -- "log10"
                -- "square"
                -- "sqrt"
                -- "cube"


            Example : 
                numerical_transformations = {
                    'default' : 'none',
                    'promo' : 'min-max-scaler',
                    'quantity' : 'log'
                }
                From the above example,
                The promo will be transformed with min-max-scaler(promo).
                The quantity will be transformed as log(quantity)
                All the numerical columns with the exception of promo and quantity will be left as is.

    categorical_transformations : dict, default = {default = 'one-hot-encoding'}
        Use this to transform categorical features. Accepts a dictionary, with mandatory key 'default'.
            The keys of the dictionary are expected to be one or more categorical features and the values should be one of the transformations which are given below.
            
            The transformations can be one of the below,
                -- "none"
                -- "one-hot-encoding"
                -- "ordinal-encoding"

            Example : 
                categorical_transformations = {
                    'default' : 'ordinal-encoding',
                    'color' : 'one-hot-encoding',
                }
                From the above example,
                The color will be transformed with one-hot-encoding(color).
                All the numerical columns with the exception of color will be transformed using ordinal-encoding.

    dimentionality_reduction : Optional[str], default = None,
        Accepts one dimentionality reduction algorithm(as of now),
            -- "PCA"
        When set to "PCA", PCA is applied on the dataset. Warning : The actual dataset is lost and reduced dataset is retained.

    dimentionality_reduction_variance_threshold : float, default = 0.95
        Ignored When dimentionality_reduction is None. Accepts a float where, 0 < x < 1. 
        Number of components to retain is based on x. x% of variance will be retained in the dataset.

    remove_zero_variance_features : bool, default = False
        When set to True, a feature with 0 variance will be dropped.

    rnd : int, default = 4
        All the numerical features in the dataframe will be rounded off to rnd.

    n_jobs : int, default = -1
        When doing parallel processing, n_jobs number of processes are used simultaneously

    verbose : bool, default = False
        When set to True, detailed information is logged.
            
    best_model_selection_weightage : Optional[dict], default = {"silhouette_score": 1, "calinski_harabasz_score": 1, "davies_bouldin_score": 1}
    """

    # ==============================  EXTRACT THE VALIDATED INPUT FROM PYDANTIC MODEL ===

    df = inp.df
    target = inp.target
    n_clusters = inp.n_clusters
    algorithms = inp.algorithms
    parameters = inp.parameters
    output_algo_consistent = inp.output_algo_consistent
    output_best_result_only = inp.output_best_result_only
    char_labels = inp.char_labels
    reassign_labels_feature = inp.reassign_labels_feature
    retain_input_df = inp.retain_input_df
    numerical_columns = inp.numerical_columns
    categorical_columns = inp.categorical_columns
    ignore_columns = inp.ignore_columns
    immune_columns = inp.immune_columns
    numerical_imputation = inp.numerical_imputation
    categorical_imputation = inp.categorical_imputation
    high_cardinality_imputation = inp.high_cardinality_imputation
    high_cardinality_features = inp.high_cardinality_features
    high_cardinality_threshold = inp.high_cardinality_threshold
    numerical_transformation = inp.numerical_transformation
    categorical_transformation = inp.categorical_transformation
    dimentionality_reduction = inp.dimentionality_reduction
    dimentionality_reduction_variance_threshold = inp.dimentionality_reduction_variance_threshold
    remove_zero_variance_features = inp.remove_zero_variance_features
    rnd = inp.rnd
    verbose = inp.verbose
    best_algo_selection_weightage = inp.best_model_selection_weightage

    # ============================== PREPROCESSING ======================================

    master_df = df.copy(deep=True)

    df = preprocessing({
        "df": df,
        "numerical_columns": numerical_columns,
        "categorical_columns": categorical_columns,
        "remove_columns": coerce_list(ignore_columns) + target,
        "immune_columns": immune_columns,
        "numerical_imputation": numerical_imputation,
        "categorical_imputation": categorical_imputation,
        "high_cardinality_imputation": high_cardinality_imputation,
        "high_cardinality_features": high_cardinality_features,
        "high_cardinality_threshold": high_cardinality_threshold,
        "numerical_transformations": numerical_transformation,
        "categorical_transformations": categorical_transformation,
        "dimentionality_reduction": dimentionality_reduction,
        "dimentionality_reduction_variance_threshold": dimentionality_reduction_variance_threshold,
        "remove_zero_variance_features": remove_zero_variance_features,
        "rnd": rnd,
        "verbose": verbose
    }
    )

    if verbose:
        print("\n \n Preprocessed input dataframe : ")
        print(df.head(3))

    sufficient_samples = df.shape[0] > MIN_SAMPLES_CLUSTERING
    # ============================== MODELLING ====================================

    if sufficient_samples:
        random.seed(1)

        # get all the grids[ grids =  algorithms * n_clusters]
        algorithm_grids = get_all_grids(
            n_clusters, selected_algorithms=algorithms, parameters=parameters)

        # record the output for each grid

        labels_df = pd.DataFrame()
        
        for grid_index in list(algorithm_grids.keys()):
            labels = mapper.get(grid_index.split("_")[0] if "_" in grid_index else grid_index, default_method)(
                df, algorithm_grids, grid_index)
            labels_df[grid_index] = labels

        # Select the best algorithm(best of all the grids)
        labels_df = remove_zero_variance_features_util(labels_df)
        best_algo_all = select_best_algo_clustering(df, labels_df, best_algo_selection_weightage)
        if verbose:
            print(
                f"\n \n Best algorithm optimised overall : '{best_algo_all}'")

        # ============================== POST PROCESSING ====================================

        # Record output labels for each k in n_clusters
        if n_clusters and not(output_best_result_only):

            # if the algorithm for eack k has to be consistent
            if output_algo_consistent:
                best_algo = best_algo_all.split("_")[0]
                algo_to_output = [grid_index
                                  for grid_index in list(algorithm_grids.keys())
                                  if grid_index.startswith(best_algo)]

            # to select the best algorithm(can be any) for each k
            else:

                if verbose:
                    print("\n \n The best algorithm of each cluster k is, ")

                algo_to_output = []
                for k in n_clusters:
                    algo_k_cluster = [grid_index
                                      for grid_index in list(algorithm_grids.keys())
                                      if grid_index.endswith(str(k))]
                    best_algo = select_best_algo_clustering(
                        df, labels_df[algo_k_cluster],best_algo_selection_weightage)
                    algo_to_output.append(best_algo)

                    if verbose:
                        print(f"{k} : {best_algo}")

        # to select the best algorithm overall(will be only one algorithm)
        else:
            algo_to_output = [best_algo_all]

        labels_df = labels_df[algo_to_output]

        # Rename the column names of labels_df from algo_names to cluster
        if n_clusters and not(output_best_result_only):
            labels_df.columns = [f"labels_k_{k}" for k in n_clusters]
        else:
            labels_df.columns = ['labels']

        # Re-assigning cluster names based on a feature
        if reassign_labels_feature:
            master_df[reassign_labels_feature].fillna(0, inplace=True)
            for label_index in labels_df.columns:
                labels_mapping_df = pd.concat([master_df[reassign_labels_feature], labels_df[label_index]], axis=1) \
                    .groupby(label_index) \
                    .mean() \
                    .reset_index()
                labels_mapping_df['new_labels'] = labels_mapping_df[reassign_labels_feature].rank(
                    method='first', ascending=False).astype('int')
                label_mapping_dict = {}
                for label_id in labels_mapping_df[label_index]:
                    label_mapping_dict[label_id] = labels_mapping_df.loc[labels_mapping_df[label_index]
                                                                         == label_id, 'new_labels'].iloc[0]
                labels_df[label_index].replace(
                    label_mapping_dict, inplace=True)

    # if there are not enough samples then cluster all the samples as a single cluster
    else:
        labels_df = pd.DataFrame(
            data={
                'labels': [0 for _ in range(master_df.shape[0])]
            }
        )
    # Map labesl from (0 -> A, ...)
    if char_labels:
        alpha_labels = list(
            ascii_uppercase) + [l1+l2 for l1 in ascii_uppercase for l2 in ascii_uppercase]
        label_min = min(labels_df.min())
        label_max = max(labels_df.max()) + 1
        num_labels = list(range(label_min, label_max))
        char_label_mapping = {}
        if len(num_labels) > 1:
            for i in range(len(num_labels)):
                char_label_mapping[num_labels[i]] = alpha_labels[i]
        else:
            char_label_mapping[0] = alpha_labels[0]
        labels_df.replace(char_label_mapping, inplace=True)

    # Return the output based on n_clusters
    if retain_input_df:
        return pd.concat([master_df, labels_df], axis=1)
    else:
        return pd.concat([master_df[target], labels_df], axis=1)
