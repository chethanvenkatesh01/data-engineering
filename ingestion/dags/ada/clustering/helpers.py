from ada.clustering.utils import get_all_algorithms,select_best_algo_clustering
from sklearn.cluster import estimate_bandwidth, MeanShift
import importlib
import numpy as np
import faiss


def default_method(df, algorithm_grids, grid_index):
    grid = algorithm_grids[grid_index]
    model = grid.fit(df).best_estimator_
    labels = model.fit(df).labels_
    return labels


def meanshift(df, algorithm_grids, grid_index):

    all_algorithms = get_all_algorithms()
    all_algorithms.reset_index(inplace=True)
    parameters = eval(
        all_algorithms.loc[all_algorithms['index'] == grid_index]['parameters'].values[0])
    bandwidth = estimate_bandwidth(df, quantile=parameters.get("quantile"),
                                   n_samples=parameters.get("n_samples"), random_state=parameters.get("random_state"))
    # Fit Mean Shift with Scikit
    meanshift = MeanShift(bandwidth=bandwidth)
    meanshift.fit(df)
    labels = meanshift.labels_
    return labels


def faiss_helper(df, algorithm_grids, grid_index):
    x_f = np.ascontiguousarray(df)
    all_algorithms = get_all_algorithms()
    all_algorithms.reset_index(inplace=True)
    parameters = eval(
        all_algorithms.loc[
            all_algorithms['index'] == grid_index.split("_")[0]
        ]['parameters'].values[0]
    )
    kmeans = faiss.Kmeans(
        d=x_f.shape[1], 
        k=int(grid_index.split("_")[1]), 
        niter=parameters.get('niter'), 
        nredo=parameters.get('nredo')
    )
    kmeans.train(x_f.astype(np.float32))
    labels = [_[0] for _ in kmeans.index.search(x_f.astype(np.float32), 1)[1]]
    return labels
