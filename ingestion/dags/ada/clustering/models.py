from pandas import DataFrame
from pydantic import BaseModel, validator
from typing import Optional,List
from ada.clustering.utils import get_all_algorithms, coerce_list

class clustering_input_model(BaseModel):
    df                                         : DataFrame
    target                                     : List[str]
    n_clusters                                 : Optional[list] = None
    algorithms                                 : Optional[list] = None
    parameters                                 : Optional[dict] = None
    output_algo_consistent                     : bool = False
    output_best_result_only                    : bool = False
    char_labels                                : bool = False
    reassign_labels_feature                    : Optional[str] = None
    retain_input_df                            : bool = True
    numerical_columns                          : Optional[list] = None
    categorical_columns                        : Optional[list] = None
    ignore_columns                             : Optional[list] = None
    immune_columns                             : Optional[list] = None
    numerical_imputation                       : dict = {'default' : 'zero'}
    categorical_imputation                     : dict = {'default' : 'NA'}
    high_cardinality_imputation                : bool = False
    high_cardinality_features                  : Optional[dict] = None
    high_cardinality_threshold                 : float = 0.05
    numerical_transformation                   : dict = {'default' : 'min-max-scaler'}
    categorical_transformation                 : dict = {'default' : 'one-hot-encoding'}
    dimentionality_reduction                   : Optional[str]= 'PCA'
    dimentionality_reduction_variance_threshold: float = 0.95
    remove_zero_variance_features              : bool = True
    rnd                                        : int = 4
    verbose                                    : bool = False
    n_jobs                                     : int = -1
    best_model_selection_weightage             : Optional[dict] = {"silhouette_score": 1, "calinski_harabasz_score": 1, "davies_bouldin_score": 1}

    class Config:
        arbitrary_types_allowed = True

    # -- target validation
    @validator("target")
    def target_val(cls, target, values):
        for tar in target:
            if tar not in values["df"].columns:
                raise ValueError(f"target ({tar}) not in the input dataframe.")
        return target

    # -- algorithms list validation
    @validator("algorithms")
    def algorithms_val(cls, algorithms):
        for alg in coerce_list(algorithms):
            if alg not in get_all_algorithms().index.to_list():
                raise ValueError(f"Algorithm {alg} is not valid.")
        return algorithms
