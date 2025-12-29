from pandas import DataFrame
from pydantic import BaseModel, validator
from typing import Optional
from .utils import dtype_selection, get_dict_key_val, coerce_list
from .constants import imp_agg, transformations


class preprocessing_model(BaseModel):
    df                                         : DataFrame
    numerical_columns                          : Optional[list] = None
    categorical_columns                        : Optional[list] = None
    remove_columns                             : Optional[list] = None
    immune_columns                             : Optional[list] = None
    numerical_imputation                       : dict = {'default' : 'zero'}
    categorical_imputation                     : dict = {'default' : 'NA'}
    high_cardinality_features                  : Optional[list] = None
    high_cardinality_imputation                : bool = False
    high_cardinality_threshold                 : float = 0.05
    numerical_transformations                  : dict = {'default' : 'min-max-scaler'}
    categorical_transformations                : dict = {'default' : 'one-hot-encoding'}
    dimentionality_reduction                   : Optional[str] = None
    dimentionality_reduction_variance_threshold: float = 0.9
    remove_zero_variance_features              : bool = False
    remove_perfect_colliniarity                : bool= False
    remove_based_on_VIF                        : bool = False
    remove_based_on_p_value                    : bool = False
    rnd                                        : int = 4
    verbose                                    : bool = False

    class Config:
        arbitrary_types_allowed = True

    # -- dataframe validation
    @validator("df")
    def df_empty(cls, df):
        if df.empty:
            raise ValueError("Dataframe is empty. Shape(n,m) should be greater than 0.")
        return df

    # -- input column names validation
    @validator("immune_columns")
    def check_if_the_given_columns_exit_in_df(cls, immune_columns, values):
        columns = values["df"].columns
        for col in coerce_list(values["numerical_columns"], values["categorical_columns"], values["remove_columns"], immune_columns):
            if col not in columns:
                raise ValueError(f"('{col}') was not found in input dataframe")

        for col in coerce_list(values["remove_columns"]):
            if col in coerce_list(values["numerical_columns"], values["categorical_columns"], immune_columns):
                raise ValueError(f"Feature ('{col}') is included in both remove_columns and numerical/categorical/immune_columns.")

        # -- data type selection
        if values["remove_columns"]:
            values['df'].drop(values["remove_columns"], axis = 1, inplace = True) 
        values["numerical_cols"], values["categorical_cols"] = dtype_selection(
                                                                values["df"], 
                                                                values["numerical_columns"], 
                                                                values["categorical_columns"])
        return immune_columns
    
    # -- numerical imputation validation
    @validator("numerical_imputation")
    def numerical_imputation_validation(cls, numerical_imputation, values):
        for key,val in numerical_imputation.items():
            if key == 'default':
                if val not in list(imp_agg.keys()):
                    raise ValueError(f"Numerical transformation ('{val}') is invalid.")
                continue
            if key not in values["numerical_cols"]:
                raise ValueError(f"The column ('{key}') from numerical imputation could not be found in input dataframe.")
            if isinstance(val,str):
                if val not in list(imp_agg.keys()):
                    raise ValueError(f"Numerical imputation method ('{val}') is invalid.")
            elif isinstance(val,dict):
                agg,feat = get_dict_key_val(val)
                if feat not in values["categorical_cols"]:
                    raise ValueError("""The group by feature in numerical imputation is expected to be categorical.""")
            else:
                raise ValueError(f"The value ('{val}') expected to be of type <str> or <dict>.")
        return numerical_imputation
    
    # -- categorical imputation vaidation
    @validator("categorical_imputation")
    def categorical_imputation_validation(cls, categorical_imputation, values):
        for key,val in categorical_imputation.items():
            if key == 'default':
                if val not in list(imp_agg.keys()):
                    raise ValueError(f"Categorical transformation ('{val}') is invalid.")
                continue
            if key not in values["categorical_cols"]:
                raise ValueError(f"The column ('{key}') from categorical imputation could not be found in input dataframe.")
            if isinstance(val,str):
                if val not in list(imp_agg.keys()):
                    raise ValueError(f"Categorical imputation method('{val}') is invalid.")
            elif isinstance(val,dict):
                agg,feat = get_dict_key_val(val)
                if key == feat:
                    raise ValueError(f"A categorical feature ({values['categorical_cols'][key]}) cannot be imputed using the same feature.")
                if feat not in values["categorical_cols"]:
                    raise ValueError("""The group by feature in categorical imputation is expected to be categorical.""")
            else:
                raise ValueError(f"The value ('{val}') expected to be of type <str> or <dict>.")
        return categorical_imputation

    # -- high cardinality impuation validation    
    @validator("high_cardinality_imputation")
    def high_cardinality_imputation_validation(cls, high_cardinality_imputation, values):
        if high_cardinality_imputation:

            if not(values["high_cardinality_features"]):
                raise ValueError(f"high_cardinality_imputation is set. High_cardinality_features cannot be None.")

            for col in coerce_list(values["high_cardinality_features"]):
                if col not in values['df'].columns:
                    raise ValueError(f"high_cardinality_features[1]({col}) is expected to be a categorical features.")
        return high_cardinality_imputation

    # -- high cardinality threshold validation
    @validator('high_cardinality_threshold')
    def high_cardinality_threshold_validation(cls, high_cardinality_threshold):
        if high_cardinality_threshold > 1:
            raise ValueError(f"Input 'high_cardinality_threshold' is expected in the range (0 < x < 1).")
        return high_cardinality_threshold

    # -- numerical transformation validation
    @validator('numerical_transformations')
    def numerical_transformations_validation(cls, numerical_transformations, values):
        for key,val in numerical_transformations.items():
            if key == 'default':
                if val not in list(transformations.keys()):
                    raise ValueError(f"Numerical transformation ('{val}') is invalid.")
                continue  
            if key not in values["numerical_cols"]:
                raise ValueError(f"The column ('{key}') from numerical transformations could not be found in input dataframe.")
            if val not in list(transformations.keys()):
                raise ValueError(f"Numerical transformation ('{val}') is invalid.")   
        return numerical_transformations

    # -- categorical transformation validation
    @validator('categorical_transformations')
    def categorical_transformations_validation(cls, categorical_transformations, values):
        for key,val in categorical_transformations.items():
            if key == 'default':
                if val not in list(transformations.keys()):
                    raise ValueError(f"Categorical transformation ('{val}') is invalid.")
                continue
            if key not in values["categorical_cols"]:
                raise ValueError(f"The column ('{key}') from categorical transformations could not be found in input dataframe.")
            if val not in list(transformations.keys()):
                raise ValueError(f"Categorical transformation ('{val}') is invalid.")
        return categorical_transformations

    # -- dimentionality reduction validation
    @validator("dimentionality_reduction")
    def dimentionality_reduction_validation(cls, dimentionality_reduction):    
        if not((dimentionality_reduction is None) or (dimentionality_reduction == "PCA")):
                raise ValueError(f"Input 'dimentionality_reduction' is expected to be either None or 'PCA'.")   
        return dimentionality_reduction

    @validator("dimentionality_reduction_variance_threshold")
    def dimentionality_reduction_variance_threshold_val(cls, dimentionality_reduction_variance_threshold):
        if dimentionality_reduction_variance_threshold > 1:
            raise ValueError(f"Input 'dimentionality_reduction_variance_threshold' is expected in the range (0 < x < 1).")
        return dimentionality_reduction_variance_threshold