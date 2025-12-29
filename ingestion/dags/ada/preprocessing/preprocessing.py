import pandas as pd
from .utils import dtype_selection, get_dict_key_val, coerce_list, remove_zero_variance_features_util, apply_PCA
from .constants import imp_agg, transformations
from .models import preprocessing_model

def preprocessing(
   inp : dict
):
    input = preprocessing_model(**inp)

    """ 
    This funtion can be used for all sorts of manipulation of the data. 
    Includes 
        -- categorical/numerical features imputation 
        -- categorical/numerical features transformation
        -- high cardinality features imputation  
        -- feature selection 
        -- dimentionality reduction

    df : pd.DataFrame
        This is mandatory input. A pandas dataframe is expected with shape(n,m) where n > 0 and m > 0.

    numerical_columns : Optional[List[str]], default = None
        When set to None, the numerical features from the df are auto-detected. Otherwise, a list of numerical features is expected.
    
    categorical_columns : Optional[List[str]], default = None
        When set to None, the categorical features from the df are auto-detected. Otherwise, a list of numerical features is expected.
    
    remove_columns : Optional[List[str]], default = None
        Accepts a list of features from df that are expected to be dropped from the df.

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

    remove_perfect_colliniarity : bool, default = False
        When set to True, if two of the features in the input dataframe have perfect collinearity, one of the feature will be randomly dropped.
    
    remove_based_on_VIF : Optional[float], default = None
        -- pending

    remove_based_on_p_value : Optional[float], default = None
        -- pending

    rnd : int, default = 4
        All the numerical features in the dataframe will be rounded off to rnd.
    
    verbose : bool, default = False
        When set to True, detailed information is logged.
            
    """

    # ==============================  EXTRACT THE VALIDATED INPUT FROM PYDANTIC MODEL ===

    df                                          = input.df
    numerical_cols                              = input.numerical_cols
    categorical_cols                            = input.categorical_cols
    remove_columns                              = input.remove_columns
    immune_columns                              = input.immune_columns
    numerical_imputation                        = input.numerical_imputation
    categorical_imputation                      = input.categorical_imputation
    high_cardinality_features                   = input.high_cardinality_features
    high_cardinality_imputation                 = input.high_cardinality_imputation
    high_cardinality_threshold                  = input.high_cardinality_threshold
    numerical_transformations                   = input.numerical_transformations
    categorical_transformations                 = input.categorical_transformations
    dimentionality_reduction                    = input.dimentionality_reduction
    dimentionality_reduction_variance_threshold = input.dimentionality_reduction_variance_threshold
    remove_zero_variance_features               = input.remove_zero_variance_features
    remove_perfect_colliniarity                 = input.remove_perfect_colliniarity
    remove_based_on_VIF                         = input.remove_based_on_VIF
    remove_based_on_p_value                     = input.remove_based_on_p_value
    rnd                                         = input.rnd
    verbose                                     = input.verbose
    
    # ============================== MISSING VALUES IMPUTATION ==========================

    # Cateogorical feature imputation

    if not(categorical_imputation.get('default')):
        categorical_imputation['default'] = 'NA'
    for col in categorical_cols:
        if col not in list(categorical_imputation.keys()):
            categorical_imputation[col] = categorical_imputation["default"]
    del categorical_imputation['default']
      
    for key,val in categorical_imputation.items():
        # simple imputation
        if isinstance(val, str):
            df.loc[:,key] = df.loc[:,key].fillna(imp_agg[val](df[key]))
            continue
        # feature dependent imputation
        if isinstance(val, dict):
            feat = get_dict_key_val(val)[1]
            feat_values = df[feat].unique()
            for feat_value in feat_values:
                agg = get_dict_key_val(val)[0]
                # if the base feature used for calculation is completely null then impute zero
                if all(df.loc[df[feat] == feat_value,key].isnull()):
                    agg = 'NA'
                df.loc[df[feat] == feat_value,key] = df.loc[df[feat] == feat_value,key]\
                                                        .fillna(imp_agg[agg](df.loc[df[feat] == feat_value,key]))

    # numerical features imputation

    if not(numerical_imputation.get('default')):
        numerical_imputation['default'] = 'zero'
    for col in numerical_cols:
        if col not in list(numerical_imputation.keys()):
            numerical_imputation[col] = numerical_imputation["default"]
    del numerical_imputation['default']

    for key,val in numerical_imputation.items():
        # simple imputation
        if isinstance(val, str):
            df.loc[:,key] = df.loc[:,key].fillna(imp_agg[val](df[key]))
            continue
        # feature dependent imputation
        if isinstance(val, dict):
            feat = get_dict_key_val(val)[1]
            feat_values = df[feat].unique()
            for feat_value in feat_values:
                agg = get_dict_key_val(val)[0]
                # if the base feature used for calculation is completely null then impute zero 
                if all(df.loc[df[feat] == feat_value,key].isnull()):
                    agg = 'zero'
                df.loc[df[feat] == feat_value,key] = df.loc[df[feat] == feat_value,key]\
                                                       .fillna(imp_agg[agg](df.loc[df[feat] == feat_value,key]))

                
    # ==============================  HIGH CARDINALITY =================================== 
    
    if high_cardinality_imputation:
        imp_on = high_cardinality_features[0]
        features = high_cardinality_features[1]
        
        # Relative frequency based imputation
        if imp_on == 'frequency':
            for col in features:
                if col not in categorical_cols:
                    raise TypeError("High cardinality features[1] has to be a list of categorical features.")
                rel_freq = pd.DataFrame(df[col].value_counts(normalize = True))
                low_freq_vals = rel_freq[rel_freq[col] < high_cardinality_threshold].index.to_list()
                df.loc[df[col].isin(low_freq_vals), col] = "other"
        
        # Feature dependent imputation
        elif imp_on in numerical_cols:
            for col in features:
                if col not in categorical_cols:
                    raise TypeError("High cardinality features[1] has to be a list of categorical features.")
            
                feature_sum = pd.DataFrame(df.groupby(col).sum()[imp_on])
                rel_freq = feature_sum/feature_sum.sum()
                low_freq_vals = rel_freq[rel_freq[imp_on] < high_cardinality_threshold].index.to_list()
                df.loc[df[col].isin(low_freq_vals), col] = "other"  
        else:
            raise TypeError("The high_carinality_features should be 'frequency' (or) a numerical feature.")
    

    # ============================== FEATURE TRANSFORMATION ==================================      

    # Categorical transformations
    
    # making a copy to not reference the original dict
    categorical_transformations = categorical_transformations.copy()

    if not(categorical_transformations.get('default')):
        categorical_transformations['default'] = 'one-hot-encoding'
    for col in categorical_cols:
        if col not in list(categorical_transformations.keys()):
            categorical_transformations[col] = categorical_transformations["default"]
    del categorical_transformations['default']

    for key, val in categorical_transformations.items():
        if val == "none":
            continue
        df = pd.concat(
                [df.drop(key, axis = 1),
                transformations[val](df[[key]])],
                axis = 1
            )
        
    # Numerical transformations
    
    # making a copy to not reference the original dict
    numerical_transformations = numerical_transformations.copy()

    if not(numerical_transformations.get('default')):
        numerical_transformations['default'] = 'none'
    for col in numerical_cols:
        if col not in list(numerical_transformations.keys()):
            numerical_transformations[col] = numerical_transformations["default"]
    del numerical_transformations['default']
    
    for key, val in numerical_transformations.items():
        if val == "none":
            continue
        df = pd.concat(
                [df.drop(key, axis = 1),
                transformations[val](df[[key]])],
                axis = 1
            )


    # ============================== FEATURE SELECTION ========================================
    
    # Make a copy of immune features
    if immune_columns:
        immune_cols = list(filter(lambda x : any([x.startswith(col) for col in immune_columns]), df.columns))
        immune_features = df[immune_cols].copy(deep=True)

    # Remove 0 variance columns
    if remove_zero_variance_features:
        df = remove_zero_variance_features_util(df)
        if df.empty:
            return df
    
    # Remove features if there is perfect colliniarity
    if remove_perfect_colliniarity:
        pass
    
    # Remove features based on VIF
    if remove_based_on_VIF: 
        pass

    if remove_based_on_p_value:
        pass
        
    # Add the immune features back to the dataframe
    if immune_columns:
        immune_cols = [col for col in immune_cols if col not in df.columns]
        if immune_cols:
            df = pd.concat(
                    [df,
                    immune_features[immune_cols]],
                    axis = 1
                )

    # ============================== DIMENSIONALITY REDUCTION =========================================
    
    # Principle Component Analysis
    if dimentionality_reduction == 'PCA':
        df = apply_PCA(df,dimentionality_reduction_variance_threshold)
    
    #TODO : t-SNE

    # Round the df to rnd
    df = df.round(rnd)


    # ============================== RETURN DF =========================================================
    
    return df