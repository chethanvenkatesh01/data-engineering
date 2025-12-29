import pandas as pd
from sklearn.decomposition import PCA

# check if the columns in given df belong to given dtype
def check_column_dtype(df, columns, dtype):
    for col in columns:
        try:
            df[col].apply(dtype)
        except ValueError:
            raise ValueError(f"Could not covert column '{col}' to {str(dtype)}.")
    return df

# extract the key and value from a dictionary
def get_dict_key_val(d):
    return list(d.keys())[0], list(d.values())[0]

# return list if arg is not None
def coerce_list(*args):
    lst = []
    for i in args:
        if i:
            lst = lst + i
    lst = [_ for _ in lst if _]
    return lst

def dtype_selection(df : pd.DataFrame, numerical_columns, categorical_columns):
        
    columns = df.columns
    
    numerical_cols = []
    categorical_cols = []
        
    # Read data-types from input
    if numerical_columns:
        df = check_column_dtype(df, numerical_columns, float)
        numerical_cols = numerical_columns
        if not(categorical_columns):
            categorical_cols = list(set(columns) - set(numerical_cols))
            df = check_column_dtype(df, categorical_cols, str)
            
    if categorical_columns:
        df = check_column_dtype(df,categorical_columns,str)
        categorical_cols = categorical_columns
        if not(numerical_columns):
            numerical_cols = list(set(columns) - set(categorical_cols))
            df = check_column_dtype(df, numerical_cols, float)
    
    # Auto Detect data type of features
    if not(numerical_columns) and not(categorical_columns):
        for col in columns:
            try:
                df[col].apply(float)
                numerical_cols.append(col)
            except:
                categorical_cols.append(col)

    return numerical_cols, categorical_cols

def remove_zero_variance_features_util(df):
    no_var_columns = df.var()[df.var()==0].index.to_list()
    if no_var_columns:
        df.drop(no_var_columns, axis = 1, inplace = True)
    if df.empty:
        df = pd.DataFrame()
    return df

# Dimentionality reduction - PCA
def apply_PCA(df, variance_threshold):
    pca = PCA(n_components=variance_threshold)
    components = pd.DataFrame(pca.fit_transform(df))
    total_variance = 0
    components_count = 1
    for i in pca.explained_variance_ratio_:
        total_variance = total_variance + i
        if total_variance > variance_threshold:
            break
        components_count = components_count + 1 
    components = components.iloc[:,0:components_count]
    components.columns = [f'PCA_{i}' for i in range(1,components.shape[1]+1)]
    return components


