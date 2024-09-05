import pandas as pd
import os


def extract(store_data, parquet_file):
    """
    :param file_path: raw parquet file
    :return: merged data frame from SQL df and parquet file
    
    >>> extract(grocery_sales, "extra_data.parquet")
    "merged dataframe"
    """
    
    raw_data_pq = pd.read_parquet(parquet_file)
    # Just to make sure that the contents of each data has the same row numbers, so no colums will become null
    assert store_data.shape[0] == raw_data_pq.shape[0]
    
    merged_df = store_data.merge(raw_data_pq, on='index')
    
    return merged_df

merged_df = extract(grocery_sales, "extra_data.parquet")


def transform(merged_df):
    """
    Implement a function named transform() with one argument, 
    taking merged_df as input, 
    filling missing numerical values (using any method of your choice), 
    adding a column "Month", 
    keeping the rows where the weekly sales are over $10,000 and drops the unnecessary columns. 
    Ultimately, it should return a DataFrame and be stored as the clean_data variable.
    
    :param merged_df: extracted and merged data frame.
    :return: clean data
    
    >>> tansform(merged_df)
    "clean_data"
    """
    
    merged_df.fillna(
        {
            'CPI' : merged_df['CPI'].mean(),
            'Weekly_Sales' : merged_df['Weekly_Sales'].mean(),
            'Unemployment' : merged_df['Unemployment'].mean()
        }, inplace = True
    )
    
    # Date column manipulation and create new column for numerical month.
    merged_df["Date"] = pd.to_datetime(merged_df["Date"], format = "%Y-%m-%d")
    merged_df["Date"].fillna(method='ffill', inplace=True)
    merged_df["Month"] = merged_df["Date"].dt.month
    
    # Filter the weekly sales that are over $10,000 and select the required columns.
    clean_data = merged_df.loc[merged_df["Weekly_Sales"] > 10000, ["Store_ID", "Month", "Dept", "IsHoliday", "Weekly_Sales", "CPI", "Unemployment"]]
    
    return clean_data

clean_data = transform(merged_df)


def avg_monthly_sales(clean_data):
    """
     Define a function called avg_monthly_sales() that takes clean_data as input and returns an aggregated DataFrame containing two columns - "Month" and "Avg_Sales" (rounded to 2 decimals). You should call the function and store the results as a variable called agg_data.
     
     :param clean_data: extracted and merged data frame.
    :return: list of average sales per month
    
    >>> avg_monthly_sales(merged_df)
    "clean_data"
    """
    agg_data = clean_data[["Month", "Weekly_Sales"]]
    
    agg_data = (agg_data.groupby("Month").agg(Avg_Sales = ("Weekly_Sales", "mean")).reset_index().round(2))
    
    return agg_data

# Call the avg_monthly_sales() function and pass the cleaned DataFrame
agg_data = avg_monthly_sales(clean_data)


def load(clean_data, clean_data_file_path, agg_data, agg_data_file_path):
    """
    :param clean_data: data frame of clean data.
    :param clean_data_file_path: desired file path of csv file.
    :param acc_data: data frame of aggregated data.
    :param agg_data_file_path: desired file path of aggregated data in csv file.
    :return: .csv file format of each dataframe.
    """
    # Save tha data as a csv file
    clean_data_file = clean_data.to_csv(clean_data_file_path, index=False)
    agg_data_file = agg_data.to_csv(agg_data_file_path, index=False)
    
    return clean_data_file, agg_data_file

load(clean_data, "clean_data.csv", agg_data, "agg_data.csv")


def validation(file_path):
    """
    Validate if the file exists at the given file path.
    
    Parameters:
    - file_path (str): The path to the file that needs to be validated.
    
    Raises:
    - Exception: If the file does not exist.
    """
    if not os.path.exists(file_path):
        raise Exception(f"There is no file at the path {file_path}")
    print(f"File at {file_path} exists and is ready for processing.")

# Call the validation() function and pass first, the cleaned DataFrame path, and then the aggregated DataFrame path
validation("clean_data.csv")
validation("agg_data.csv")