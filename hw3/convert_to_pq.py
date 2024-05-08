import os
import duckdb

def csvgz_parquet(dir_path):
  """
  Convert all csv.gz files in a directory to parquet files in the same directory.
  ---
  Args:
    dir_path (string): Path to csv.gz files
  Return:
    None
  """
  for filename in os.listdir(dir_path):  # Iterate over each file
    if filename.endswith(".csv.gz"):  # Look for .csv.gz files
      file_path = os.path.join(dir_path, filename)  # Full path to the file
      print(f"Working on {file_path}")
      parquet_filename = os.path.splitext(os.path.splitext(filename)[0])[0] + ".parquet"  # .parquet name
      parquet_path = os.path.join(dir_path, parquet_filename)  # Output path
      with duckdb.connect(database=':memory:') as con:
        con.execute(f"COPY (SELECT * FROM read_csv_auto('{file_path}', ALL_VARCHAR=True)) TO '{parquet_path}' (FORMAT PARQUET);")

def csv_parquet(dir_path):
  """
  Convert all csv files in a directory to parquet files in the same directory.
  ---
  Args:
    dir_path (string): Path to csv.gz files
  Return:
    None
  """
  for filename in os.listdir(dir_path):  # Iterate over each file
    if filename.endswith(".csv"):  # Look for .csv.gz files
      file_path = os.path.join(dir_path, filename)  # Full path to the file
      print(f"Working on {file_path}")
      parquet_filename = os.path.splitext(os.path.splitext(filename)[0])[0] + ".parquet"  # .parquet name
      parquet_path = os.path.join(dir_path, parquet_filename)  # Output path
      with duckdb.connect(database=':memory:') as con:
        con.execute(f"COPY (SELECT * FROM read_csv_auto('{file_path}', ALL_VARCHAR=True)) TO '{parquet_path}' (FORMAT PARQUET);")