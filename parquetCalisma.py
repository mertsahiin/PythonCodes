import pandas as pd
parquet_file = r'C:\\Users\\merte\\Desktop\\parquet'
df = pd.read_parquet(parquet_file, engine='auto')
df.sort_values('first_name', ascending=[1])