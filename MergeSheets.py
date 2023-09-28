import pandas as pd
import os

files_path = os.getcwd() + '\\Files'

files = [f for f in os.listdir(files_path) if f.endswith('.csv')]

dataframes = []
for file in files:
    df = pd.read_csv(os.path.join(files_path, file))
    dataframes.append(df)

merged_df = pd.concat(dataframes)

outpath = os.getcwd()
merged_df.to_csv(os.path.join(outpath, 'Datos.csv'), index = False)
