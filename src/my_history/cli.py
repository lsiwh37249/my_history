import pandas as pd
import sys

var = sys.argv[1]  

def cnt():
    df = pd.read_parquet("~/tmp/history.parquet")
    fdf = df[df['cmd'].str.contains(var)]
    cnt = fdf['cnt'].sum()
    print(cnt)

