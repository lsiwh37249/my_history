import pandas as pd
import sys
import argparse

def cnt(q):
    df = read_parquet()
    #df = read_parquet('~/tmp/history.parquet')
    fdf = df[df['cmd'].str.contains(q)]
    cnt = fdf['cnt'].sum()
    return cnt
    #return None  아무것도 출력하지 않고 종료한다.


def read_parquet(path='~/data/parquet'):

    df = pd.read_parquet(path)
    return df

def showcnt(amount, yyyymmdd):
    df = read_parquet()
    fdf = df[df['dt'] == yyyymmdd]
    sdf = fdf.sort_values(by='cnt', ascending=False).head(amount)
    ddf = sdf.drop(columns=['dt'])
    return ddf.to_string(index=False)


def query():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-s','--command', type=str )
    parser.add_argument('-t','--amount', type=int )
    parser.add_argument('-d','--date', type=str )

    args = parser.parse_args()
    
    if args.command:
        i = cnt(f'{args.command}')
        print(f'{args.command} 사용 횟수는 {i}회 입니다')
    else:
        j = showcnt(args.amount, args.date)
        print(j)

if __name__ == "__main__":
    query()
