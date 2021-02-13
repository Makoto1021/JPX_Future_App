import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import timedelta
import io
import requests
import re
from bs4 import BeautifulSoup
from functools import partial, reduce
from download_data import get_url, clean_dataframe, get_csv, format_data
from transform_data import *
import os
import glob
import boto3
from to_csv_on_s3 import to_csv_on_s3

## 1-2. パラメーターの設定
# reading s3 bucket
client = boto3.client('s3')
BUCKET_NAME = 'jpx-future-bucket'
FOLDER_RAW = "raw_data/"
FOLDER_FINAL = "final_data/"
PATH_GROUP = 's3://' + BUCKET_NAME + '/groups.xlsx'
HISTORICAL_GROUP = 's3://' + BUCKET_NAME + '/' + FOLDER_FINAL + 'historical_by_group.csv'
HISTORICAL_GROU_PRODUCT = 's3://' + BUCKET_NAME + '/' + FOLDER_FINAL + 'historical_by_group_product.csv'
TOPIX_MULTI = 1.0

def jpx_future():

    days = 2
    print("days", days)
    print("TOPIX_MULTI", TOPIX_MULTI)

    day = dt.today() - timedelta(days=days)
    colnames = ['institutions_sell_code', 'institutions_sell', 
            'institutions_sell_eng', 'volume_sell', 'institutions_buy_code', 
            'institutions_buy', 'institutions_buy_eng', 'volume_buy']

    print("2. データのダウンロード")

    ### 2-2. 日中立会取引データのダウンロード
    df_wholeday = format_data(datatype=3, day=day, colnames=colnames, FOLDER_RAW=FOLDER_RAW, BUCKET_NAME=BUCKET_NAME)
    df_wholeday_JNET = format_data(datatype=4, day=day, colnames=colnames, FOLDER_RAW=FOLDER_RAW, BUCKET_NAME=BUCKET_NAME)
    df_night = format_data(datatype=1, day=day, colnames=colnames, FOLDER_RAW=FOLDER_RAW, BUCKET_NAME=BUCKET_NAME)
    df_night_JNET = format_data(datatype=2, day=day, colnames=colnames, FOLDER_RAW=FOLDER_RAW, BUCKET_NAME=BUCKET_NAME)
    print("function worked!")

    ## 3. データの初期整理
    text = "\n3. データの初期整理"
    print(text)
    ### 3-1. 先物/OP識別カラムを加える
    df_wholeday["先物/OP"] = np.nan
    df_wholeday["先物/OP"] = df_wholeday.JPX_code.apply(fut_or_op)

    df_wholeday_JNET["先物/OP"] = np.nan
    df_wholeday_JNET["先物/OP"] = df_wholeday_JNET.JPX_code.apply(fut_or_op)

    df_night["先物/OP"] = np.nan
    df_night["先物/OP"] = df_night.JPX_code.apply(fut_or_op)

    df_night_JNET["先物/OP"] = np.nan
    df_night_JNET["先物/OP"] = df_night_JNET.JPX_code.apply(fut_or_op)

    ## 3-2. 銘柄を抽出する
    df_wholeday["銘柄"] = np.nan
    df_wholeday["銘柄"] = df_wholeday.JPX_code.apply(get_meigara)

    df_wholeday_JNET["銘柄"] = np.nan
    df_wholeday_JNET["銘柄"] = df_wholeday_JNET.JPX_code.apply(get_meigara)

    df_night["銘柄"] = np.nan
    df_night["銘柄"] = df_night.JPX_code.apply(get_meigara)

    df_night_JNET["銘柄"] = np.nan
    df_night_JNET["銘柄"] = df_night_JNET.JPX_code.apply(get_meigara)

    ## 4. 立会とJNETを統合し、限月ごとに整理して並び替える
    text = "\n4. 立会とJNETを統合します"
    print(text)
    ### 4-2. 日中の立会とJNETを統合する

    #　日中立会
    text = "4-2. 日中取引の統合…"
    print(text)
    # 225ラージ
    df_wholeday_large = df_wholeday[(df_wholeday["銘柄"]=="日経225（ラージ）") & (df_wholeday["先物/OP"] == "先物")]

    # 限月の抽出
    df_wholeday_large = get_gengetsu(df_wholeday_large)

    # サイズの確認
    for g in df_wholeday_large["限月"].unique():
        shape = df_wholeday_large[df_wholeday_large["限月"] == g].shape
        
        text = f"日中225ラージの第{g}限月のデータサイズは{shape}"
        print(text)
        
    #　JNET
    df_wholeday_JNET_large = df_wholeday_JNET[(df_wholeday_JNET["銘柄"]=="日経225（ラージ）") & (df_wholeday_JNET["先物/OP"] == "先物")]

    # 限月の抽出
    df_wholeday_JNET_large = get_gengetsu(df_wholeday_JNET_large)

    # サイズの確認
    for g in df_wholeday_JNET_large["限月"].unique():
        shape = df_wholeday_JNET_large[df_wholeday_JNET_large["限月"] == g].shape
        
        text = f"日中225ラージJNETの第{g}限月のデータサイズは{shape}"
        print(text)
        
    df_wholeday_large_stacked = make_long_df(df_wholeday_large)
    df_wholeday_JNET_large_stacked = make_long_df(df_wholeday_JNET_large)
    df_wholeday_large_sum = merge_JNET(df_wholeday_large_stacked, df_wholeday_JNET_large_stacked)

    # 225ミニ
    df_wholeday_mini = df_wholeday[(df_wholeday["銘柄"]=="日経225（ミニ）") & (df_wholeday["先物/OP"] == "先物")]

    # 限月の抽出
    df_wholeday_mini = get_gengetsu(df_wholeday_mini)

    # サイズの確認
    for g in df_wholeday_mini["限月"].unique():
        shape = df_wholeday_mini[df_wholeday_mini["限月"] == g].shape
        
        text = f"日中225ミニの第{g}限月のデータサイズは{shape}"
        print(text)

    # 225ミニJNET
    df_wholeday_JNET_mini = df_wholeday_JNET[(df_wholeday_JNET["銘柄"]=="日経225（ミニ）") & (df_wholeday_JNET["先物/OP"] == "先物")]

    # 限月の抽出
    df_wholeday_JNET_mini = get_gengetsu(df_wholeday_JNET_mini)

    # サイズの確認
    for g in df_wholeday_JNET_mini["限月"].unique():
        shape = df_wholeday_JNET_mini[df_wholeday_JNET_mini["限月"] == g].shape
        
        text = f"日中225ミニJNETの第{g}限月のデータサイズは{shape}"
        print(text)

    df_wholeday_mini_stacked = make_long_df(df_wholeday_mini)
    df_wholeday_JNET_mini_stacked = make_long_df(df_wholeday_JNET_mini)
    df_wholeday_mini_sum = merge_JNET(df_wholeday_mini_stacked, df_wholeday_JNET_mini_stacked)

    # TOPIX
    df_wholeday_topix = df_wholeday[(df_wholeday["銘柄"]=="TOPIX") & (df_wholeday["先物/OP"] == "先物")]

    # 限月の抽出
    df_wholeday_topix = get_gengetsu(df_wholeday_topix)

    # サイズの確認
    for g in df_wholeday_topix["限月"].unique():
        shape = df_wholeday_topix[df_wholeday_topix["限月"] == g].shape
        
        text = f"日中TOPIXの第{g}限月のデータサイズは{shape}"
        print(text)

    df_wholeday_JNET_topix = df_wholeday_JNET[(df_wholeday_JNET["銘柄"]=="TOPIX") & (df_wholeday_JNET["先物/OP"] == "先物")]

    # 限月の抽出
    df_wholeday_JNET_topix = get_gengetsu(df_wholeday_JNET_topix)

    # サイズの確認
    for g in df_wholeday_JNET_topix["限月"].unique():
        shape = df_wholeday_JNET_topix[df_wholeday_JNET_topix["限月"] == g].shape
        
        text = f"日中TOPIX、JNETの第{g}限月のデータサイズは{shape}"
        print(text)

    df_wholeday_topix_stacked = make_long_df(df_wholeday_topix)
    df_wholeday_JNET_topix_stacked = make_long_df(df_wholeday_JNET_topix)
    df_wholeday_topix_sum = merge_JNET(df_wholeday_topix_stacked, df_wholeday_JNET_topix_stacked)

    ### 4-3. ナイトセッションの立会とJNETを統合する
    print("4-3. ナイトセッションの統合…")
    #　ナイト立会
    df_night_large = df_night[(df_night["銘柄"]=="日経225（ラージ）") & (df_night["先物/OP"] == "先物")]

    # 限月の抽出
    df_night_large = get_gengetsu(df_night_large)

    # サイズの確認
    for g in df_night_large["限月"].unique():
        shape = df_night_large[df_night_large["限月"] == g].shape
        
    text = f"ナイト日経225ラージの第{g}限月のデータサイズは{shape}"
    print(text)

    #　ナイトJNET
    df_night_JNET_large = df_night_JNET[(df_night_JNET["銘柄"]=="日経225（ラージ）") & (df_night_JNET["先物/OP"] == "先物")]

    # 限月の抽出
    df_night_JNET_large = get_gengetsu(df_night_JNET_large)

    # サイズの確認
    for g in df_night_JNET_large["限月"].unique():
        shape = df_night_JNET_large[df_night_JNET_large["限月"] == g].shape
        
        text = f"ナイト日経225ラージJNETの第{g}限月のデータサイズは{shape}"
        print(text)
        
    df_night_large_stacked = make_long_df(df_night_large)
    df_night_JNET_large_stacked = make_long_df(df_night_JNET_large)
    df_night_large_sum = merge_JNET(df_night_large_stacked, df_night_JNET_large_stacked)

    # 225ミニ
    #　ナイト立会
    df_night_mini = df_night[(df_night["銘柄"]=="日経225（ミニ）") & (df_night["先物/OP"] == "先物")]

    # 限月の抽出
    df_night_mini = get_gengetsu(df_night_mini)

    # サイズの確認
    for g in df_night_mini["限月"].unique():
        shape = df_night_mini[df_night_mini["限月"] == g].shape
        
        text = f"ナイト日経225ミニの第{g}限月のデータサイズは{shape}"
        print(text)

    #　ナイトJNET
    df_night_JNET_mini = df_night[(df_night["銘柄"]=="日経225（ミニ）") & (df_night["先物/OP"] == "先物")]

    # 限月の抽出
    df_night_JNET_mini = get_gengetsu(df_night_JNET_mini)

    # サイズの確認
    for g in df_night_JNET_mini["限月"].unique():
        shape = df_night_JNET_mini[df_night_JNET_mini["限月"] == g].shape
        
        text = f"ナイト日経225ミニJNETの第{g}限月のデータサイズは{shape}"
        print(text)

    df_night_mini_stacked = make_long_df(df_night_mini)
    df_night_JNET_mini_stacked = make_long_df(df_night_JNET_mini)
    df_night_mini_sum = merge_JNET(df_night_mini_stacked, df_night_JNET_mini_stacked)

    # TOPIX
    #　ナイト立会
    df_night_topix = df_night[(df_night["銘柄"]=="TOPIX") & (df_night["先物/OP"] == "先物")]

    # 限月の抽出
    df_night_topix = get_gengetsu(df_night_topix)

    # サイズの確認
    for g in df_night_topix["限月"].unique():
        shape = df_night_topix[df_night_topix["限月"] == g].shape
        
        text = f"ナイトTOPIXの第{g}限月のデータサイズは{shape}"
        print(text)

    #　ナイトJNET
    df_night_JNET_topix = df_night[(df_night["銘柄"]=="TOPIX") & (df_night["先物/OP"] == "先物")]

    # 限月の抽出
    df_night_JNET_topix = get_gengetsu(df_night_JNET_topix)

    # サイズの確認
    for g in df_night_JNET_topix["限月"].unique():
        shape = df_night_JNET_topix[df_night_JNET_topix["限月"] == g].shape
        
        text = f"ナイトTOPIXのJNETの第{g}限月のデータサイズは{shape}"
        print(text)

    df_night_topix_stacked = make_long_df(df_night_topix)
    df_night_JNET_topix_stacked = make_long_df(df_night_JNET_topix)
    df_night_topix_sum = merge_JNET(df_night_topix_stacked, df_night_JNET_topix_stacked)

    ## 5. 銘柄ごとに日中取引の情報をナイトから補う
    text = "\n5. 銘柄ごとに日中取引の情報をナイトから補います"
    print(text)

    ### 5-2. 情報を補完する
    # 日経225ラージ
    df_wholeday_large_sum_full = complement_night(df_day=df_wholeday_large_sum, df_night=df_night_large_sum)

    # 日経225ミニ
    df_wholeday_mini_sum_full = complement_night(df_day=df_wholeday_mini_sum, df_night=df_night_mini_sum)
    # 日経225ミニの値を1/10する
    df_wholeday_mini_sum_full["volume"] = df_wholeday_mini_sum_full["volume"] / 10

    # TOPIX
    df_wholeday_topix_sum_full = complement_night(df_day=df_wholeday_topix_sum, df_night=df_night_topix_sum)
    print("df_wholeday_topix_sum_full colnames", df_wholeday_topix_sum_full.columns)

    ## 6. 銘柄別で全限月の合計を出す
    text = "\n6. 銘柄別で全限月の合計を計算"
    print(text)

    df_wide_total = pd.concat([df_wholeday_large_sum_full, df_wholeday_mini_sum_full, df_wholeday_topix_sum_full])

    # Appending groups
    df_group = pd.read_excel(PATH_GROUP, converters={'group':str,'institutions_code':str})
    df_wide_total = pd.merge(df_wide_total, df_group, how='left')

    ### 6-1. 銘柄ごとに売り・買いを横並びにして差を計算する
    # ラージ
    df_wholeday_large_total = df_wholeday_large_sum_full.groupby(["institutions_code", 
                                                                "sell_buy"]).agg({"institutions":max,
                                                                                "institutions_eng":max,
                                                                                "JPX_code":max,
                                                                                "instrument":max,
                                                                                "date":max,
                                                                                "先物/OP":max,
                                                                                "銘柄":max,
                                                                                "volume":sum}).reset_index()
    df_large_pivoted = df_wholeday_large_total.pivot(index='institutions_eng', columns='sell_buy', values='volume')
    df_large_pivoted = df_large_pivoted.reset_index().rename_axis(None, axis=1)
    df_large_pivoted["diff"] = df_large_pivoted["buy"] - df_large_pivoted["sell"]
    df_large_pivoted["product"] = df_wholeday_large_total['銘柄'].unique()[0] # modification
    df_large_pivoted["date"] = day.strftime("%Y-%m-%d")

    # ミニ
    df_wholeday_mini_total = df_wholeday_mini_sum_full.groupby(["institutions_code", 
                                                            "sell_buy"]).agg({"institutions":max,
                                                                                "institutions_eng":max,
                                                                                "JPX_code":max,
                                                                                "instrument":max,
                                                                                "date":max,
                                                                                "先物/OP":max,
                                                                                "銘柄":max,
                                                                                "volume":sum}).reset_index()
    df_mini_pivoted = df_wholeday_mini_total.pivot(index='institutions_eng', columns='sell_buy', values='volume')
    df_mini_pivoted = df_mini_pivoted.reset_index().rename_axis(None, axis=1)
    df_mini_pivoted["diff"] = df_mini_pivoted["buy"] - df_mini_pivoted["sell"]   
    df_mini_pivoted["product"] = df_wholeday_mini_total['銘柄'].unique()[0] # modification    
    df_mini_pivoted["date"] = day.strftime("%Y-%m-%d")

    # TOPIX
    df_wholeday_topix_total = df_wholeday_topix_sum_full.groupby(["institutions_code", 
                                                                "sell_buy"]).agg({"institutions":max,
                                                                                "institutions_eng":max,
                                                                                "JPX_code":max,
                                                                                "instrument":max,
                                                                                "date":max,
                                                                                "先物/OP":max,
                                                                                "銘柄":max,
                                                                                "volume":sum}).reset_index()

    df_topix_pivoted = df_wholeday_topix_total.pivot(index='institutions_eng', columns='sell_buy', values='volume')
    df_topix_pivoted = df_topix_pivoted.reset_index().rename_axis(None, axis=1)
    df_topix_pivoted["diff"] = df_topix_pivoted["buy"] - df_topix_pivoted["sell"]
    df_topix_pivoted["product"] = df_wholeday_topix_total['銘柄'].unique()[0] # modification
    df_topix_pivoted["date"] = day.strftime("%Y-%m-%d")

    ### 6-2. 銘柄別のデータフレームを横並びに統合する      
    df_total_final = pd.concat([dfi.set_index('institutions_eng') for dfi in [df_large_pivoted, df_mini_pivoted, df_topix_pivoted]], 
                            axis=0, # modification
                            sort=False).reset_index()

    # 日経ラージとミニの合計を元DFに足す
    df_nikkei_sum = df_total_final[df_total_final["product"]!="TOPIX"].groupby('institutions_eng').agg({"buy":sum, "sell":sum, "diff":sum, "date":max}).reset_index()
    df_nikkei_sum['product'] = "Nikkei sum"
    df_total_final = pd.concat([df_total_final, df_nikkei_sum], sort=False)

    # 総合計を元DFに足す
    def multiply_TOPIX(number): 
        return TOPIX_MULTI * number

    df_topix = df_total_final[df_total_final['product']=='TOPIX']
    df_topix[["buy", "sell", "diff"]] = df_topix[["buy", "sell", "diff"]].apply(multiply_TOPIX)
    df_topix['product'] = "TOPIX adjusted"

    df_sum = pd.concat([df_nikkei_sum, df_topix], sort=False)

    df_sum = df_sum.groupby('institutions_eng').agg({"buy":sum, "sell":sum, "diff":sum, "date":max}).reset_index()
    df_sum["product"] = "total"

    df_total_final = pd.concat([df_total_final, df_sum], sort=False)
    df_total_final = pd.merge(df_total_final, df_group, how='left')

    df_by_group_product = df_total_final.groupby(["group", "product"]).agg({"buy":sum, "sell":sum, "diff":sum, "date":max}).reset_index()
    df_by_group = df_total_final.groupby(["group"]).agg({"buy":sum, "sell":sum, "diff":sum, "date":max}).reset_index()

    # appending historical data
    try:
        historical_by_group = pd.read_csv(HISTORICAL_GROUP)
        historical_by_group = pd.concat([historical_by_group, df_by_group], sort=False)
        historical_by_group = historical_by_group[["date", "group", "buy", "sell", "diff"]]
        historical_by_group = historical_by_group.sort_values(["group", "date"])
    except:
        historical_by_group = df_by_group
    historical_by_group.to_csv("historical_by_group.csv")

    try:
        historical_by_group_product = pd.read_csv(HISTORICAL_GROU_PRODUCT)
        historical_by_group_product = pd.concat([historical_by_group_product, df_by_group_product], sort=False)
        historical_by_group_product = historical_by_group_product[["product", "group", "date", "buy", "sell", "diff"]]
        historical_by_group_product = historical_by_group_product.sort_values(["product", "group", "date"])
    except:
        historical_by_group_product = df_by_group_product
    historical_by_group_product.to_csv("historical_by_group_product.csv")

    ## 9. CSV/Excelファイルに保存
    text = "\n9. Excelファイルに保存します"
    print(text)

    filename = FOLDER_FINAL + "historical_by_group.csv"
    to_csv_on_s3(historical_by_group, bucketName=BUCKET_NAME, fileName=filename)

    filename = FOLDER_FINAL + "historical_by_group_product.csv"
    to_csv_on_s3(historical_by_group_product, bucketName=BUCKET_NAME, fileName=filename)

    filename = FOLDER_FINAL + "df_total_final" + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".csv"
    to_csv_on_s3(df_total_final, bucketName=BUCKET_NAME, fileName=filename, index=True)

    filename = FOLDER_FINAL + "df_wide_total" + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".csv"
    to_csv_on_s3(df_wide_total, bucketName=BUCKET_NAME, fileName=filename, index=True)