import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import timedelta
import io
import requests
import re
from bs4 import BeautifulSoup
from functools import partial, reduce
from download_data import get_url, clean_dataframe, get_csv
from transform_data import *
import os
import glob
import boto3
from to_csv_on_s3 import to_csv_on_s3

## 1-2. パラメーターの設定
# 元データをダウンロードするためのフォルダ
# reading s3 bucket
client = boto3.client('s3')

dirname = os.getcwd()
# FOLDER_RAW = os.path.join(dirname, '元データ/')
BUCKET_NAME = 'jpx-future-bucket'
FOLDER_RAW = "raw_data/"

# 完成したデータを保存するためのフォルダ
# FOLDER_FINAL = os.path.join(dirname, '完成データ/')
# FOLDER_FINAL = 's3://jpx-future-bucket/完成データ/'
FOLDER_FINAL = "final_data/"

# 企業のグループ分けに使うExcelファイル
PATH_GROUP = os.path.join(dirname, '企業名グループ.xlsx')

# TOPIXに掛ける係数（225合計と合わせて総合計を計算する際に使う）
TOPIX_MULTI = 1.0

def jpx_future():

    days = 0
    print("days", days)
    print("TOPIX_MULTI", TOPIX_MULTI)

    day = dt.today() - timedelta(days=days)
    colnames = ['institutions_sell_code', 'institutions_sell', 
            'institutions_sell_eng', 'volume_sell', 'institutions_buy_code', 
            'institutions_buy', 'institutions_buy_eng', 'volume_buy']

    print("2. データのダウンロード")
    
    ### 2-2. 日中立会取引データのダウンロード
    url_wholeday = get_url(datatype=3, day=day)
    if url_wholeday == "No data":
        # 休日などでファイルがない場合はエラーメッセージを返します
        text = "%s年%s月%s日の日中立会取引データが見つかりません"%(day.year, day.month, day.day)
        print(text)
    elif url_wholeday == "No page":
        text = "メインページが見つかりません。https://www.jpx.co.jp/markets/derivatives/participant-volume/index.html　を確認してください"
        print(text)
    elif requests.get(url_wholeday).ok:
        s=requests.get(url_wholeday).content
        df_wholeday = clean_dataframe(get_csv(s, colnames = colnames), day = day)
        filename = FOLDER_RAW + "日中立会取引" + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".csv"
        # df_wholeday.to_csv(filename)
        to_csv_on_s3(df_wholeday, bucketName=BUCKET_NAME, fileName=filename)
        text = "%s年%s月%s日の日中立会取引データをダウンロードしました"%(day.year, day.month, day.day)
        print(text)
    else:
        text = "何らかの理由でデータが見つかりません"
        print(text)
    
    ### 2-3. 日中JNET取引データのダウンロード
    url_wholeday_JNET = get_url(datatype=4, day=day)
    if url_wholeday == "No data":
        # 休日などでファイルがない場合はエラーメッセージを返します
        text = "%s年%s月%s日の日中JNET取引データが見つかりません"%(day.year, day.month, day.day)
        print(text)
    elif url_wholeday == "No page":
        text = "メインページが見つかりません。https://www.jpx.co.jp/markets/derivatives/participant-volume/index.html　を確認してください"
        print(text)
    elif requests.get(url_wholeday_JNET).ok:
        s=requests.get(url_wholeday_JNET).content
        df_wholeday_JNET = clean_dataframe(get_csv(s, colnames = colnames), day = day)
        filename = FOLDER_RAW + "日中JNET取引" + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".csv"
        # df_wholeday_JNET.to_csv(filename)
        to_csv_on_s3(df_wholeday_JNET, bucketName=BUCKET_NAME, fileName=filename)
        text = "%s年%s月%s日の日中JNET取引データをダウンロードしました"%(day.year, day.month, day.day)
        print(text)
    else:
        text = "何らかの理由でデータが見つかりません"
        print(text)
    
    ### 2-4. ナイト立会取引データのダウンロード
    url_night = get_url(datatype=1, day=day)
    if url_wholeday == "No data":
        # 休日などでファイルがない場合はエラーメッセージを返します
        text = "%s年%s月%s日のナイト立会取引データが見つかりません"%(day.year, day.month, day.day)
        print(text)
    elif url_wholeday == "No page":
        text = "メインページが見つかりません。https://www.jpx.co.jp/markets/derivatives/participant-volume/index.html　を確認してください"
        print(text)
    elif requests.get(url_night).ok:
        s=requests.get(url_night).content
        df_night = clean_dataframe(get_csv(s, colnames = colnames), day = day)
        filename = FOLDER_RAW + "ナイト立会取引" + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".csv"
        # df_night.to_csv(filename)
        to_csv_on_s3(df_night, bucketName=BUCKET_NAME, fileName=filename)
        text = "%s年%s月%s日のナイト立会取引データをダウンロードしました"%(day.year, day.month, day.day)
        print(text)
    else:
        text = "何らかの理由でデータが見つかりません"
        print(text)
    
    ### 2-5. ナイトJNET取引データのダウンロード
    url_night_JNET = get_url(datatype=2, day=day)
    if url_wholeday == "No data":
        # 休日などでファイルがない場合はエラーメッセージを返します
        text = "%s年%s月%s日のナイトJNET取引データが見つかりません"%(day.year, day.month, day.day)
        print(text)
    elif url_wholeday == "No page":
        text = "メインページが見つかりません。https://www.jpx.co.jp/markets/derivatives/participant-volume/index.html　を確認してください"
        print(text)
    elif requests.get(url_night_JNET).ok:
        s=requests.get(url_night_JNET).content
        df_night_JNET = clean_dataframe(get_csv(s, colnames = colnames), day = day)
        filename = FOLDER_RAW + "ナイトJNET取引" + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".csv"
        # df_night_JNET.to_csv(filename)
        to_csv_on_s3(df_night_JNET, bucketName=BUCKET_NAME, fileName=filename)
        text = "%s年%s月%s日のナイトJNET取引データをダウンロードしました"%(day.year, day.month, day.day)
        print(text)
    else:
        text = "何らかの理由でデータが見つかりません"
        print(text)
    
    
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
    df_large_pivoted = df_wholeday_large_total.pivot(index='institutions', columns='sell_buy', values='volume')
    df_large_pivoted = df_large_pivoted.reset_index().rename_axis(None, axis=1)
    df_large_pivoted["diff"] = df_large_pivoted["buy"] - df_large_pivoted["sell"]

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
    df_mini_pivoted = df_wholeday_mini_total.pivot(index='institutions', columns='sell_buy', values='volume')
    df_mini_pivoted = df_mini_pivoted.reset_index().rename_axis(None, axis=1)
    df_mini_pivoted["diff"] = df_mini_pivoted["buy"] - df_mini_pivoted["sell"]       

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

    df_topix_pivoted = df_wholeday_topix_total.pivot(index='institutions', columns='sell_buy', values='volume')
    df_topix_pivoted = df_topix_pivoted.reset_index().rename_axis(None, axis=1)
    df_topix_pivoted["diff"] = df_topix_pivoted["buy"] - df_topix_pivoted["sell"]
    print("df_topix_pivoted colnames", df_topix_pivoted.columns)

    ### 6-2. 銘柄別のデータフレームを横並びに統合する      
    df_total_final = pd.concat([dfi.set_index('institutions') for dfi in [df_large_pivoted, 
                                                              df_mini_pivoted, 
                                                              df_topix_pivoted]], 
                   keys=['日経225ラージ', 
                         '日経225ミニ', 
                         'Topix'], axis=1, sort=False).rename_axis(mapper=['銘柄', '売買'], axis=1).rename_axis(mapper=['institutions'], axis=0)   

    ### 6-3. 225ラージ・ミニの合計とTOPIXも合わせた総合計を出す
    df_total_final["日経225合計", "buy"] = df_total_final["日経225ラージ", "buy"] + df_total_final["日経225ミニ", "buy"]
    df_total_final["日経225合計", "sell"] = df_total_final["日経225ラージ", "sell"] + df_total_final["日経225ミニ", "sell"]
    df_total_final["日経225合計", "diff"] = df_total_final["日経225合計", "buy"] - df_total_final["日経225合計", "sell"]
    df_total_final["総合計", "buy"] = df_total_final["日経225合計", "buy"] + df_total_final["Topix", "buy"] * TOPIX_MULTI
    df_total_final["総合計", "sell"] = df_total_final["日経225合計", "sell"] + df_total_final["Topix", "sell"] * TOPIX_MULTI
    df_total_final["総合計", "diff"] = df_total_final["総合計", "buy"] - df_total_final["総合計", "sell"]
    df_total_final = df_total_final.rename(columns={'buy': "買い", 'sell': "売り", 'diff':"差引"})
    df_total_final = df_total_final.rename_axis(mapper=['取引参加者'], axis=0)

    ### 6-3. 企業をグループ分けする
    groups = pd.read_excel(PATH_GROUP)
    groups = groups.set_index(['企業名'])
    groups.columns = pd.MultiIndex.from_product([groups.columns, ['new']])
    df_final_with_group = df_total_final.merge(groups, left_index = True, right_index = True, how='left')
    text = "6-3. 企業をグループ分けしました"
    print(text)

    # リストにない企業はグループ４に振り分け
    df_final_with_group['グループ', 'new'] = df_final_with_group['グループ', 'new'].fillna(4).astype(int)
    df_final_with_group = df_final_with_group.set_index([('グループ','new')], append=True).rename_axis(['取引参加者','グループ'])
    row_total = df_final_with_group.sum()
    df_final_with_group.loc[("", "総合計"),:] = row_total
    reordered_cols = ["売り", "買い", "差引"]
    df_final_with_group = df_final_with_group.reindex(reordered_cols, axis=1, level=1)

    ### 6-4. グループ別のデータをつくる
    df_total_grouped = df_final_with_group.groupby(level=[1]).sum()
    sankasha = pd.DataFrame({"グループ":[1, 2, 3, 4], '取引参加者':["アメリカ系合計", "ヨーロッパ系合計", "国内大手合計", "ネット証券他合計"]})
    sankasha = sankasha.set_index('グループ')
    sankasha.columns = pd.MultiIndex.from_product([sankasha.columns, ['new']])
    df_total_grouped = df_total_grouped.merge(sankasha, 
                                           left_index=True, 
                                           right_index=True, 
                                           how='left').set_index([('取引参加者', 'new')], append=True).rename_axis(['グループ', '取引参加者'])

    row_obei = df_total_grouped[df_total_grouped.index.isin(["アメリカ系合計", "ヨーロッパ系合計"], level='取引参加者')].sum()
    row_kokunai = df_total_grouped[df_total_grouped.index.isin(["国内大手合計", "ネット証券他合計"], level='取引参加者')].sum()
    df_total_grouped.loc[("1+2", "欧米合計"),:] = row_obei
    df_total_grouped.loc[("3+4", "国内合計"),:] = row_kokunai
    df_total_grouped = df_total_grouped.rename(columns={'sell': "売り", 'buy': "買い", 'diff':"差引"})
    reordered_cols = ["売り", "買い", "差引"]
    df_total_grouped = df_total_grouped.reindex(reordered_cols, axis=1, level=1)

    ## 7. ラージ、ミニ、TOPIXで限月別の横並びデータをつくる
    text = "\n7. ラージ、ミニ、TOPIXで限月別の横並びデータを作成します"
    print(text)

    ### 7-2. 横並びデータをつくる      
    # 225ラージ
    df_wholeday_large_wide = get_wide(df_wholeday_large_sum_full)
    df_wholeday_large_wide = df_wholeday_large_wide.rename_axis(['限月', '売り買い'], axis=1)
    row_total = df_wholeday_large_wide.sum()
    df_wholeday_large_wide.loc[("総合計"),:] = row_total
    text = "225ラージのデータが完成しました"
    print(text)

    # 225ミニ
    df_wholeday_mini_wide = get_wide(df_wholeday_mini_sum_full)
    df_wholeday_mini_wide = df_wholeday_mini_wide.rename_axis(['限月', '売り買い'], axis=1)
    row_total = df_wholeday_mini_wide.sum()
    df_wholeday_mini_wide.loc[("総合計"),:] = row_total
    text = "225ミニのデータが完成しました"
    print(text)

    # TOPIX
    df_wholeday_topix_wide = get_wide(df_wholeday_topix_sum_full)
    df_wholeday_topix_wide = df_wholeday_topix_wide.rename_axis(['限月', '売り買い'], axis=1)
    row_total = df_wholeday_topix_wide.sum()
    df_wholeday_topix_wide.loc[("総合計"),:] = row_total
    text = "TOPIXのデータが完成しました"
    print(text)

    ## 8. 時系列データの作成
    text = "\n8. 時系列データを作成します"
    print(text)

    ### 8-1. 銘柄別
    df_today = df_total_grouped.iloc[:4, (df_total_grouped.columns.get_level_values(0).isin(['Topix', '日経225合計', "総合計"])) ]
    date = day.strftime("%Y-%m-%d")
    print("df_today.columns", df_today.columns)
    df_today.index = pd.MultiIndex.from_product([[date], df_today.index.get_level_values(1)])
    df_today = df_today.unstack().reorder_levels([2,0,1], axis=1).sort_index(axis=1)
    new_cols = ["売り", "買い", "差引"]
    df_today = df_today.reindex(new_cols, axis=1, level=2)
    

    # 時系列データの読み込み
    # df_history = pd.read_excel(FOLDER_FINAL + "時系列データ.xlsx", header=[0, 1, 2])
    df_history = pd.read_excel('s3://jpx-future-bucket/final_data/時系列データ.xlsx', header=[0, 1, 2])
    #matchedfiles = glob.glob(FOLDER_FINAL + '先物*.csv')
    #matchedfiles.sort()
    #latest_file = matchedfiles[-1] # 最も日付が新しいファイルを見つけて読み込み
    #df_history = pd.read_excel(latest_file, sheet_name="時系列", header=[0, 1, 2])
    

    #　読み込んだ時系列データに新しいデータを追加
    df_history.loc[day.strftime("%Y-%m-%d"), :] = df_today.squeeze()

    # 新しいデータが最後の行に来るよう並び替え
    print("df_history.index", df_history.index)
    df_history = df_history.sort_index(axis = 0)
    text = "銘柄別時系列データを作成しました"
    print(text)

    ### 8-2. グループ別合計
    df_today_total_group = df_today.xs('総合計', axis=1, level=1, drop_level=False)
    df_today_group = df_today_total_group.iloc[:, (df_today_total_group.columns.get_level_values(2).isin(["売り", "買い"]))]
    df_today_group = df_today_group.sum(axis=1, level=0)
    df_today_group.columns = pd.MultiIndex.from_product([df_today_group.columns, ['売り＋買い']])
    df_today_group = df_today_group.stack(level=0)
    df_today_group['前日比'] = 0
    df_today_group = df_today_group.unstack().swaplevel(0,1,axis=1).sort_index(axis=1)
    new_cols = ["売り＋買い", "前日比"]
    df_today_group = df_today_group.reindex(new_cols, axis=1, level=1)

    # 時系列データの読み込み
    # df_group_history = pd.read_excel(FOLDER_FINAL + "時系列グループ別合計データ.xlsx", header=[0, 1])
    df_group_history = pd.read_excel(latest_file, sheet_name="時系列グループ別合計", header=[0, 1])

    #　読み込んだ時系列データに新しい行を追加
    df_group_history.loc[day.strftime("%Y-%m-%d"), :] = df_today_group.squeeze()
    df_group_history = df_group_history.sort_index(axis = 0) 

    s = df_group_history.filter(like="売り＋買い").rename(columns={"売り＋買い":"前日比"}, level=1)
    res = ((s - s.shift(1))/s.shift(1))
    df_group_history.update(res)
    text = "グループ別時系列データを作成しました"
    print(text)

    ### 8-3. 欧米・国内合計
    df_region_total = df_group_history.copy(deep=True)
    df_region_total["欧米合計", '売り＋買い'] = df_region_total["アメリカ系合計", "売り＋買い"] + df_region_total["ヨーロッパ系合計", "売り＋買い"]
    df_region_total["国内合計", '売り＋買い'] = df_region_total["国内大手合計", "売り＋買い"] + df_region_total["ネット証券他合計", "売り＋買い"]

    df_region_total["欧米合計", '前日比'] = np.nan
    df_region_total["国内合計", '前日比'] = np.nan

    df_region_total["欧米合計", '構成比'] = df_region_total["欧米合計", '売り＋買い'] / (df_region_total["欧米合計", '売り＋買い'] + df_region_total["国内合計", '売り＋買い'])
    df_region_total["国内合計", '構成比'] = df_region_total["国内合計", '売り＋買い'] / (df_region_total["欧米合計", '売り＋買い'] + df_region_total["国内合計", '売り＋買い'])

    s = df_region_total.filter(like="売り＋買い").rename(columns={"売り＋買い":"前日比"}, level=1)
    res = ((s - s.shift(1))/s.shift(1))
    df_region_total.update(res)
    df_region_total = df_region_total.iloc[:, (df_region_total.columns.get_level_values(0).isin(['欧米合計', '国内合計']))]
    new_cols = ["欧米合計", "国内合計"]
    df_region_total = df_region_total.reindex(new_cols, axis=1, level=0)
    text = "欧米・国内合計時系列データを作成しました"
    print(text)

    ## 9. CSV/Excelファイルに保存
    text = "\n9. Excelファイルに保存します"
    print(text)

    
    filename = FOLDER_FINAL + "時系列地域別合計データ.csv"
    df_region_total.to_csv(filename)
    to_csv_on_s3(df_region_total, bucketName=BUCKET_NAME, fileName=filename)

    filename = FOLDER_FINAL + "時系列グループ別合計データ.csv"
    df_group_history.to_csv(filename)
    to_csv_on_s3(df_group_history, bucketName=BUCKET_NAME, fileName=filename)
    
    filename = FOLDER_FINAL + "時系列データ.csv"
    df_history.to_csv(filename)
    to_csv_on_s3(df_history, bucketName=BUCKET_NAME, fileName=filename)
    
    filename = FOLDER_FINAL + "225ラージ限月別内訳" + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".csv"
    df_wholeday_large_wide.to_csv(filename)
    to_csv_on_s3(df_wholeday_large_wide, bucketName=BUCKET_NAME, fileName=filename)

    filename = FOLDER_FINAL + "225ミニ限月別内訳" + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".csv"
    df_wholeday_mini_wide.to_csv(filename)
    to_csv_on_s3(df_wholeday_mini_wide, bucketName=BUCKET_NAME, fileName=filename)

    print("df_wholeday_topix_wide colnames", df_wholeday_topix_wide.columns)
    filename = FOLDER_FINAL + "TOPIX限月別内訳" + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".csv"
    df_wholeday_topix_wide.to_csv(filename)
    to_csv_on_s3(df_wholeday_topix_wide, bucketName=BUCKET_NAME, fileName=filename)

    filename = FOLDER_FINAL + "企業別総合計" + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".csv"
    df_final_with_group.to_csv(filename)
    to_csv_on_s3(df_final_with_group, bucketName=BUCKET_NAME, fileName=filename)

    filename = FOLDER_FINAL + "グループ別総合計" + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".csv"
    df_total_grouped.to_csv(filename)
    to_csv_on_s3(df_total_grouped, bucketName=BUCKET_NAME, fileName=filename)

    """
    filename = FOLDER_FINAL + "先物" + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".xlsx"
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    df_wholeday_large_wide.to_excel(writer, sheet_name='225ラージ限月別内訳')
    df_wholeday_mini_wide.to_excel(writer, sheet_name='225ミニ限月別内訳')
    df_wholeday_topix_wide.to_excel(writer, sheet_name='TOPIX限月別内訳')
    df_final_with_group.to_excel(writer, sheet_name='企業別総合計')
    df_total_grouped.to_excel(writer, sheet_name='グループ別総合計')
    df_history.to_excel(writer, sheet_name='時系列')
    df_group_history.to_excel(writer, sheet_name='時系列グループ別合計')
    df_region_total.to_excel(writer, sheet_name='時系列地域別合計')
    writer.save()
    """

    text = "Excelファイルの保存が完了しました"
    print(text)

    text = "\n画面を閉じてください"
    print(text)