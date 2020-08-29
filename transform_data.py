import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import timedelta
import io
import requests
import re
from bs4 import BeautifulSoup
from functools import partial, reduce

def fut_or_op(code):
    code = str(code)
    if code[1:2] == "3" or code[1:2] == "8":
        return "プット"
    elif code[1:2] == "4" or code[1:2] == "9":
        return "コール"
    elif code[1:2] == "6":
        return "先物"
    else:
        return np.nan


def get_meigara(code):
    code = str(code)
    if code[-2:] == "05":
        return "TOPIX"
    elif code[-2:] == "18":
        return "日経225（ラージ）"
    elif code[-2:] == "19":
        return "日経225（ミニ）"
    else:
        return np.nan


# 限月を抽出するFunction
def get_gengetsu_year(code):
    code = str(code)
    year = 2015 + int(code[2:3])
    return year

def get_gengetsu_month(code):
    code = str(code)
    gengetsu = int(code[3:5])
    return gengetsu

def get_gengetsu(df):
    
    df["限月_月"] = df.JPX_code.apply(get_gengetsu_month)
    df["限月_年"] = df.JPX_code.apply(get_gengetsu_year)

    arr = np.sort(df["限月_月"].unique())
    d = {}
    i = 1
    for g in arr:
        d.update({g:i})
        i += 1
    
    df["限月"] = df["限月_月"].replace(d, inplace=False)
    
    return df

# 売りと買いを縦に積み重ねるFunction
def make_long_df(df):

    # データフレームを売りと買いにわける
    df_sell = df[["institutions_sell_code", "institutions_sell", "institutions_sell_eng", 
                                     "volume_sell", "JPX_code", "instrument", "date",
                                     "先物/OP", "銘柄", "限月_月", "限月_年", "限月"]]

    df_buy = df[["institutions_buy_code", "institutions_buy","institutions_buy_eng", 
                                     "volume_buy", "JPX_code", "instrument", "date",
                                    "先物/OP", "銘柄", "限月_月", "限月_年", "限月"]]

    #　カラム名を売りと買いで揃える
    df_sell = df_sell.rename({"institutions_sell_code":"institutions_code", 
                              "institutions_sell":"institutions", 
                              "institutions_sell_eng":"institutions_eng",
                              "volume_sell":"volume"}, axis = 1)

    df_buy = df_buy.rename({"institutions_buy_code":"institutions_code", 
                              "institutions_buy":"institutions", 
                              "institutions_buy_eng":"institutions_eng",
                              "volume_buy":"volume"}, axis = 1)

    #　売り・買いがわかるよう新しい変数をつくる
    df_sell["sell_buy"] = "sell"
    df_buy["sell_buy"] = "buy"
    
    # 積み重ねる
    df_stacked = pd.concat([df_sell, df_buy], ignore_index=True)
    
    # 空の行を落とす
    ind = df_stacked[df_stacked["institutions_code"] == "-"].index.values
    df_stacked = df_stacked.drop(ind)

    return df_stacked

# JNETを立会取引に統合するFunction
def merge_JNET(df, df_JNET):
    """
    JNETのデータフレームを立会取引のデータフレームに統合する
    
    パラメーター
    df: 立会取引のデータフレーム
    df_JNET:　JNETのデータフレーム

    注意：
    dfとdf_JNETは同じ銘柄のものを使うこと。
    dfとdf_JNETにはmake_long_dfのfunctionを適用してから使うこと。
    """
    
    df_sum = df.copy(deep=True)
    df_sum["volume"] = df_sum["volume"].astype(int)
    df_JNET["volume"] = df_JNET["volume"].astype(int)

    df_JNET = df_JNET.reset_index(drop=True)
    for ind in df_JNET.index:

        row = df_JNET.iloc[ind, :]
        inst_code = row["institutions_code"]
        year = row["限月_年"]
        month = row["限月_月"]
        sell_buy = row["sell_buy"]
        volume = row["volume"]

        match = df_sum[(df_sum["institutions_code"] == inst_code) & \
                                  (df_sum["限月_年"] == year) & \
                                  (df_sum["限月_月"]== month) & \
                                  (df_sum["sell_buy"] == sell_buy)]

        if match.shape[0] != 0:
            df_sum.loc[(df_sum["institutions_code"] == inst_code) & \
                                  (df_sum["限月_年"] == year) & \
                                  (df_sum["限月_月"]== month) & \
                                  (df_sum["sell_buy"] == sell_buy), "volume"] = (match.volume + volume)
    return df_sum

def complement_night(df_day, df_night):
    print("補完前の日中データサイズ：", df_day.shape)
    for code in df_day.institutions_code.unique():
        temp = df_day[df_day.institutions_code == code]
        for g in temp['限月'].tolist():
            temp2 = temp[temp['限月']==g]

            if temp2.shape[0] < 2:
                which = temp2.sell_buy.values[0]
                # print("what it has is", which)
                missing = ["sell", "buy"]
                missing.remove(which)
                year = temp2["限月_年"].values
                month = temp2["限月_月"].values
                # print("missing is", missing[0])
                print(f"会社コード{code}の第{g}限月には{missing[0]}がない")

                #限月でなく年と月で詳しく見ないとずれる可能性がある
                #日中とナイトの第一限月が同じ月とは限らないため
                row = df_night[(df_night["institutions_code"]==code) & \
                                         (df_night["限月_年"]==year[0]) & \
                                         (df_night["限月_月"]==month[0]) & \
                                         (df_night["sell_buy"]==missing[0])]
                

                if row.shape[0] != 0:
                    value = row['volume'].values[0]
                    tocopy = df_day[(df_day["institutions_code"]==code) & \
                                                   (df_day["限月"]==g)]
                    tocopy["volume"] = value
                    tocopy["sell_buy"] = missing
                    df_day = df_day.append(tocopy, ignore_index=True)
                    print(f"会社コード{code}の第{g}限月の{missing[0]}を{value}で補完した")
                else:
                    print("ナイト・セッションには該当データなし")
            else:
                print("欠損データなし")
                
        print("補完後の日中データサイズ：", df_day.shape)
    return df_day

def get_wide(df):
    df_wide = df.set_index(['institutions', '限月','sell_buy'])['volume'].unstack('限月').unstack('sell_buy')
    list_gengetsu = df_wide.columns.get_level_values(0).unique().tolist()
    for g in list_gengetsu:
        df_wide[g, 'diff'] =  df_wide[g, 'buy'] - df_wide[g, 'sell']
    df_wide["合計", 'buy'] = df_wide.iloc[:, df_wide.columns.get_level_values(1)=='buy'].sum(axis=1)
    df_wide["合計", 'sell'] = df_wide.iloc[:, df_wide.columns.get_level_values(1)=='sell'].sum(axis=1)
    df_wide["合計", 'diff'] = df_wide.iloc[:, df_wide.columns.get_level_values(1)=='diff'].sum(axis=1)
    df_wide = df_wide.sort_index(axis=1)
    df_wide = df_wide.rename(columns={'sell': "売り", 'buy': "買い", 'diff':"差引"}, level=1)
    new_cols = ["売り", "買い", "差引"]
    df_wide = df_wide.reindex(new_cols, axis=1, level=1)
    df_wide.index.names = ["取引参加者"]
    return df_wide