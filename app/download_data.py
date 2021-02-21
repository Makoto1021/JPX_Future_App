import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import timedelta
import io
import requests
import re
from bs4 import BeautifulSoup
from functools import partial, reduce
from to_csv_on_s3 import to_csv_on_s3


def get_url(datatype, day):
    """
    datatypeの値は1~4のいずれかをとる。
        1: ナイト・セッション、立会取引
        2: ナイト・セッション、JNET取引
        3: 日中、立会取引
        4: 日中、JNET取引
    """
    main_page_url = "https://www.jpx.co.jp/markets/derivatives/participant-volume/index.html"
    date = str(day.year) + '{:02d}'.format(day.month) + '{:02d}'.format(day.day)
    response = requests.get(main_page_url)
    
    if datatype == 1:
        csv = "volume_by_participant_night.csv"
        download_url = "%s_%s"%(date, csv)
    elif datatype == 2:
        csv = "volume_by_participant_night_J-NET.csv"
        download_url = "%s_%s"%(date, csv)
    elif datatype == 3:
        csv = "volume_by_participant_whole_day.csv"
        download_url = "%s_%s"%(date, csv)
    elif datatype == 4:
        csv = "volume_by_participant_whole_day_J-NET.csv"
        download_url = "%s_%s"%(date, csv)
    else:
        download_url == None
    
    if response.ok:
        soup = BeautifulSoup(response.text, "html.parser")
        for a in soup.findAll('a', href=re.compile(csv)):
            if download_url in a["href"]:
                url =  "https://www.jpx.co.jp/" + a["href"]
                return url
            else:
                url = None
        if url == None:
            print("ダウンロードできるデータが見つかりません")
            text = "No data"
            return text
    else:
        print("メインページが見つかりません")
        text = "No page"
        return text


# URLからCSVを読み込み
def get_csv(request, colnames):
    df = pd.read_csv(io.StringIO(request.decode('utf-8')), header=0, names = colnames, usecols=[0,1, 2, 3, 4, 5, 6, 7])
    return df

# 読み込んだCSVをきれいに整理
def clean_dataframe(df, day):
    
    code_indices = []
    
    for index, row in df.iterrows():
        if row['institutions_sell_code'] == 'JPX Code':
            code_indices.append(index)
            
    df['JPX_code'] = np.nan
    df['instrument'] = np.nan
    for first, second in zip(code_indices, code_indices[1:]):

        code = df['institutions_sell'][first]
        df.loc[df.index[first:second], 'JPX_code'] = code
        inst = df['institutions_sell'][first+1]
        df.loc[df.index[first:second], 'instrument'] = inst

    last = code_indices[-1]
    code = df['institutions_sell'][last]
    inst = df['institutions_sell'][last+1]
    df.loc[df.index[last:], 'JPX_code'] = code
    df.loc[df.index[last:], 'instrument'] = inst
    
    df = df.drop(df[df['institutions_sell_code']=="JPX Code"].index)
    df = df.drop(df[df['institutions_sell_code']=="Instrument"].index)
    # df['date'] = dt.date(day.year, day.month, day.day)
    df['date'] = day.strftime("%Y-%m-%d")
    df.reset_index(drop=True)
    
    return df


def format_data(datatype, day, colnames, FOLDER_RAW, BUCKET_NAME):
    dict = {1: "ナイト立会取引", 2:"ナイトJNET取引", 3:"日中立会取引", 4:"日中JNET取引"}
    url = get_url(datatype=datatype, day=day)
    if url == "No data":
        # 休日などでファイルがない場合はエラーメッセージを返します
        text = "%s年%s月%s日の%sが見つかりません"%(day.year, day.month, day.day, dict[datatype])
        print(text)
    elif url == "No page":
        text = "メインページが見つかりません。https://www.jpx.co.jp/markets/derivatives/participant-volume/index.html　を確認してください"
        print(text)
    elif requests.get(url).ok:
        s=requests.get(url).content
        df = clean_dataframe(get_csv(s, colnames = colnames), day = day)
        filename = FOLDER_RAW + dict[datatype] + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".csv"
        to_csv_on_s3(df, bucketName=BUCKET_NAME, fileName=filename)
        text = "%s年%s月%s日の%sをダウンロードしました"%(day.year, day.month, day.day, dict[datatype])
        print(text)
    else:
        text = "何らかの理由でデータが見つかりません"
        print(text)
    return df