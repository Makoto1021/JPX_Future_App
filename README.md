# JPX_Future_App
App for analysing data from Japanese Future trading


# Data source

Data is downloaded from JPX official website. https://www.jpx.co.jp/markets/derivatives/participant-volume/index.html

# Usage
## 1. Activate virtual environment

In terminal, run 

`conda create --name jpx_env python=3.6`

`conda activate jpx_env`

`pip install -r requirements.txt`

## 2. Run the app
In terminal, run `python jpx_future.py`

It opens tkinter app. Click "データを取得する" to get the calculated table. 
If you want to get the past data, change the value in "何日前のデータを取得しますか？". For data from the previous day, enter `1`, for two days ago, enter `2`...

Raw data will be saved in "元データ" folder in `.xlsx` format. The calculated table will be saved in the folder "完成データ" in `.xlsx`.

Run once a day. 
