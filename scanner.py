# TODO - Instead of throwing out old polls, timestamp + merge them into a large historical data csv
# TODO - Poll NYSE stocks in addition to NASDAQ stocks



import pandas as pd
import time, os, logging
import requests

from twilio.rest import Client as TW_Client
from pprint import pprint
from os.path import join, dirname
from dotenv import load_dotenv

logging.basicConfig(filename="runtime.log", level=logging.INFO)
dotenv_path = join(dirname(__file__), ".env")
load_dotenv(dotenv_path)

DROP_TRIGGER =      0.01        # Desired drop percentage required to trigger an alert
DROP_GAP =          60          # Number of minutes between polls
MIN_MARKET_CAP =    2000000000  # Minimum market cap for polled stocks
SECTORS =           ["Technology", "Consumer Durables", "Finance", "Consumer Services", "Health Care"]

AV_API_KEYS = ["FQ5B9LTU9V93PR1F"]
TWILIO_ACCOUNT = os.environ.get("TWILIO_ACCOUNT")
TWILIO_TOKEN = os.environ.get("TWILIO_TOKEN")
PHONE_NUMBER = os.environ.get("PHONE_NUMBER")

twilio = TW_Client(TWILIO_ACCOUNT, TWILIO_TOKEN)


def main():

    keygen = av_keygen()
    df_nasdaq = pd.read_csv("./company_list_nasdaq.csv")
    df_nasdaq = df_nasdaq[(df_nasdaq["Sector"].isin(SECTORS)) & (df_nasdaq["MarketCap"] > MIN_MARKET_CAP)]
    df_nasdaq = df_nasdaq[["Sector", "MarketCap", "Symbol", "Name"]]
    symbols = df_nasdaq["Symbol"].values
    df2_nasdaq = data_for_symbols(symbols,keygen)
    df_nasdaq = pd.merge(df_nasdaq,df2_nasdaq, on="Symbol", how="inner")

    df_nyse = pd.read_csv("./company_list_nyse.csv")
    df_nyse = df_nyse[(df_nyse["Sector"].isin(SECTORS)) & (df_nyse["MarketCap"] > MIN_MARKET_CAP)]
    df_nyse = df_nyse[["Sector", "MarketCap", "Symbol", "Name"]]
    symbols = df_nyse["Symbol"].values
    df2_nyse = data_for_symbols(symbols,keygen)
    df_nyse = pd.merge(df_nyse,df2_nyse, on="Symbol", how="inner")

    df = df_nasdaq.append(df_nyse)
    try:
        last_df = pd.read_csv("last_poll_prices.csv")
        last_ts = last_df["Timestamp"].values.astype('datetime64[m]')[0]
        current_ts = df["Timestamp"].values.astype('datetime64[m]')[0]
        dt = (current_ts - last_ts).item().total_seconds()
        if (dt > (DROP_GAP-5)*60) and (dt < (DROP_GAP*2-5)*60):
            # time difference matches up, proceed to compare and text_alert
            last_df.rename(columns={"Price": "LastPrice"}, inplace=True)
            df3 = pd.merge(df,last_df, on="Symbol", how="inner")
            df3["PercentDrop"] = df3.apply(price_drop_for_row, axis=1)
            print(df3.head())
        # save new values as last price csv
    except Exception as e:
        log(e, log_type=logging.WARNING)
        
    df.to_csv("last_poll_prices.csv")
    wait_for_next_poll(DROP_GAP)


def price_drop_for_row(row):

    p2 = float(row["LastPrice"])
    p1 = float(row["Price"])
    if ((p2 > 0) and (p1 > 0)):
        percent_drop = (p2 - p1) / p2
        if percent_drop > DROP_TRIGGER:
            log(row["Symbol"] + ": has dropped " + str(round(percent_drop*100,2)) + "%" + " in the last hour", text_alert=True)
        return percent_drop
    else:
        return 0.0


def data_for_symbols(symbols, keygen):

    df = pd.DataFrame()
    while len(symbols) > 0:
        if len(symbols) > 100:
            symbols_string = str(symbols[:100])
            symbols = symbols[100:]
        else:
            symbols_string = str(symbols)
            symbols = []
        qstring = symbols_string.replace("'", "").replace(" ", ",")[1:-1]
        url = "https://www.alphavantage.co/query?function=BATCH_STOCK_QUOTES&symbols=" + qstring + "&apikey=" + AV_API_KEYS[0]
        res = requests.get(url).json()
        df2 = pd.DataFrame(res["Stock Quotes"])
        df = df.append(df2)
    df.rename(columns={"1. symbol": "Symbol", "2. price": "Price", "4. timestamp": "Timestamp"}, inplace=True)
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], format="%Y-%m-%d %H:%M")
    df = df.drop("3. volume", axis=1)

    return df


def av_keygen():

    counter, size = -1, len(AV_API_KEYS)
    def get():
        counter += 1
        return AV_API_KEYS[counter % size]
    return get


def send_text(msg):

    tw_message = twilio.messages.create(
        to=PHONE_NUMBER,
        from_="+14404343662",
        body=str(msg))


def wait_for_next_poll(mins):

    for i in range(mins):
        log("Polling again in " + str(mins-i) + " minutes.")
        time.sleep(60)


def log(msg, log_type=logging.INFO, text_alert=False):

    print(msg)
    if log_type == logging.WARNING:
        logging.warning(msg)
    else:
        logging.info(msg)
    if text_alert:
        send_text(msg)


if __name__ == "__main__":
    main()
