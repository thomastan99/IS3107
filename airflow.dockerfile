FROM apache/airflow:2.5.1

RUN pip install openpyxl praw telethon tweepy yfinance google-cloud-core==1.5.0 pandas==0.23.3 and pandas-gbq==0.5.0
RUN pip install pytrends --upgrade
