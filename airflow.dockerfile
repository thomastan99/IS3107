FROM apache/airflow:2.5.1

RUN pip install openpyxl praw telethon tweepy google-cloud-core==1.5.0
RUN pip install pytrends --upgrade
