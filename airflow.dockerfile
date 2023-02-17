FROM apache/airflow:2.5.1

RUN pip install openpyxl pytrends praw telethon tweepy
