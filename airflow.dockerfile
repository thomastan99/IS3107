FROM apache/airflow:2.5.1

RUN pip install openpyxl praw telethon tweepy yfinance google-cloud-core==1.5.0 pandas pandas-gbq nltk
RUN pip3 install snscrape
RUN python -m nltk.downloader vader_lexicon
RUN pip install pytrends --upgrade
