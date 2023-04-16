FROM apache/airflow:2.5.1

RUN pip install openpyxl praw telethon tweepy yfinance google-cloud-core==1.5.0 
RUN pip install pandas pandas-gbq nltk apache-airflow-providers-papermill seaborn matplotlib pandas-ta
RUN pip install tensorflow 
RUN pip install scikit-learn
RUN pip3 install snscrape
RUN python -m nltk.downloader vader_lexicon
RUN pip install pytrends --upgrade
RUN pip install --upgrade pip ipython ipykernel
RUN ipython kernel install --name "python3" --user
