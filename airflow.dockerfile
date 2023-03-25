FROM apache/airflow:2.5.1

COPY airflow/hostname_resolver.py /tmp/ 
RUN cp /tmp/hostname_resolver.py $(pip show apache-airflow | grep ^Location | cut -d' ' -f2)/airflow/

RUN pip install openpyxl praw telethon tweepy yfinance google-cloud-core==1.5.0 pandas pandas-gbq
RUN pip install pytrends --upgrade
