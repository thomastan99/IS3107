# IS3107

# Credentials

## GMAIL & GCS & MongoDB:

username: nus3107crypto@gmail.com

password: crypto12345

## TWITTER:

username: @nus3107crypto

password: crypto12345

## REDDIT:

username: @crypto3107

password: crypto12345

## Airflow

### Steps to Start Airflow

1. Ensure virtual environment on local device is set up

```
virtualenv env
source env/bin/activate
```

2. Run Docker commands to start airflow services on localhost:8080

```
docker-compose build
docker-compose up airflow-init
docker-compose up -d
```

3. Navigate to airflow webserver
   1. go to localhost:8080
   2. username: airflow, password: airflow

### Existing DAGs

1. `extract_transform_load_pipeline`

   - This is the main dag to be run for our ETL pipeline
   - Scheduled to run daily
   - Extracts batch data from coincap (Quantitative), google, reddit and twitter (Qualitative)
   - WIP: Transforms data
   - WIP: Loads data

2. `stream_twitter_dag`

   - Consists of 1 task which handles continuous streaming of real time Twitter data into Google BigQuery
