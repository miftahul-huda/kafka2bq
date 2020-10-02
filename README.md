# kafka2bq
Read topic from Kafka and insert the data to BigQuery. Using Python Flask.

## Installation

1. Install Python & pip
2. Install Python library requirements: pip install -r requirements.txt

## Run the application

1. Make sure you create the credential to insert BigQuery and download the credential.
Set the credential to use Google BigQuery : export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/credential/json/file


2. Run the Twitter Analysis Server:
python server.py

3. Open the web browser and point to http://localhost:5555/web

