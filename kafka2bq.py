from confluent_kafka import Consumer, KafkaError
from confluent_kafka import TopicPartition
import os
import subprocess
import sys
import argparse
import numpy as np
from google.oauth2 import service_account
from google.cloud import bigquery
import json
from datetime import datetime
import time

class Empty:
    configuration= None


class KafkaConsumer:
    settings = {
        'bootstrap.servers': '',
        'group.id': '',
        'client.id': '',
        'enable.auto.commit': True,
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'}
    }

    messageCounter= 0

    buffer=[]

    doneWaiting =  False

    startTime = None

    def read_config_from_file(self, config_file):
        file = config_file
        f = open(file, "r")
        lines = f.readlines()

        o = self.parse_config(lines)
        self.settings['bootstrap.servers'] = o.bootstrapServer
        self.settings['group.id'] = o.groupId
        self.settings['client.id'] = o.clientId

        return o

    def parse_config(self, lines):
        o = Empty()
        o.partition = 0
        o.credential =  None

        counter = 0
        while counter < len(lines):
            line = lines[counter]
            if("#" in line or ""):
                counter = counter + 1
                continue

            elif("-bootstrap-server " in line):
                s = line.replace("-bootstrap-server ", "")
                s = s.strip()
                o.bootstrapServer = s
            elif("-group-id " in line):
                s = line.replace("-group-id ", "")
                s = s.strip()
                o.groupId = s
            elif("-partition " in line):
                s = line.replace("-partition ", "")
                s = s.strip()
                o.partition = int(s)

            elif("-client-id " in line):
                s = line.replace("-client-id ", "")
                s = s.strip()
                o.clientId = s

            elif("-credential " in line):
                s = line.replace("-credential ", "")
                s = s.strip()
                o.credential = s

            elif("-kafka-topic " in line):
                s = line.replace("-kafka-topic ", "")
                s = s.strip()
                o.kafkaTopic = s

            elif("-project " in line):
                s = line.replace("-project ", "")
                s = s.strip()
                o.project = s

            elif("-output-bq-table " in line):
                s = line.replace("-output-bq-table ", "")
                s = s.strip()
                o.outputBqTable = s

            elif("-rows-insert-num " in line):
                s = line.replace("-rows-insert-num ", "")
                s = s.strip()
                o.rowsInsertNum = int(s)

            elif("-max-time-waiting " in line):
                s = line.replace("-max-time-waiting ", "")
                s = s.strip()
                o.maxTimeWaiting = int(s)

            counter = counter + 1
        return o


    def get_client(self, key_path=None, project_id=None):
        if(key_path is None):
            client = bigquery.Client(project_id)
        else:
            credentials = service_account.Credentials.from_service_account_file(
                    key_path,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            client = bigquery.Client(
                    credentials=credentials,
                    project=credentials.project_id,
            )
        return client

    def get_datetime(self):
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        return dt_string

    def process_to_bq(self, msg, config):

        if(msg is not None):
            now = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
            msg = msg.replace("NOW()", now)
            item = json.loads(msg )
            #print(item)
            self.buffer.append(item)
            self.messageCounter = self.messageCounter + 1

        if((self.messageCounter > config.rowsInsertNum and len(self.buffer) > 0)) :
            self.save(config)
        else:
            if(msg is None):
                self.save(config)

    def save(self,  config):
        if(len(self.buffer) > 0):
            stime = self.get_datetime()
            print(stime + " : " + config.clientId +  " : Saving to " + config.outputBqTable)
            startTime = datetime.now()
            #self.print_all(self.buffer)
            errors = self.save_data(self.buffer, config)
            errors = []

            stime = self.get_datetime()
            print(stime + " : " + config.clientId +  " :Total rows  : " + str(len( self.buffer )))

            if(len(errors) == 0):
                self.messageCounter = 0
                self.buffer = []
                endtime = datetime.now()
                print(stime +  " : " + config.clientId +  " : Data has been saved in " + str(endtime - startTime))
                print("\n\n")
            else:
                print(stime +  " : " + config.clientId +  " : Error saving to BigQuery")
                print(errors)
                print(stime +  " : " + config.clientId +  " : Waiting for 3 seconds...")
                time.sleep(3)

    def print_all(self, arr):
        for item in arr:
            print(item)

    def save_data(self, result, config):

        project_id = config.project
        credential = config.credential
        table_id = config.outputBqTable
        client = self.get_client(credential, project_id)

        table = client.get_table(table_id)  # API request
        errors = client.insert_rows(table, result)  # API request
        return errors



    def process(self, config):
        c = Consumer(self.settings)
        c.subscribe([config.kafkaTopic])

        #topic_partition = TopicPartition(config.kafkaTopic, config.partition)
        #c.assign([topic_partition])

        try:
            while True:
                msg = c.poll(0.1)
                if msg is None:

                    self.process_to_bq(None, config)

                    continue
                elif not msg.error():
                    #dt_object = datetime.fromtimestamp(float(msg.timestamp()))
                    #print(config.clientId +  ' : Received message at: {0}'.format(msg.timestamp()))
                    #print(msg.value())
                    self.process_to_bq(msg.value(), config)

                elif msg.error().code() == KafkaError._PARTITION_EOF:
                    print(config.clientId +  ' : End of partition reached {0}/{1}'
                          .format(msg.topic(), msg.partition()))
                else:
                    print(config.clientId +  ' :Error occured: {0}'.format(msg.error().str()))

        except KeyboardInterrupt:
            pass

        finally:
            c.close()

    def start(self, description):
        parser = argparse.ArgumentParser(description=description)
        required = parser.add_argument_group("Required argument")
        required.add_argument('-f', '--file', type=str, help="configuration filename", required=True)
        args = parser.parse_args()
        o = self.read_config_from_file(args.file)
        print("====================================")
        print("Kafka bootstrap : " + o.bootstrapServer )
        print("Kafka Topic : " + o.kafkaTopic )
        print("Client Group : " + o.groupId )
        print("Client id : " + o.clientId )
        print("BigQuery table : " + o.outputBqTable )
        print("====================================")
        self.process(o)

if __name__ == "__main__":
    o = KafkaConsumer()
    o.start("Consume Kafka to BigQuery")
