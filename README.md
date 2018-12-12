# Introduction

This project provides a Kafka Streams application that will convert JSON data emitted by Attunity. 
This project utilizes the converter system from Kafka Connect to allow the output format 
to be pluggable by the user. The main goal of this project was to convert the data written by Attunity
and persist that data into Kafka topics based on the schema and table name. The key of each record is 
based on the keys of the table. The value is a struct representation of the data in the table. In the 
case of a delete, the value is a null. 




## Getting Started 

Your Attunity job needs to write data to two topics. The metadata and the data topic. The information 
about the schemas is written to the metadata topic, and the data topic is the actual data encoded 
as JSON. 


This will build the code and the docker container. 

```bash
mvn clean package
```

You must create the output topics before running the container. I recommend creating the Attunity data
topic a head of time. The number of partitions directly correlates with the number of containers 
we can use to process the data. [Read this](https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster) 
to learn how to select the number of partitions for a topic.

```bash
kafka-topics --zookeeper zookeeper:2181 --create --topic employees.salaries --partitions 1 --replication-factor 1
```

Launch the container running the conversion job. 

```bash
docker run --rm -e ATTUNITY_BOOTSTRAP_SERVERS=kafka:9092 \
    -e ATTUNITY_SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
    -e ATTUNITY_ATTUNITY_RECORD_NAMESPACE=com.github.jcustenborder.example \
    -e ATTUNITY_ATTUNITY_DATA_TOPIC=data \
    -e ATTUNITY_ATTUNITY_METADATA_TOPIC=metadata \
    -e ATTUNITY_ATTUNITY_OUTPUT_TOPIC_PREFIX=cdc. \
    attunity-json-conversion
```