# Introduction

This project provides a Kafka Streams application that will convert JSON data emitted by Attunity. 
This project utilizes the converter system from Kafka Connect to allow the output format 
to be pluggable by the user. The main goal of this project was to convert the data written by Attunity
and persist that data into Kafka topics based on the schema and table name. The key of each record is 
based on the keys of the table. The value is a struct representation of the data in the table. In the 
case of a delete, the value is a null. 

```bash
mvn clean pacakge
```



```bash
kafka-topics --zookeeper zookeeper:2181 --create --topic employees.salaries --partitions 1 --replication-factor 1
```


```bash
docker run --rm -e ATTUNITY_BOOTSTRAP_SERVERS=kafka:9092 \
    -e ATTUNITY_SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
    -e ATTUNITY_ATTUNITY_RECORD_NAMESPACE=com.github.jcustenborder.example \
    -e ATTUNITY_ATTUNITY_DATA_TOPIC=data \
    -e ATTUNITY_ATTUNITY_METADATA_TOPIC=metadata \
    -e ATTUNITY_ATTUNITY_OUTPUT_TOPIC_PREFIX=cdc. \
    attunity-json-conversion
```

```bash
cat src/test/resources/com/github/jcustenborder/kafka/conversion/attunity/model/Metadata.json | jq -c . | kafka-console-producer --property "parse.key=true" --property "key.separator=~" --broker-list kafka:9092 --topic metadata

cat src/test/resources/com/github/jcustenborder/kafka/conversion/attunity/model/Data.json | jq -c . | kafka-console-producer --broker-list kafka:9092 --topic data



```