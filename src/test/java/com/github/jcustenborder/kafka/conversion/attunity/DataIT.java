package com.github.jcustenborder.kafka.conversion.attunity;

import com.github.jcustenborder.kafka.conversion.attunity.model.Data;
import com.github.jcustenborder.kafka.conversion.attunity.model.Metadata;
import com.github.jcustenborder.kafka.serialization.jackson.JacksonSerializer;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class DataIT {

  Data data;
  Metadata metadata;

  <T> T readResource(String name, Class<T> c) throws IOException {
    try (InputStream inputStream = this.getClass().getResourceAsStream(name)) {
      return ObjectMapperFactory.INSTANCE.readValue(inputStream, c);
    } catch (IOException ex) {
      throw new IOException("Exception while loading " + name, ex);
    }
  }

  @BeforeEach
  public void before() throws IOException {
    data = readResource("/com/github/jcustenborder/kafka/conversion/attunity/model/Data.json", Data.class);
    metadata = readResource("/com/github/jcustenborder/kafka/conversion/attunity/model/Metadata.json", Metadata.class);
  }


  @Test
  @Disabled
  public void foo() {
    Map<String, Object> settings = ImmutableMap.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonSerializer.class.getName()
    );

    ProducerRecord metadataRecord = new ProducerRecord(
        "metadata",
        metadata.messageSchemaId(),
        metadata
    );

    ProducerRecord dataRecord = new ProducerRecord(
        "data",
        "asdfa",
        data
    );

    Producer<String, Object> producer = new KafkaProducer<String, Object>(settings);
    producer.send(metadataRecord);
    for (int i = 0; i < 100; i++) {
      producer.send(dataRecord);
    }
    producer.flush();
    producer.close();
  }

}
