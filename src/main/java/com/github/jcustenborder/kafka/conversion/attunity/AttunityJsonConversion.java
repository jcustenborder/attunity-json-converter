/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.conversion.attunity;

import com.github.jcustenborder.DockerProperties;
import com.github.jcustenborder.kafka.conversion.attunity.model.Data;
import com.github.jcustenborder.kafka.conversion.attunity.model.Metadata;
import com.github.jcustenborder.kafka.conversion.attunity.streams.SchemaTopicNameExtractor;
import com.github.jcustenborder.kafka.conversion.attunity.streams.StructSerde;
import com.github.jcustenborder.kafka.serialization.jackson.JacksonSerde;
import com.google.common.base.Preconditions;
import io.confluent.connect.avro.AvroConverter;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class AttunityJsonConversion {
  static final String METADATA_STORE = "metadata-store";
  static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
  private static final Logger log = LoggerFactory.getLogger(AttunityJsonConversion.class);

  public static void main(String... args) throws Exception {
    DockerProperties<String> dockerProperties = DockerProperties.builder()
        .patterns("^ATTUNITY_(.+)$")
        .environment(System.getenv())
        .build();
    final Map<String, String> settings = dockerProperties.toMap();
    Preconditions.checkState(
        settings.containsKey(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG),
        "You must set %s",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
    );
    Preconditions.checkState(
        settings.containsKey(SCHEMA_REGISTRY_URL_CONFIG),
        "You must set %s",
        SCHEMA_REGISTRY_URL_CONFIG
    );
    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "convert-attunity");

    final StreamsConfig streamsConfig = new StreamsConfig(settings);

    final Config config = new Config(settings);
    final StructSerde keySerde = StructSerde.of(new AvroConverter());
    keySerde.configure(settings, true);
    final StructSerde valueSerde = StructSerde.of(new AvroConverter());
    valueSerde.configure(settings, false);
    final JacksonSerde<Metadata> metadataSerde = JacksonSerde.of(Metadata.class);
    final JacksonSerde<Data> dataSerde = JacksonSerde.of(Data.class);

    final StreamsBuilder streamsBuilder = new StreamsBuilder();


    final GlobalKTable<String, Metadata> metadataTable = streamsBuilder.globalTable(
        config.metadataTopic,
        Materialized.<String, Metadata, KeyValueStore<Bytes, byte[]>>as(METADATA_STORE)
            .withKeySerde(Serdes.String())
            .withValueSerde(metadataSerde)
    );

    final StructKeyValueMapper converter = new StructKeyValueMapper(config);
    final SchemaTopicNameExtractor topicNameExtractor = new SchemaTopicNameExtractor();

    //At this point we don't care about the key. All the data we need is in the value.
    final KStream<byte[], Data> dataStream = streamsBuilder.stream(config.dataTopic,
        Consumed.with(Serdes.ByteArray(), dataSerde)
    );

    dataStream.join(metadataTable,
        (bytes, data) -> data.messageSchemaId(),
        ConversionRequest::of
    ).map(converter)
        .to(topicNameExtractor, Produced.with(keySerde, valueSerde));

    Topology topology = streamsBuilder.build();


    KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
    streams.start();
    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

}
