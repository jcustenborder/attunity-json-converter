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
package com.github.jcustenborder.kafka.conversion.attunity.streams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

public class StructSerde implements Serde<Struct> {
  private final Converter converter;
  private final StructSerializer serializer;
  private final StructDeserializer deserializer;

  private StructSerde(Converter converter) {
    this.converter = converter;
    this.serializer = new StructSerializer(this.converter);
    this.deserializer = new StructDeserializer(this.converter);
  }

  public static final StructSerde of(Converter converter) {
    return new StructSerde(converter);
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {
    this.serializer.configure(map, b);
    this.deserializer.configure(map, b);
  }

  @Override
  public void close() {

  }

  @Override
  public Serializer<Struct> serializer() {
    return this.serializer;
  }

  @Override
  public Deserializer<Struct> deserializer() {
    return this.deserializer;
  }

  static class StructSerializer implements Serializer<Struct> {
    private final Converter converter;

    StructSerializer(Converter converter) {
      this.converter = converter;
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
      this.converter.configure(map, b);
    }

    @Override
    public byte[] serialize(String topic, Struct struct) {
      if (null != struct) {
        return this.converter.fromConnectData(topic, struct.schema(), struct);
      } else {
        return null;
      }
    }

    @Override
    public void close() {

    }
  }

  static class StructDeserializer implements Deserializer<Struct> {
    private final Converter converter;

    StructDeserializer(Converter converter) {
      this.converter = converter;
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
      this.converter.configure(map, b);
    }

    @Override
    public Struct deserialize(String topic, byte[] bytes) {
      SchemaAndValue value = this.converter.toConnectData(topic, bytes);

      if (null != value) {
        return (Struct) value.value();
      } else {
        return null;
      }
    }

    @Override
    public void close() {

    }
  }
}
