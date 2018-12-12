/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.conversion.attunity;

import com.github.jcustenborder.kafka.conversion.attunity.model.Data;
import com.github.jcustenborder.kafka.conversion.attunity.model.Metadata;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class StructKeyValueMapperTest {

  @Test
  public void foo() throws IOException {
    Config config = new Config(ImmutableMap.of(
        Config.RECORD_NAMESPACE_CONFIG, "com.foo",
        Config.METADATA_TOPIC_CONFIG, "metadata",
        Config.DATA_TOPIC_CONFIG, "data",
        Config.OUTPUT_TOPIC_PREFIX_CONFIG, "converted"
    ));
    StructKeyValueMapper converter = new StructKeyValueMapper(config);
    Metadata metadata;
    try (InputStream stream = this.getClass().getResourceAsStream("model/Metadata.json")) {
      metadata = ObjectMapperFactory.INSTANCE.readValue(stream, Metadata.class);
    }
    Data data;
    try (InputStream stream = this.getClass().getResourceAsStream("model/Data.json")) {
      data = ObjectMapperFactory.INSTANCE.readValue(stream, Data.class);
    }
    ConversionRequest request = ConversionRequest.of(data, metadata);

    KeyValue<Struct, Struct> record = converter.apply(null, request);
    assertNotNull(record);

  }

}
