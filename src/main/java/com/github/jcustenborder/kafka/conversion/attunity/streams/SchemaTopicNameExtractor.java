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
package com.github.jcustenborder.kafka.conversion.attunity.streams;

import com.github.jcustenborder.kafka.conversion.attunity.Constants;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SchemaTopicNameExtractor implements TopicNameExtractor<Struct, Struct> {
  Map<Schema, String> schemaToTopicLookup = new ConcurrentHashMap<>();

  @Override
  public String extract(Struct key, Struct value, RecordContext recordContext) {
    final String result = this.schemaToTopicLookup.computeIfAbsent(key.schema(), schema -> {
      final String schemaName = schema.parameters().get(Constants.PARM_SCHEMA);
      final String tableName = schema.parameters().get(Constants.PARM_TABLE);
      return String.format("%s.%s", schemaName, tableName).toLowerCase();
    });
    return result;
  }
}
