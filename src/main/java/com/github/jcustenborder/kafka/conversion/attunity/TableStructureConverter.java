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

import com.github.jcustenborder.kafka.conversion.attunity.converters.ColumnConverter;
import com.github.jcustenborder.kafka.conversion.attunity.model.Metadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;

public class TableStructureConverter {
  private static final Logger log = LoggerFactory.getLogger(TableStructureConverter.class);
  private final Config config;

  public TableStructureConverter(Config config) {
    this.config = config;
  }

  String recordNamespace(Metadata.Message message) {
    StringBuilder builder = new StringBuilder();
    builder.append(this.config.recordNamespace);
    if (this.config.includeSchema) {
      builder.append('.');
      builder.append(message.lineage().schema().toLowerCase());
    }
    return builder.toString();
  }

  String valueRecordName(Metadata.Message message) {
    StringBuilder builder = new StringBuilder();
    builder.append(this.config.tableInputFormat.to(
        this.config.tableOutputFormat,
        message.lineage().table()
    ));
    builder.append("Value");
    return builder.toString();
  }

  String keyRecordName(Metadata.Message message) {
    StringBuilder builder = new StringBuilder();
    builder.append(this.config.tableInputFormat.to(
        this.config.tableOutputFormat,
        message.lineage().table()
    ));
    builder.append("Key");
    return builder.toString();
  }

  public Schema buildValue(Metadata metadata, List<ColumnConverter> converters) {
    String recordNamespace = recordNamespace(metadata.message());
    String recordName = valueRecordName(metadata.message());
    String name = String.format("%s.%s", recordNamespace, recordName);
    log.trace("buildValue() - Creating builder for {}", name);

    SchemaBuilder builder = SchemaBuilder.struct()
        .parameters(metadata.parameters())
        .name(name);

    for (ColumnConverter converter : converters) {
      log.trace("buildValue() - Adding {} to schema", converter.columnName());
      converter.addField(builder);
    }

    return builder.build();
  }

  public Schema buildKey(Metadata metadata, List<ColumnConverter> converters) {
    String recordNamespace = recordNamespace(metadata.message());
    String recordName = keyRecordName(metadata.message());
    String name = String.format("%s.%s", recordNamespace, recordName);
    log.trace("buildKey() - Creating builder for {}", name);

    SchemaBuilder builder = SchemaBuilder.struct()
        .parameters(metadata.parameters())
        .name(name);

    converters.stream()
        .filter(c -> c.column().primaryKeyPosition() > 0)
        .sorted(Comparator.comparingInt(c2 -> c2.column().primaryKeyPosition()))
        .forEach(c -> {
          log.trace("buildKey() - Adding {} to schema", c.columnName());
          c.addField(builder);
        });

    return builder.build();
  }
}
