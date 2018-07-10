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
package com.github.jcustenborder.kafka.conversion.attunity.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.jcustenborder.kafka.conversion.attunity.Config;
import com.github.jcustenborder.kafka.conversion.attunity.model.Metadata;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;

public class TimeColumnConverter extends ColumnConverter {

  public TimeColumnConverter(Config config, Metadata.Column column, String columnName) {
    super(config, column, columnName);
  }

  @Override
  protected SchemaBuilder schemaBuilder() {
    return Time.builder();
  }

  @Override
  public Object convert(JsonNode node) {
    if (node.isNull()) {
      return null;
    } else {
      return java.util.Date.from(
          LocalTime.parse(node.asText())
              .atDate(LocalDate.ofEpochDay(0))
              .toInstant(ZoneOffset.UTC)
      );
    }
  }
}
