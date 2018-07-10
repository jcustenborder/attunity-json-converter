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
import com.google.common.base.Preconditions;
import org.apache.kafka.connect.data.SchemaBuilder;

public abstract class ColumnConverter {
  protected final Config config;
  protected final Metadata.Column column;
  protected final String columnName;

  public ColumnConverter(Config config, Metadata.Column column, String columnName) {
    this.config = config;
    this.columnName = columnName;
    Preconditions.checkNotNull(column, "column cannot be null.");
    this.column = column;
  }

  public Metadata.Column column() {
    return this.column;
  }

  public String columnName() {
    return this.columnName;
  }

  public String fieldName() {
    return this.columnName;
  }

  public void addField(SchemaBuilder builder) {
    final SchemaBuilder fieldBuilder = schemaBuilder();
    builder.parameters(column.properties(this.columnName));
    if (column.isnullable()) {
      builder.optional();
    }
    builder.field(fieldName(), fieldBuilder.build());
  }

  protected abstract SchemaBuilder schemaBuilder();

  public abstract Object convert(JsonNode node);
}
