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
package com.github.jcustenborder.kafka.conversion.attunity.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.jcustenborder.kafka.conversion.attunity.Constants;
import com.google.common.collect.ImmutableMap;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class Metadata {
  @JsonProperty
  String magic;
  @JsonProperty
  String type;
  @JsonProperty
  Object headers;

  @JsonProperty
  String messageSchemaId;
  @JsonProperty
  String messageSchema;
  @JsonProperty
  Message message;

  public String magic() {
    return this.magic;
  }

  public String type() {
    return this.type;
  }

  public Object headers() {
    return this.headers;
  }

  public String messageSchemaId() {
    return this.messageSchemaId;
  }

  public String messageSchema() {
    return this.messageSchema;
  }

  public Message message() {
    return this.message;
  }


  public Map<String, String> parameters() {
    Map<String, String> result = new LinkedHashMap<>();
    result.put("attunity.message.schemaId", this.messageSchemaId());
    result.put("attunity.message.lineage.server", this.message().lineage().server());
    result.put(Constants.PARM_SCHEMA, this.message().lineage().schema());
    result.put(Constants.PARM_TABLE, this.message().lineage().table());
    result.put("attunity.message.lineage.task", this.message().lineage().task());
    result.put("attunity.message.lineage.tableVersion", Integer.toString(this.message().lineage().tableVersion()));
    return ImmutableMap.copyOf(result);
  }

  public enum ColumnType {
    INT4,
    DATE,
    INT2,
    INT8,
    NUMERIC,
    REAL8,
    REAL4,
    STRING,
    WSTRING,
    CLOB,
    BYTES,
    TIME,
    DATETIME
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
  public static class Message {
    @JsonProperty
    String schemaId;
    @JsonProperty
    Lineage lineage;
    @JsonProperty
    TableStructure tableStructure;
    @JsonProperty
    String dataSchema;

    public String schemaId() {
      return this.schemaId;
    }

    public Lineage lineage() {
      return this.lineage;
    }

    public TableStructure tableStructure() {
      return this.tableStructure;
    }

    public String dataSchema() {
      return this.dataSchema;
    }
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
  public static class Lineage {
    @JsonProperty
    String server;
    @JsonProperty
    String task;
    @JsonProperty
    String schema;
    @JsonProperty
    String table;
    @JsonProperty
    Integer tableVersion;
    @JsonProperty
    Date timestamp;

    public String server() {
      return this.server;
    }

    public String task() {
      return this.task;
    }

    public String schema() {
      return this.schema;
    }

    public String table() {
      return this.table;
    }

    public Integer tableVersion() {
      return this.tableVersion;
    }

    public Date timestamp() {
      return this.timestamp;
    }
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
  public static class TableStructure {
    @JsonProperty
    Map<String, Column> tableColumns;

    public Map<String, Column> tableColumns() {
      return this.tableColumns;
    }
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
  public static class Column {
    @JsonProperty
    int ordinal;
    @JsonProperty
    ColumnType type;
    @JsonProperty
    int length;
    @JsonProperty
    int precision;
    @JsonProperty
    int scale;
    @JsonProperty
    int primaryKeyPosition;
    @JsonProperty
    boolean nullable;

    public int ordinal() {
      return this.ordinal;
    }

    public ColumnType type() {
      return this.type;
    }

    public int length() {
      return this.length;
    }

    public int precision() {
      return this.precision;
    }

    public int scale() {
      return this.scale;
    }

    public int primaryKeyPosition() {
      return this.primaryKeyPosition;
    }

    public boolean isnullable() {
      return this.nullable;
    }

    public Map<String, String> properties(String columnName) {
      LinkedHashMap<String, String> result = new LinkedHashMap<>();
      result.put("attunity.column.name", columnName);
      result.put("attunity.column.length", Integer.toString(this.length()));
      result.put("attunity.column.ordinal", Integer.toString(this.ordinal()));
      result.put("attunity.column.precision", Integer.toString(this.precision()));
      result.put("attunity.column.primaryKeyPosition", Integer.toString(this.primaryKeyPosition()));
      result.put("attunity.column.scale", Integer.toString(this.scale()));
      result.put("attunity.column.type", this.type().toString());
      return ImmutableMap.copyOf(result);
    }
  }
}
