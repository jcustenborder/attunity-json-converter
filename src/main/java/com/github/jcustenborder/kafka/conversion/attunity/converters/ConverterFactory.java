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

import com.github.jcustenborder.kafka.conversion.attunity.Config;
import com.github.jcustenborder.kafka.conversion.attunity.model.Metadata;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConverterFactory {
  private final Config config;

  public ConverterFactory(Config config) {
    this.config = config;
  }


  public List<ColumnConverter> createConverters(Metadata.TableStructure tableStructure) {
    List<ColumnConverter> result = new ArrayList<>();

    for (Map.Entry<String, Metadata.Column> kvp : tableStructure.tableColumns().entrySet()) {
      result.add(createConverter(kvp.getKey(), kvp.getValue()));
    }

    return ImmutableList.copyOf(result);
  }

  private ColumnConverter createConverter(String columnName, Metadata.Column column) {
    ColumnConverter result;
    switch (column.type()) {
      case INT2:
        result = new Int2ColumnConverter(config, column, columnName);
        break;
      case INT4:
        result = new Int4ColumnConverter(config, column, columnName);
        break;
      case INT8:
        result = new Int8ColumnConverter(config, column, columnName);
        break;
      case REAL4:
        result = new Real4ColumnConverter(config, column, columnName);
        break;
      case REAL8:
        result = new Real8ColumnConverter(config, column, columnName);
        break;
      case TIME:
        result = new TimeColumnConverter(config, column, columnName);
        break;
      case DATE:
        result = new DateColumnConverter(config, column, columnName);
        break;
      case DATETIME:
        result = new DateTimeColumnConverter(config, column, columnName);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("%s(%s) is not a supported type.", columnName, column.type())
        );
    }

    return result;
  }

}
