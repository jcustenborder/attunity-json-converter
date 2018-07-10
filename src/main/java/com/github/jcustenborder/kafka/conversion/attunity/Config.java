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

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class Config extends AbstractConfig {

  public static final String RECORD_NAMESPACE_CONFIG = "attunity.record.namespace";
  public static final String RECORD_NAMESPACE_DOC = "The namespace to create the output records in.";
  public static final String INCLUDE_SCHEMA_CONFIG = "attunity.record.namespace.include.schema";
  public static final String INCLUDE_SCHEMA_DOC = "Flag to determine if the schema should be appended to the namespace.";
  public static final String DATA_TOPIC_CONFIG = "attunity.data.topic";
  public static final String DATA_TOPIC_DOC = "Topic that contains the data.";
  public static final String METADATA_TOPIC_CONFIG = "attunity.metadata.topic";
  public static final String METADATA_TOPIC_DOC = "Topic that contains the metadata for the topic.";
  public static final String OUTPUT_TOPIC_PREFIX_CONFIG = "attunity.output.topic.prefix";
  public static final String OUTPUT_TOPIC_PREFIX_DOC = "Topic that contains the metadata for the topic.";
  public static final String TABLES_CONFIG = "attunity.tables";
  public static final String TABLES_DOC = "Tables to convert.";
  public final String recordNamespace;
  public final boolean includeSchema;
  public final CaseFormat tableInputFormat = CaseFormat.LOWER_UNDERSCORE;
  public final CaseFormat tableOutputFormat = CaseFormat.LOWER_UNDERSCORE;
  public final String dataTopic;
  public final String metadataTopic;
  public final String outputTopicPrefix;
  public final List<String> tables;

  public Config(Map<?, ?> originals) {
    super(config(), originals);
    this.recordNamespace = getString(RECORD_NAMESPACE_CONFIG);
    this.includeSchema = getBoolean(INCLUDE_SCHEMA_CONFIG);
    this.dataTopic = getString(DATA_TOPIC_CONFIG);
    this.metadataTopic = getString(DATA_TOPIC_CONFIG);
    this.outputTopicPrefix = getString(OUTPUT_TOPIC_PREFIX_CONFIG);
    this.tables = getList(TABLES_CONFIG);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(RECORD_NAMESPACE_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, RECORD_NAMESPACE_DOC)
        .define(INCLUDE_SCHEMA_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH, INCLUDE_SCHEMA_DOC)
        .define(DATA_TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DATA_TOPIC_DOC)
        .define(METADATA_TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, METADATA_TOPIC_DOC)
        .define(OUTPUT_TOPIC_PREFIX_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, OUTPUT_TOPIC_PREFIX_DOC)
        .define(TABLES_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, TABLES_DOC);
  }

  public <T extends Enum<T>> T getEnum(Class<T> enumClass, String key) {
    Preconditions.checkNotNull(enumClass, "enumClass cannot be null");
    Preconditions.checkState(enumClass.isEnum(), "enumClass must be an enum.");
    String textValue = this.getString(key);
    return Enum.valueOf(enumClass, textValue);
  }
}
