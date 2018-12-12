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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.github.jcustenborder.kafka.conversion.attunity.converters.ColumnConverter;
import com.github.jcustenborder.kafka.conversion.attunity.converters.ConverterFactory;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StructKeyValueMapper implements KeyValueMapper<byte[], ConversionRequest, KeyValue<Struct, Struct>> {
  private static final Logger log = LoggerFactory.getLogger(StructKeyValueMapper.class);
  private final Config config;
  private final ConverterFactory converterFactory;
  private final Cache<String, State> schemaCache;
  private final TableStructureConverter tableStructureConverter;


  public StructKeyValueMapper(Config config) {
    this.config = config;
    this.converterFactory = new ConverterFactory(config);
    this.tableStructureConverter = new TableStructureConverter(config);
    this.schemaCache = CacheBuilder.newBuilder()
        .expireAfterAccess(5, TimeUnit.MINUTES)
        .build();
  }

  public State lookupSchemas(ConversionRequest request) {
    try {
      return this.schemaCache.get(request.data.messageSchemaId(), () -> {
        log.info("Building state for {}", request.data.messageSchemaId());
        final List<ColumnConverter> converters = converterFactory.createConverters(
            request.metadata.message().tableStructure()
        );
        final Schema keySchema = tableStructureConverter.buildKey(request.metadata, converters);
        final Schema valueSchema = tableStructureConverter.buildValue(request.metadata, converters);
        return State.of(keySchema, valueSchema, converters);
      });
    } catch (ExecutionException e) {
      throw new IllegalStateException("Exception thrown while building schema state.", e);
    }
  }

  @Override
  public KeyValue<Struct, Struct> apply(byte[] bytes, ConversionRequest request) {
    State state = lookupSchemas(request);

    final Struct key = new Struct(state.key);

    final Struct value;

    if (request.isDelete()) {
      value = null;
    } else {
      value = new Struct(state.value);
    }

    log.debug("convert() - Converting key");

    for (Field field : state.key.fields()) {
      final ColumnConverter converter = state.converters.get(field.name());
      final JsonNode node = request.data.message().data().getOrDefault(
          converter.columnName(),
          NullNode.instance
      );
      final Object fieldValue = converter.convert(node);
      key.put(field.name(), fieldValue);
    }

    for (Field field : state.value.fields()) {
      final Object fieldValue;
      if (state.keyFields.contains(field.name())) {
        fieldValue = key.get(field.name());
      } else {
        final ColumnConverter converter = state.converters.get(field.name());
        final JsonNode node = request.data.message().data().getOrDefault(
            converter.columnName(),
            NullNode.instance
        );
        fieldValue = converter.convert(node);
      }
      value.put(field.name(), fieldValue);
    }

    return new KeyValue<>(key, value);
  }

  static class State {
    final Schema key;
    final Schema value;
    final Map<String, ColumnConverter> converters;
    final Set<String> keyFields;

    private State(Schema key, Schema value, List<ColumnConverter> converters) {
      this.key = key;
      this.value = value;
      this.converters = converters.stream()
          .collect(
              Collectors.toMap(ColumnConverter::fieldName, Function.identity())
          );
      this.keyFields = key.fields().stream()
          .map(Field::name)
          .collect(Collectors.toSet());

    }

    public static State of(Schema key, Schema value, List<ColumnConverter> converters) {
      return new State(key, value, converters);
    }
  }
}
