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
package com.github.jcustenborder.kafka.conversion.attunity.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;

public class Data {
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

  public enum Operation {
    REFRESH,
    INSERT,
    UPDATE,
    DELETE
  }

  public static class Headers {
    @JsonProperty
    Operation operation;
    @JsonProperty
    String changeSequence;
    @JsonProperty
    String timestamp;
    @JsonProperty
    String streamPosition;
    @JsonProperty
    String transactionId;
    @JsonProperty
    String changeMask;
    @JsonProperty
    String columnMask;
    @JsonProperty
    String externalSchemaId;

    public Operation operation() {
      return this.operation;
    }

    public String changeSequence() {
      return this.changeSequence;
    }

    public String timestamp() {
      return this.timestamp;
    }

    public String streamPosition() {
      return this.streamPosition;
    }

    public String transactionId() {
      return this.transactionId;
    }

    public String changeMask() {
      return this.changeMask;
    }

    public String columnMask() {
      return this.columnMask;
    }

    public String externalSchemaId() {
      return this.externalSchemaId;
    }
  }

  public static class Message {
    @JsonProperty
    Map<String, JsonNode> data;
    @JsonProperty
    Map<String, JsonNode> beforeData;
    @JsonProperty
    Headers headers;

    public Map<String, JsonNode> data() {
      return this.data;
    }

    public Map<String, JsonNode> beforeData() {
      return this.beforeData;
    }

    public Headers headers() {
      return this.headers;
    }

    public boolean isDelete() {
      return Operation.DELETE == this.headers.operation;
    }

  }
}
