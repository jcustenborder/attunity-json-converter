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

import com.github.jcustenborder.kafka.conversion.attunity.model.Data;
import com.github.jcustenborder.kafka.conversion.attunity.model.Metadata;

public class ConversionRequest {
  public final Data data;
  public final Metadata metadata;

  ConversionRequest(Data data, Metadata metadata) {
    this.data = data;
    this.metadata = metadata;
  }

  public static ConversionRequest of(Data data, Metadata metadata) {
    return new ConversionRequest(data, metadata);
  }

  public boolean isDelete() {
    return data.message().isDelete();
  }
}
