/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.mapping.field;

import java.util.Objects;

public record TextField(String name, String value, float[] embedding) implements Field {
  public TextField(String name, String value) {
    this(name, value, null);
  }

  public TextField {
    if ("_id".equals(name)) {
      throw new IllegalArgumentException("use IdField for _id, not TextField");
    }
  }

  public TextField withEmbedding(float[] newEmbedding) {
    return new TextField(name, value, newEmbedding);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass())
      return false;
    TextField textField = (TextField) o;
    return Objects.equals(name, textField.name) && Objects.equals(value, textField.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }
}
