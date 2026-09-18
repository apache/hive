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

import java.util.Arrays;
import java.util.Objects;

/** Compressed or opaque bytes stored on a Lucene document but not indexed for search. */
public final class BinaryField implements Field {
  private final String name;
  private final byte[] value;

  public BinaryField(String name, byte[] value) {
    if (name == null || name.isBlank()) {
      throw new IllegalArgumentException("binary field name is required");
    }
    Objects.requireNonNull(value, "binary field value");
    this.name = name;
    this.value = value;
  }

  @Override
  public String name() {
    return name;
  }

  public byte[] value() {
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof BinaryField that)) {
      return false;
    }
    return name.equals(that.name) && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, Arrays.hashCode(value));
  }
}
