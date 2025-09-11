/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.serde2.typeinfo;

import com.google.common.collect.Iterables;

import java.util.List;

public class VariantVal {

  private final byte[] value;
  private final byte[] metadata;

  private VariantVal(List<Object> data) {
      this.metadata = convertToByteArray(data, 0);
      this.value = convertToByteArray(data, 1);
  }

  public static VariantVal from(List<Object> data) {
    return new VariantVal(data);
  }

  private static byte[] convertToByteArray(List<Object> data, int position) {
    Object obj = (data != null) ?
        Iterables.get(data, position, null) : null;
    return switch (obj) {
      case null -> null;
      case byte[] bytes -> bytes;
      case org.apache.hadoop.io.BytesWritable bytesWritable -> bytesWritable.getBytes();
      case org.apache.hadoop.io.Text text -> text.getBytes();
      default ->
        throw new IllegalArgumentException("Unsupported type for Variant field: " + obj.getClass());
    };
  }

  public byte[] getValue() {
    return value;
  }

  public byte[] getMetadata() {
    return metadata;
  }
}
