/**
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

package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.ObjectOutputStream;
import java.util.Arrays;

/**
 * Size-optimized implementation of MapJoinKeyBase. MJK only needs to support equality and
 * hashCode, so for simple cases we can write the requisite writables that are part of the
 * key into byte array and retain the functionality without storing the writables themselves.
 */
@SuppressWarnings("deprecation")
public class MapJoinKeyBytes extends MapJoinKey {
  /**
   * First byte is field count. The rest is written using BinarySortableSerDe.
   */
  private byte[] array;

  private void setBytes(byte[] array) {
    this.array = array;
  }

  @Override
  public void write(MapJoinObjectSerDeContext context, ObjectOutputStream out) {
    throw new UnsupportedOperationException(this.getClass().getName() + " cannot be serialized");
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof MapJoinKeyBytes)) return false;
    MapJoinKeyBytes other = (MapJoinKeyBytes)obj;
    return Arrays.equals(this.array, other.array);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(array);
  }

  @Override
  public boolean hasAnyNulls(int fieldCount, boolean[] nullsafes) {
    if (this.array.length == 0) return false; // null key
    byte nulls = (byte)(this.array[0]);
    for (int i = 0; i < fieldCount; ++i) {
      if (((nulls & 1) == 0) && (nullsafes == null || !nullsafes[i])) return true;
      nulls >>>= 1;
    }
    return false;
  }

  public static MapJoinKey fromBytes(MapJoinKey key, boolean mayReuseKey, byte[] structBytes) {
    MapJoinKeyBytes result = (mayReuseKey && key != null)
        ? (MapJoinKeyBytes)key : new MapJoinKeyBytes();
    result.setBytes(structBytes);
    return result;
  }
}
