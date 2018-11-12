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
package org.apache.hadoop.hive.druid.serde;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Writable for Druid results.
 */
public class DruidWritable implements Writable {

  /**
   * value map stores column name to value mapping.
   * This is only used when the result is not compacted.
   */
  private final Map<String, Object> value;

  /**
   * list of values in a compacted form, Serializer/Deserializer needs to agree on the order of values.
   * This is only used when the result is compacted.
   */
  private transient List<Object> compactedValue;

  private final boolean compacted;

  public DruidWritable(boolean compacted) {
    this.compacted = compacted;
    if (compacted) {
      compactedValue = Lists.newArrayList();
      value = null;
    } else {
      value = new HashMap<>();
      compactedValue = null;
    }
  }

  public DruidWritable(Map<String, Object> value) {
    this.value = value;
    this.compactedValue = null;
    compacted = false;
  }

  public DruidWritable(List<Object> value) {
    this.compacted = true;
    this.compactedValue = value;
    this.value = null;
  }

  public Map<String, Object> getValue() {
    if (compacted) {
      throw new UnsupportedOperationException(
          "compacted DruidWritable does not support getValue(), use getCompactedValue()");
    }
    return value;
  }

  List<Object> getCompactedValue() {
    if (!compacted) {
      throw new UnsupportedOperationException(
          "non compacted DruidWritable does not support getCompactedValue(), use getValue()");
    }
    return compactedValue;
  }

  void setCompactedValue(List<Object> compactedValue) {
    this.compactedValue = compactedValue;
  }

  boolean isCompacted() {
    return compacted;
  }

  @Override public void write(DataOutput out) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override public int hashCode() {
    return Objects.hash(value, compactedValue, compacted);
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DruidWritable that = (DruidWritable) o;
    return compacted == that.compacted && Objects.equals(value, that.value) && Objects.equals(compactedValue,
        that.compactedValue);
  }

  @Override public String toString() {
    return "DruidWritable{"
        + "value="
        + value
        + ", compactedValue="
        + compactedValue
        + ", compacted="
        + compacted
        + '}';
  }
}
