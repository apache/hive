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

import org.apache.hadoop.hive.serde.serdeConstants;

public class TimestampTypeInfo extends PrimitiveTypeInfo {

  private final int precision;

  public TimestampTypeInfo() {
    this(6);
  }

  public TimestampTypeInfo(int precision) {
    super(serdeConstants.TIMESTAMP_TYPE_NAME);
    if (precision != 6 && precision != 9) {
      throw new RuntimeException("Unsupported value for precision: " + precision);
    }
    this.precision = precision;
  }

  public int getPrecision() {
    return precision;
  }

  @Override
  public String getQualifiedName() {
    return precision == 6
        ? serdeConstants.TIMESTAMP_TYPE_NAME
        : serdeConstants.TIMESTAMP_TYPE_NAME + "(" + precision + ")";
  }

  @Override
  public String getTypeName() {
    return getQualifiedName();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimestampTypeInfo)) {
      return false;
    }
    TimestampTypeInfo that = (TimestampTypeInfo) o;
    return precision == that.precision;
  }

  @Override
  public String toString() {
    return getQualifiedName();
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(precision);
  }
}
