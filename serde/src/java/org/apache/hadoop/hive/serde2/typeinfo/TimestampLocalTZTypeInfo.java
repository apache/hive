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

import java.time.ZoneId;
import java.util.Objects;

import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.serde.serdeConstants;

public class TimestampLocalTZTypeInfo extends PrimitiveTypeInfo {
  private static final long serialVersionUID = 1L;

  private ZoneId timeZone;
  private final int precision;

  public TimestampLocalTZTypeInfo() {
    this(6, ZoneId.systemDefault());
  }

  public TimestampLocalTZTypeInfo(String timeZoneStr) {
    this(6, TimestampTZUtil.parseTimeZone(timeZoneStr));
  }

  public TimestampLocalTZTypeInfo(int precision, ZoneId timeZone) {
    super(serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME);
    if (precision != 6 && precision != 9) {
      throw new RuntimeException("Unsupported value for precision: " + precision);
    }
    this.precision = precision;
    this.timeZone = timeZone;
  }

  public int getPrecision() {
    return precision;
  }

  @Override
  public void setTypeName(String typeName) {
    // No need to set type name, it should always be {@link serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME}
    return;
  }

  @Override
  public String getTypeName() {
    if (precision == 9) {
      return serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME + "(9)";
    }
    return super.getTypeName();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    TimestampLocalTZTypeInfo that = (TimestampLocalTZTypeInfo) other;
    return precision == that.precision && Objects.equals(timeZone, that.timeZone);
  }

  /**
   * Generate the hashCode for this TypeInfo.
   */
  @Override
  public int hashCode() {
    return Objects.hash(typeName, precision, timeZone);
  }

  @Override
  public String toString() {
    return getQualifiedName(timeZone, precision);
  }

  @Override
  public String getQualifiedName() {
    return getQualifiedName(null, precision);
  }

  static String getQualifiedName(ZoneId timeZone, int precision) {
    StringBuilder sb = new StringBuilder(serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME);
    if (timeZone != null) {
      sb.append("('");
      sb.append(timeZone);
      sb.append("')");
    }
    if (precision > 6) {
      sb.append("(");
      sb.append(precision);
      sb.append(")");
    }
    return sb.toString();
  }

  public static String getQualifiedName(ZoneId timeZone) {
    return getQualifiedName(timeZone, 6);
  }

  public ZoneId timeZone() {
    return timeZone;
  }

  public ZoneId getTimeZone() {
    return timeZone;
  }

  public void setTimeZone(ZoneId timeZone) {
    this.timeZone = timeZone;
  }

}
