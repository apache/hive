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

  public TimestampLocalTZTypeInfo() {
    super(serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME);
  }

  public TimestampLocalTZTypeInfo(String timeZoneStr) {
    super(serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME);
    this.timeZone = TimestampTZUtil.parseTimeZone(timeZoneStr);
  }

  @Override
  public String getTypeName() {
    return serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME;
  }

  @Override
  public void setTypeName(String typeName) {
    // No need to set type name, it should always be {@link serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME}
    return;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    TimestampLocalTZTypeInfo dti = (TimestampLocalTZTypeInfo) other;

    return this.timeZone().equals(dti.timeZone());
  }

  /**
   * Generate the hashCode for this TypeInfo.
   */
  @Override
  public int hashCode() {
    return Objects.hash(typeName, timeZone);
  }

  @Override
  public String toString() {
    return getQualifiedName(timeZone);
  }

  @Override
  public String getQualifiedName() {
    return getQualifiedName(null);
  }

  public static String getQualifiedName(ZoneId timeZone) {
    StringBuilder sb = new StringBuilder(serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME);
    if (timeZone != null) {
      sb.append("('");
      sb.append(timeZone);
      sb.append("')");
    }
    return sb.toString();
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
