/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;

public class WritableTimestampLocalTZObjectInspector extends
    AbstractPrimitiveWritableObjectInspector implements SettableTimestampLocalTZObjectInspector {

  public WritableTimestampLocalTZObjectInspector() {
  }

  public WritableTimestampLocalTZObjectInspector(TimestampLocalTZTypeInfo typeInfo) {
    super(typeInfo);
  }

  @Override
  public TimestampLocalTZWritable getPrimitiveWritableObject(Object o) {
    if (o == null) {
      return null;
    }
    TimestampLocalTZWritable t = (TimestampLocalTZWritable) o;
    TimestampLocalTZTypeInfo timestampTZTypeInfo = (TimestampLocalTZTypeInfo) typeInfo;
    if (!t.getTimestampTZ().getZonedDateTime().getZone().equals(timestampTZTypeInfo.timeZone())) {
      t.setTimeZone(timestampTZTypeInfo.timeZone());
    }
    return t;
  }

  @Override
  public TimestampTZ getPrimitiveJavaObject(Object o) {
    if (o == null) {
      return null;
    }
    TimestampLocalTZWritable t = (TimestampLocalTZWritable) o;
    TimestampLocalTZTypeInfo timestampTZTypeInfo = (TimestampLocalTZTypeInfo) typeInfo;
    if (!t.getTimestampTZ().getZonedDateTime().getZone().equals(timestampTZTypeInfo.timeZone())) {
      t.setTimeZone(timestampTZTypeInfo.timeZone());
    }
    return t.getTimestampTZ();
  }

  @Override
  public Object set(Object o, byte[] bytes, int offset) {
    ((TimestampLocalTZWritable) o).set(bytes, offset, ((TimestampLocalTZTypeInfo) typeInfo).timeZone());
    return o;
  }

  @Override
  public Object set(Object o, TimestampTZ t) {
    if (t == null) {
      return null;
    }
    TimestampLocalTZTypeInfo timestampTZTypeInfo = (TimestampLocalTZTypeInfo) typeInfo;
    if (!t.getZonedDateTime().getZone().equals(timestampTZTypeInfo.timeZone())) {
      t.setZonedDateTime(t.getZonedDateTime().withZoneSameInstant(timestampTZTypeInfo.timeZone()));
    }
    ((TimestampLocalTZWritable) o).set(t);
    return o;
  }

  @Override
  public Object set(Object o, TimestampLocalTZWritable t) {
    if (t == null) {
      return null;
    }
    TimestampLocalTZTypeInfo timestampTZTypeInfo = (TimestampLocalTZTypeInfo) typeInfo;
    if (!t.getTimestampTZ().getZonedDateTime().getZone().equals(timestampTZTypeInfo.timeZone())) {
      t.getTimestampTZ().setZonedDateTime(
          t.getTimestampTZ().getZonedDateTime().withZoneSameInstant(timestampTZTypeInfo.timeZone()));
    }
    ((TimestampLocalTZWritable) o).set(t);
    return o;
  }

  @Override
  public Object create(byte[] bytes, int offset) {
    return new TimestampLocalTZWritable(bytes, offset, ((TimestampLocalTZTypeInfo) typeInfo).timeZone());
  }

  @Override
  public Object create(TimestampTZ t) {
    return new TimestampLocalTZWritable(t);
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new TimestampLocalTZWritable((TimestampLocalTZWritable) o);
  }
}
