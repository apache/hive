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
package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class JavaTimestampObjectInspector
    extends AbstractPrimitiveJavaObjectInspector
    implements SettableTimestampObjectInspector {

  protected JavaTimestampObjectInspector() {
    super(TypeInfoFactory.timestampTypeInfo);
  }

  public TimestampWritableV2 getPrimitiveWritableObject(Object o) {
    return o == null ? null : new TimestampWritableV2((Timestamp) o);
  }

  @Override
  public Timestamp getPrimitiveJavaObject(Object o) {
    return o == null ? null : (Timestamp) o;
  }

  @Override
  public Object copyObject(Object o) {
    if (o == null) {
      return null;
    }
    Timestamp source = (Timestamp) o;
    return new Timestamp(source);
  }

  public Timestamp get(Object o) {
    return (Timestamp) o;
  }

  @Deprecated
  public Object set(Object o, java.sql.Timestamp value) {
    if (value == null) {
      return null;
    }
    ((Timestamp) o).setTimeInMillis(value.getTime(), value.getNanos());
    return o;
  }

  public Object set(Object o, Timestamp value) {
    if (value == null) {
      return null;
    }
    ((Timestamp) o).set(value);
    return o;
  }

  public Object set(Object o, byte[] bytes, int offset) {
    TimestampWritableV2.setTimestamp((Timestamp) o, bytes, offset);
    return o;
  }

  public Object set(Object o, TimestampWritableV2 tw) {
    if (tw == null) {
      return null;
    }
    Timestamp t = (Timestamp) o;
    t.set(tw.getTimestamp());
    return t;
  }

  @Deprecated
  public Object create(java.sql.Timestamp value) {
    return Timestamp.ofEpochMilli(value.getTime(), value.getNanos());
  }

  public Object create(Timestamp value) {
    return new Timestamp(value);
  }

  public Object create(byte[] bytes, int offset) {
    return TimestampWritableV2.createTimestamp(bytes, offset);
  }
}
