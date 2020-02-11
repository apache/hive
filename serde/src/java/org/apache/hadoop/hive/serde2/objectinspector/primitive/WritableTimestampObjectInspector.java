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

public class WritableTimestampObjectInspector extends
    AbstractPrimitiveWritableObjectInspector implements
    SettableTimestampObjectInspector {

  public WritableTimestampObjectInspector() {
    super(TypeInfoFactory.timestampTypeInfo);
  }

  @Override
  public TimestampWritableV2 getPrimitiveWritableObject(Object o) {
    return o == null ? null : (TimestampWritableV2) o;
  }

  public Timestamp getPrimitiveJavaObject(Object o) {
    return o == null ? null : ((TimestampWritableV2) o).getTimestamp();
  }

  public Object copyObject(Object o) {
    return o == null ? null : new TimestampWritableV2((TimestampWritableV2) o);
  }

  public Object set(Object o, byte[] bytes, int offset) {
    ((TimestampWritableV2) o).set(bytes, offset);
    return o;
  }

  @Deprecated
  public Object set(Object o, java.sql.Timestamp t) {
    if (t == null) {
      return null;
    }
    ((TimestampWritableV2) o).set(Timestamp.ofEpochMilli(t.getTime(), t.getNanos()));
    return o;
  }

  public Object set(Object o, Timestamp t) {
    if (t == null) {
      return null;
    }
    ((TimestampWritableV2) o).set(t);
    return o;
  }

  public Object set(Object o, TimestampWritableV2 t) {
    if (t == null) {
      return null;
    }
    ((TimestampWritableV2) o).set(t);
    return o;
  }

  public Object create(byte[] bytes, int offset) {
    return new TimestampWritableV2(bytes, offset);
  }

  public Object create(java.sql.Timestamp t) {
    return new TimestampWritableV2(Timestamp.ofEpochMilli(t.getTime(), t.getNanos()));
  }

  public Object create(Timestamp t) {
    return new TimestampWritableV2(t);
  }
}
