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

import java.sql.Timestamp;

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class JavaTimestampObjectInspector
    extends AbstractPrimitiveJavaObjectInspector
    implements SettableTimestampObjectInspector {

  protected JavaTimestampObjectInspector() {
    super(TypeInfoFactory.timestampTypeInfo);
  }

  public TimestampWritable getPrimitiveWritableObject(Object o) {
    return o == null ? null : new TimestampWritable((Timestamp) o);
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
    Timestamp copy = new Timestamp(source.getTime());
    copy.setNanos(source.getNanos());
    return copy;
  }

  public Timestamp get(Object o) {
    return (Timestamp) o;
  }

  public Object set(Object o, Timestamp value) {
    if (value == null) {
      return null;
    }
    ((Timestamp) o).setTime(value.getTime());
    return o;
  }

  public Object set(Object o, byte[] bytes, int offset) {
    TimestampWritable.setTimestamp((Timestamp) o, bytes, offset);
    return o;
  }

  public Object set(Object o, TimestampWritable tw) {
    if (tw == null) {
      return null;
    }
    Timestamp t = (Timestamp) o;
    t.setTime(tw.getTimestamp().getTime());
    t.setNanos(tw.getTimestamp().getNanos());
    return t;
  }

  public Object create(Timestamp value) {
    return new Timestamp(value.getTime());
  }

  public Object create(byte[] bytes, int offset) {
    return TimestampWritable.createTimestamp(bytes, offset);
  }
}
