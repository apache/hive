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
package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import java.sql.Timestamp;

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class WritableTimestampObjectInspector extends
    AbstractPrimitiveWritableObjectInspector implements
    SettableTimestampObjectInspector {

  public WritableTimestampObjectInspector() {
    super(TypeInfoFactory.timestampTypeInfo);
  }

  @Override
  public TimestampWritable getPrimitiveWritableObject(Object o) {
    return o == null ? null : (TimestampWritable) o;
  }

  public Timestamp getPrimitiveJavaObject(Object o) {
    return o == null ? null : ((TimestampWritable) o).getTimestamp();
  }

  public Object copyObject(Object o) {
    return o == null ? null : new TimestampWritable((TimestampWritable) o);
  }

  public Object set(Object o, byte[] bytes, int offset) {
    ((TimestampWritable) o).set(bytes, offset);
    return o;
  }

  public Object set(Object o, Timestamp t) {
    if (t == null) {
      return null;
    }
    ((TimestampWritable) o).set(t);
    return o;
  }

  public Object set(Object o, TimestampWritable t) {
    if (t == null) {
      return null;
    }
    ((TimestampWritable) o).set(t);
    return o;
  }

  public Object create(byte[] bytes, int offset) {
    return new TimestampWritable(bytes, offset);
  }

  public Object create(Timestamp t) {
    return new TimestampWritable(t);
  }
}
