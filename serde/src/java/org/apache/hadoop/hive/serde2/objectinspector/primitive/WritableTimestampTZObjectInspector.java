/**
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
import org.apache.hadoop.hive.serde2.io.TimestampTZWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class WritableTimestampTZObjectInspector extends
    AbstractPrimitiveWritableObjectInspector implements SettableTimestampTZObjectInspector {

  public WritableTimestampTZObjectInspector() {
    super(TypeInfoFactory.timestampTZTypeInfo);
  }

  @Override
  public TimestampTZWritable getPrimitiveWritableObject(Object o) {
    return o == null ? null : (TimestampTZWritable) o;
  }

  @Override
  public Object set(Object o, byte[] bytes, int offset) {
    ((TimestampTZWritable) o).set(bytes, offset);
    return o;
  }

  @Override
  public Object set(Object o, TimestampTZ t) {
    if (t == null) {
      return null;
    }
    ((TimestampTZWritable) o).set(t);
    return o;
  }

  @Override
  public Object set(Object o, TimestampTZWritable t) {
    if (t == null) {
      return null;
    }
    ((TimestampTZWritable) o).set(t);
    return o;
  }

  @Override
  public Object create(byte[] bytes, int offset) {
    return new TimestampTZWritable(bytes, offset);
  }

  @Override
  public Object create(TimestampTZ t) {
    return new TimestampTZWritable(t);
  }

  @Override
  public TimestampTZ getPrimitiveJavaObject(Object o) {
    return o == null ? null : ((TimestampTZWritable) o).getTimestampTZ();
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new TimestampTZWritable((TimestampTZWritable) o);
  }
}
