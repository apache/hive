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

import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * A WritableByteObjectInspector inspects a ByteWritable Object.
 */
public class WritableByteObjectInspector extends
    AbstractPrimitiveWritableObjectInspector implements
    SettableByteObjectInspector {

  public WritableByteObjectInspector() {
    super(TypeInfoFactory.byteTypeInfo);
  }

  @Override
  public byte get(Object o) {
    return ((ByteWritable) o).get();
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new ByteWritable(((ByteWritable) o).get());
  }

  @Override
  public Object getPrimitiveJavaObject(Object o) {
    return o == null ? null : Byte.valueOf(((ByteWritable) o).get());
  }

  @Override
  public Object create(byte value) {
    return new ByteWritable(value);
  }

  @Override
  public Object set(Object o, byte value) {
    ((ByteWritable) o).set(value);
    return o;
  }
}
