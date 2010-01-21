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

/**
 * A JavaByteObjectInspector inspects a Java Byte Object.
 */
public class JavaByteObjectInspector extends
    AbstractPrimitiveJavaObjectInspector implements SettableByteObjectInspector {

  JavaByteObjectInspector() {
    super(PrimitiveObjectInspectorUtils.byteTypeEntry);
  }

  @Override
  public Object getPrimitiveWritableObject(Object o) {
    return o == null ? null : new ByteWritable(((Byte) o).byteValue());
  }

  @Override
  public byte get(Object o) {
    return ((Byte) o).byteValue();
  }

  @Override
  public Object create(byte value) {
    return Byte.valueOf(value);
  }

  @Override
  public Object set(Object o, byte value) {
    return Byte.valueOf(value);
  }
}
