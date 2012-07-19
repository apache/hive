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

package org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.lazy.LazyBinary;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BytesWritable;

public class LazyBinaryObjectInspector extends
  AbstractPrimitiveLazyObjectInspector<BytesWritable> implements
    BinaryObjectInspector {

  protected LazyBinaryObjectInspector() {
    super(PrimitiveObjectInspectorUtils.binaryTypeEntry);
  }

  @Override
  public Object copyObject(Object o) {
    return null == o ? null : new LazyBinary((LazyBinary)o);
  }

  @Override
  public byte[] getPrimitiveJavaObject(Object o) {
    if (null == o) {
      return null;
    }
    return LazyUtils.createByteArray(((LazyBinary) o).getWritableObject());
  }

  @Override
  public BytesWritable getPrimitiveWritableObject(Object o) {
    return null == o ? null : ((LazyBinary) o).getWritableObject();
  }
}
