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

import java.util.Arrays;

import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
/**
 * A WritableBinaryObjectInspector inspects a BytesWritable Object.
 */
public class WritableBinaryObjectInspector extends AbstractPrimitiveWritableObjectInspector
    implements SettableBinaryObjectInspector {

  WritableBinaryObjectInspector() {
    super(TypeInfoFactory.binaryTypeInfo);
  }

  @Override
  public BytesWritable copyObject(Object o) {
    if (null == o) {
      return null;
    }
    BytesWritable incoming = (BytesWritable)o;
    byte[] bytes = new byte[incoming.getLength()];
    System.arraycopy(incoming.getBytes(),0, bytes, 0, incoming.getLength());
    return new BytesWritable(bytes);
  }

  @Override
  public byte[] getPrimitiveJavaObject(Object o) {
    return o == null ? null : LazyUtils.createByteArray((BytesWritable)o);
  }

  @Override
  public BytesWritable getPrimitiveWritableObject(Object o) {
    return null == o ? null : (BytesWritable)o;
  }

  /*
   * {@inheritDoc}
   */
  @Override
  public BytesWritable set(Object o, byte[] bb) {
    BytesWritable incoming = (BytesWritable)o;
    if (bb != null){
      incoming.set(bb, 0, bb.length);
    }
    return incoming;
  }

  /*
   * {@inheritDoc}
   */
  @Override
  public BytesWritable set(Object o, BytesWritable bw) {
    BytesWritable incoming = (BytesWritable)o;
    if (bw != null){
      incoming.set(bw);
    }
    return incoming;
  }

  /*
   * {@inheritDoc}
   */
  @Override
  public BytesWritable create(byte[] bb) {
    return new BytesWritable(Arrays.copyOf(bb, bb.length));
  }

  /*
   * {@inheritDoc}
   */
  @Override
  public BytesWritable create(BytesWritable bw) {

    BytesWritable newCpy = new BytesWritable();
    if (null != bw){
      newCpy.set(bw);
    }
    return newCpy;
  }

}
