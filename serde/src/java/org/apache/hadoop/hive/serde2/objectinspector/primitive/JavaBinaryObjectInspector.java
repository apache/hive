package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import java.util.Arrays;

import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.io.BytesWritable;

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

public class JavaBinaryObjectInspector extends AbstractPrimitiveJavaObjectInspector implements
    SettableBinaryObjectInspector {

  JavaBinaryObjectInspector() {
    super(PrimitiveObjectInspectorUtils.binaryTypeEntry);
  }

  @Override
  public byte[] copyObject(Object o) {
    if (null == o){
      return null;
    }
    byte[] incoming = (byte[])o;
    byte[] outgoing = new byte[incoming.length];
    System.arraycopy(incoming, 0, outgoing, 0, incoming.length);
    return outgoing;
  }

  @Override
  public BytesWritable getPrimitiveWritableObject(Object o) {
    return o == null ? null : new BytesWritable((byte[])o);
  }

  @Override
  public byte[] getPrimitiveJavaObject(Object o) {
    return (byte[])o;
  }

  /*
   * {@inheritDoc}
   */
  @Override
  public byte[] set(Object o, byte[] bb) {
    return bb == null ? null : Arrays.copyOf(bb, bb.length);
  }

  /*
   * {@inheritDoc}
   */
  @Override
  public byte[] set(Object o, BytesWritable bw) {
    return bw == null ? null :  LazyUtils.createByteArray(bw);
  }

  /*
   * {@inheritDoc}
   */
  @Override
  public byte[] create(byte[] bb) {
    return bb == null ? null : Arrays.copyOf(bb, bb.length);
  }

  /*
   * {@inheritDoc}
   */
  @Override
  public byte[] create(BytesWritable bw) {
    return bw == null ? null : LazyUtils.createByteArray(bw);
  }

}
