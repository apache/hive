package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
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
  public ByteArrayRef copyObject(Object o) {
    if (null == o){
      return null;
    }

    ByteArrayRef ba = new ByteArrayRef();
    byte[] incoming = ((ByteArrayRef)o).getData();
    byte[] outgoing = new byte[incoming.length];
    System.arraycopy(incoming, 0, outgoing, 0, incoming.length);
    ba.setData(outgoing);
    return ba;
  }

  @Override
  public BytesWritable getPrimitiveWritableObject(Object o) {
    return o == null ? null : new BytesWritable(((ByteArrayRef)o).getData());
  }

  @Override
  public ByteArrayRef getPrimitiveJavaObject(Object o) {
    return (ByteArrayRef)o;
  }
  @Override
  public ByteArrayRef set(Object o, ByteArrayRef bb) {
    ByteArrayRef ba = (ByteArrayRef)o;
    ba.setData(bb.getData());
    return ba;
  }

  @Override
  public ByteArrayRef set(Object o, BytesWritable bw) {
    if (null == bw){
      return null;
    }
    ByteArrayRef ba = (ByteArrayRef)o;
    ba.setData(bw.getBytes());
    return ba;
  }

  @Override
  public ByteArrayRef create(ByteArrayRef bb) {
    ByteArrayRef ba = new ByteArrayRef();
    ba.setData(bb.getData());
    return ba;
  }

  @Override
  public ByteArrayRef create(BytesWritable bw) {
    if(null == bw){
      return null;
    }
    ByteArrayRef ba = new ByteArrayRef();
    ba.setData(bw.getBytes());
    return ba;
  }

}
