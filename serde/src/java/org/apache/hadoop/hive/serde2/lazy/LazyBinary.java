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

package org.apache.hadoop.hive.serde2.lazy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;

import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyBinaryObjectInspector;
import org.apache.hadoop.io.BytesWritable;

public class LazyBinary extends LazyPrimitive<LazyBinaryObjectInspector, BytesWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(LazyBinary.class);

  public LazyBinary(LazyBinaryObjectInspector oi) {
    super(oi);
    data = new BytesWritable();
  }

  public LazyBinary(LazyBinary other){
    super(other);
    BytesWritable incoming = other.getWritableObject();
    byte[] bytes = new byte[incoming.getLength()];
    System.arraycopy(incoming.getBytes(), 0, bytes, 0, incoming.getLength());
    data = new BytesWritable(bytes);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    super.init(bytes, start, length);
    byte[] recv = new byte[length];
    System.arraycopy(bytes.getData(), start, recv, 0, length);
    byte[] decoded = decodeIfNeeded(recv);
    data.set(decoded, 0, decoded.length);
  }

  // todo this should be configured in serde
  public static byte[] decodeIfNeeded(byte[] recv) {
    try {
      return Base64.getDecoder().decode(recv);
    } catch (IllegalArgumentException e) {
      // use the original bytes in case decoding should fail
      LOG.debug("Data does not contain only Base64 characters so return original byte array", e);
      return recv;
    }
  }
}
