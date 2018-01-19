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
package org.apache.hadoop.hive.serde2.lazydio;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyBinary;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyBinaryObjectInspector;

public class LazyDioBinary extends LazyBinary {

  public LazyDioBinary(LazyBinaryObjectInspector oi) {
    super(oi);
  }

  LazyDioBinary(LazyDioBinary copy) {
    super(copy);
  }

  /* (non-Javadoc)
   * This provides a LazyBinary like class which can be initialized from data stored in a
   * binary format.
   *
   * @see org.apache.hadoop.hive.serde2.lazy.LazyObject#init
   *        (org.apache.hadoop.hive.serde2.lazy.ByteArrayRef, int, int)
   */
  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    if (bytes == null) {
      throw new RuntimeException("bytes cannot be null!");
    }
    isNull = false;
    byte[] recv = new byte[length];
    System.arraycopy(bytes.getData(), start, recv, 0, length);
    data.set(recv, 0, length);
  }
}
