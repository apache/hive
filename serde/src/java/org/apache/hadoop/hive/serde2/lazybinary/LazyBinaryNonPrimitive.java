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
package org.apache.hadoop.hive.serde2.lazybinary;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * LazyBinaryNonPrimitive.
 *
 * @param <OI>
 */
public abstract class LazyBinaryNonPrimitive<OI extends ObjectInspector>
    extends LazyBinaryObject<OI> {

  protected ByteArrayRef bytes;
  protected int start;
  protected int length;

  protected LazyBinaryNonPrimitive(OI oi) {
    super(oi);
    bytes = null;
    start = 0;
    length = 0;
  }

  @Override
  public Object getObject() {
    return this;
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    if (null == bytes) {
      throw new RuntimeException("bytes cannot be null!");
    }
    if (length <= 0) {
      throw new RuntimeException("length should be positive!");
    }
    this.bytes = bytes;
    this.start = start;
    this.length = length;
  }

  @Override
  public int hashCode() {
    return LazyUtils.hashBytes(bytes.getData(), start, length);
  }
}
