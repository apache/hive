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
package org.apache.hadoop.hive.serde2.lazybinary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VInt;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDateObjectInspector;

/**
 * A LazyBinaryObject that encodes a java.sql.Date in a VInt.
 *
 */
public class LazyBinaryDate extends
    LazyBinaryPrimitive<WritableDateObjectInspector, DateWritable> {
  static final Logger LOG = LoggerFactory.getLogger(LazyBinaryDate.class);

  LazyBinaryDate(WritableDateObjectInspector oi) {
    super(oi);
    data = new DateWritable();
  }

  LazyBinaryDate(LazyBinaryDate copy) {
    super(copy);
    data = new DateWritable(copy.data);
  }

  /**
   * Reusable member for decoding integer.
   */
  VInt vInt = new LazyBinaryUtils.VInt();

  /**
   * Initializes LazyBinaryDate object.
   * @param bytes
   * @param start
   * @param length
   */
  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    LazyBinaryUtils.readVInt(bytes.getData(), start, vInt);
    assert (length == vInt.length);
    data.set(vInt.value);
  }
}
