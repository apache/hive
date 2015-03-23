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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VInt;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveIntervalYearMonthObjectInspector;

/**
 * LazyBinaryHiveIntervalYearMonth
 * A LazyBinaryObject that encodes a HiveIntervalYearMonth
 */
public class LazyBinaryHiveIntervalYearMonth extends
    LazyBinaryPrimitive<WritableHiveIntervalYearMonthObjectInspector, HiveIntervalYearMonthWritable>{
  static final Log LOG = LogFactory.getLog(LazyBinaryHiveIntervalYearMonth.class);

  /**
   * Reusable member for decoding integer.
   */
  VInt vInt = new LazyBinaryUtils.VInt();

  LazyBinaryHiveIntervalYearMonth(WritableHiveIntervalYearMonthObjectInspector oi) {
    super(oi);
    data = new HiveIntervalYearMonthWritable();
  }

  LazyBinaryHiveIntervalYearMonth(LazyBinaryHiveIntervalYearMonth copy) {
    super(copy);
    data = new HiveIntervalYearMonthWritable(copy.data);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    data.setFromBytes(bytes.getData(), start, length, vInt);
  }
}
