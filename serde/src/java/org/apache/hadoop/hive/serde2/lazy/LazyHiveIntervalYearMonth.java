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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveIntervalYearMonthObjectInspector;
import org.apache.hadoop.io.Text;

public class LazyHiveIntervalYearMonth
    extends LazyPrimitive<LazyHiveIntervalYearMonthObjectInspector, HiveIntervalYearMonthWritable> {

  public LazyHiveIntervalYearMonth(LazyHiveIntervalYearMonthObjectInspector oi) {
    super(oi);
    data = new HiveIntervalYearMonthWritable();
  }

  public LazyHiveIntervalYearMonth(LazyHiveIntervalYearMonth copy) {
    super(copy);
    data = new HiveIntervalYearMonthWritable(copy.data);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    String s = null;
    try {
      s = Text.decode(bytes.getData(), start, length);
      data.set(HiveIntervalYearMonth.valueOf(s));
      isNull = false;
    } catch (Exception e) {
      isNull = true;
      logExceptionMessage(bytes, start, length, "INTERVAL_YEAR_MONTH");
    }
  }

  public static void writeUTF8(OutputStream out, HiveIntervalYearMonthWritable i)
      throws IOException {
    ByteBuffer b = Text.encode(i.toString());
    out.write(b.array(), 0, b.limit());
  }

  @Override
  public HiveIntervalYearMonthWritable getWritableObject() {
    return data;
  }
}
