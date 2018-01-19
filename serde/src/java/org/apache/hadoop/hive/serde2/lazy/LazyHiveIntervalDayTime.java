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

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveIntervalDayTimeObjectInspector;
import org.apache.hadoop.io.Text;

public class LazyHiveIntervalDayTime
    extends LazyPrimitive<LazyHiveIntervalDayTimeObjectInspector, HiveIntervalDayTimeWritable> {

  public LazyHiveIntervalDayTime(LazyHiveIntervalDayTimeObjectInspector oi) {
    super(oi);
    data = new HiveIntervalDayTimeWritable();
  }

  public LazyHiveIntervalDayTime(LazyHiveIntervalDayTime copy) {
    super(copy);
    data = new HiveIntervalDayTimeWritable(copy.data);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    String s = null;
    try {
      s = Text.decode(bytes.getData(), start, length);
      data.set(HiveIntervalDayTime.valueOf(s));
      isNull = false;
    } catch (Exception e) {
      isNull = true;
      logExceptionMessage(bytes, start, length, "INTERVAL_DAY_TIME");
    }
  }

  public static void writeUTF8(OutputStream out, HiveIntervalDayTimeWritable i) throws IOException {
    ByteBuffer b = Text.encode(i.toString());
    out.write(b.array(), 0, b.limit());
  }

  @Override
  public HiveIntervalDayTimeWritable getWritableObject() {
    return data;
  }
}
