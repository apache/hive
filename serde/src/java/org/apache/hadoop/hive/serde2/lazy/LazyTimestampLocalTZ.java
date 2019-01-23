/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.lazy;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;

import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyTimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;

/**
 * LazyPrimitive for TimestampLocalTZ. Similar to LazyTimestamp.
 */
public class LazyTimestampLocalTZ extends
    LazyPrimitive<LazyTimestampLocalTZObjectInspector, TimestampLocalTZWritable> {

  private ZoneId timeZone;

  public LazyTimestampLocalTZ(LazyTimestampLocalTZObjectInspector lazyTimestampTZObjectInspector) {
    super(lazyTimestampTZObjectInspector);
    TimestampLocalTZTypeInfo typeInfo = (TimestampLocalTZTypeInfo) oi.getTypeInfo();
    if (typeInfo == null) {
      throw new RuntimeException("TimestampLocalTZ type used without type params");
    }
    timeZone = typeInfo.timeZone();
    data = new TimestampLocalTZWritable();
  }

  public LazyTimestampLocalTZ(LazyTimestampLocalTZ copy) {
    super(copy);
    timeZone = copy.timeZone;
    data = new TimestampLocalTZWritable(copy.data);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    String s = null;
    if (!LazyUtils.isDateMaybe(bytes.getData(), start, length)) {
      isNull = true;
      return;
    }

    TimestampTZ t = null;
    try {
      s = new String(bytes.getData(), start, length, StandardCharsets.US_ASCII);
      if (s.equals("NULL")) {
        isNull = true;
        logExceptionMessage(bytes, start, length,
            serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME.toUpperCase());
      } else {
        t = TimestampTZUtil.parse(s, timeZone);
        isNull = false;
      }
    } catch (DateTimeParseException e) {
      isNull = true;
      logExceptionMessage(bytes, start, length, serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME.toUpperCase());
    }
    data.set(t);
  }

  @Override
  public TimestampLocalTZWritable getWritableObject() {
    return data;
  }

  public static void writeUTF8(OutputStream out, TimestampLocalTZWritable i) throws IOException {
    byte[] b = TimestampLocalTZWritable.nullBytes;
    if (i != null) {
      b = i.toString().getBytes(StandardCharsets.US_ASCII);
    }
    out.write(b);
  }
}
