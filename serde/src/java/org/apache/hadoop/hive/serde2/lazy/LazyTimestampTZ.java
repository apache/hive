/**
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

import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.TimestampTZWritable;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyTimestampTZObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.time.format.DateTimeParseException;

/**
 * LazyPrimitive for TimestampTZ. Similar to LazyTimestamp.
 */
public class LazyTimestampTZ extends
    LazyPrimitive<LazyTimestampTZObjectInspector, TimestampTZWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(LazyTimestampTZ.class);

  public LazyTimestampTZ(LazyTimestampTZObjectInspector lazyTimestampTZObjectInspector) {
    super(lazyTimestampTZObjectInspector);
    data = new TimestampTZWritable();
  }

  public LazyTimestampTZ(LazyTimestampTZ copy) {
    super(copy);
    data = new TimestampTZWritable(copy.data);
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
      s = new String(bytes.getData(), start, length, "US-ASCII");
      if (s.equals("NULL")) {
        isNull = true;
        logExceptionMessage(bytes, start, length,
            serdeConstants.TIMESTAMPTZ_TYPE_NAME.toUpperCase());
      } else {
        t = TimestampTZ.parse(s);
        isNull = false;
      }
    } catch (UnsupportedEncodingException e) {
      isNull = true;
      LOG.error("Unsupported encoding found ", e);
    } catch (DateTimeParseException e) {
      isNull = true;
      logExceptionMessage(bytes, start, length, serdeConstants.TIMESTAMPTZ_TYPE_NAME.toUpperCase());
    }
    data.set(t);
  }

  @Override
  public TimestampTZWritable getWritableObject() {
    return data;
  }

  public static void writeUTF8(OutputStream out, TimestampTZWritable i) throws IOException {
    if (i == null) {
      out.write(TimestampTZWritable.nullBytes);
    } else {
      out.write(i.toString().getBytes("US-ASCII"));
    }
  }
}
