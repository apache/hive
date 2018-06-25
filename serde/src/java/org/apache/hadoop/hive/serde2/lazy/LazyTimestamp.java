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
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyTimestampObjectInspector;

/**
 *
 * LazyTimestamp.
 * Serializes and deserializes a Timestamp in the JDBC timestamp format
 *
 *    YYYY-MM-DD HH:MM:SS.[fff...]
 *
 */
public class LazyTimestamp extends LazyPrimitive<LazyTimestampObjectInspector, TimestampWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(LazyTimestamp.class);

  public LazyTimestamp(LazyTimestampObjectInspector oi) {
    super(oi);
    data = new TimestampWritable();
  }

  public LazyTimestamp(LazyTimestamp copy) {
    super(copy);
    data = new TimestampWritable(copy.data);
  }

  /**
   * Initilizes LazyTimestamp object by interpreting the input bytes
   * as a JDBC timestamp string
   *
   * @param bytes
   * @param start
   * @param length
   */
  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    String s = null;
    if (!LazyUtils.isDateMaybe(bytes.getData(), start, length)) {
      isNull = true;
      return;
    }
    try {
      s = new String(bytes.getData(), start, length, "US-ASCII");
    } catch (UnsupportedEncodingException e) {
      LOG.error("Unsupported encoding found ", e);
      s = "";
    }

    Timestamp t = null;
    if (s.compareTo("NULL") == 0) {
      isNull = true;
      logExceptionMessage(bytes, start, length, "TIMESTAMP");
    } else {
      try {
        t = oi.getTimestampParser().parseTimestamp(s);
        isNull = false;
      } catch (IllegalArgumentException e) {
        isNull = true;
        logExceptionMessage(bytes, start, length, "TIMESTAMP");
      }
    }
    data.set(t);
  }

  /**
   * Writes a Timestamp in JDBC timestamp format to the output stream
   * @param out
   *          The output stream
   * @param i
   *          The Timestamp to write
   * @throws IOException
   */
  public static void writeUTF8(OutputStream out, TimestampWritable i)
      throws IOException {
    if (i == null) {
      // Serialize as time 0
      out.write(TimestampWritable.nullBytes);
    } else {
      out.write(i.toString().getBytes("US-ASCII"));
    }
  }

  @Override
  public TimestampWritable getWritableObject() {
    return data;
  }
}
