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
import java.sql.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyDateObjectInspector;
import org.apache.hadoop.io.Text;

/**
 *
 * LazyDate.
 * Serializes and deserializes a Date in the SQL date format
 *
 *    YYYY-MM-DD
 *
 */
public class LazyDate extends LazyPrimitive<LazyDateObjectInspector, DateWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(LazyDate.class);

  public LazyDate(LazyDateObjectInspector oi) {
    super(oi);
    data = new DateWritable();
  }

  public LazyDate(LazyDate copy) {
    super(copy);
    data = new DateWritable(copy.data);
  }

  /**
   * Initializes LazyDate object by interpreting the input bytes as a SQL date string.
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
      s = Text.decode(bytes.getData(), start, length);
      data.set(Date.valueOf(s));
      isNull = false;
    } catch (Exception e) {
      isNull = true;
      logExceptionMessage(bytes, start, length, "DATE");
    }
  }

  /**
   * Writes a Date in SQL date format to the output stream.
   * @param out
   *          The output stream
   * @param d
   *          The Date to write
   * @throws IOException
   */
  public static void writeUTF8(OutputStream out, DateWritable d)
      throws IOException {
    ByteBuffer b = Text.encode(d.toString());
    out.write(b.array(), 0, b.limit());
  }

}
