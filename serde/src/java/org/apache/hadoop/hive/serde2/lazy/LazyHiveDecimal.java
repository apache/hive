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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.io.Text;

public class LazyHiveDecimal extends LazyPrimitive<LazyHiveDecimalObjectInspector, HiveDecimalWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(LazyHiveDecimal.class);

  private final int precision;
  private final int scale;
  private static final byte[] nullBytes = new byte[]{0x0, 0x0, 0x0, 0x0};

  public LazyHiveDecimal(LazyHiveDecimalObjectInspector oi) {
    super(oi);
    DecimalTypeInfo typeInfo = (DecimalTypeInfo)oi.getTypeInfo();
    if (typeInfo == null) {
      throw new RuntimeException("Decimal type used without type params");
    }

    precision = typeInfo.precision();
    scale = typeInfo.scale();
    data = new HiveDecimalWritable();
  }

  public LazyHiveDecimal(LazyHiveDecimal copy) {
    super(copy);
    precision = copy.precision;
    scale = copy.scale;
    data = new HiveDecimalWritable(copy.data);
  }

  /**
   * Initilizes LazyHiveDecimal object by interpreting the input bytes
   * as a numeric string
   *
   * @param bytes
   * @param start
   * @param length
   */
  @Override
  public void init(ByteArrayRef bytes, int start, int length) {

    // Set the HiveDecimalWritable from bytes without converting to String first for
    // better performance.
    data.setFromBytes(bytes.getData(), start, length);
    if (!data.isSet()) {
      isNull = true;
    } else {
      isNull = !data.mutateEnforcePrecisionScale(precision, scale);
    }
    if (isNull) {
      LOG.debug("Data not in the HiveDecimal data type range so converted to null. Given data is :"
          + new String(bytes.getData(), start, length, StandardCharsets.UTF_8));
    }
  }

  @Override
  public HiveDecimalWritable getWritableObject() {
    return data;
  }

  /**
   * Writes HiveDecimal object to output stream as string
   * @param outputStream
   * @param hiveDecimal
   * @throws IOException
   */
  public static void writeUTF8(OutputStream outputStream, HiveDecimal hiveDecimal, int scale)
    throws IOException {
    if (hiveDecimal == null) {
      outputStream.write(nullBytes);
    } else {
      byte[] scratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES];
      int index = hiveDecimal.toFormatBytes(scale, scratchBuffer);
      outputStream.write(scratchBuffer, index, scratchBuffer.length - index);
    }
  }

  /**
   * Writes HiveDecimal object to output stream as string
   * @param outputStream
   * @param hiveDecimal
   * @throws IOException
   */
  public static void writeUTF8(
      OutputStream outputStream,
      HiveDecimal hiveDecimal, int scale,
      byte[] scratchBuffer)
    throws IOException {
    if (hiveDecimal == null) {
      outputStream.write(nullBytes);
    } else {
      int index = hiveDecimal.toFormatBytes(scale, scratchBuffer);
      outputStream.write(scratchBuffer, index, scratchBuffer.length - index);
    }
  }

  /**
   * Writes HiveDecimalWritable object to output stream as string
   * @param outputStream
   * @param hiveDecimalWritable
   * @throws IOException
   */
  public static void writeUTF8(
      OutputStream outputStream,
      HiveDecimalWritable hiveDecimalWritable, int scale,
      byte[] scratchBuffer)
    throws IOException {
    if (hiveDecimalWritable == null || !hiveDecimalWritable.isSet()) {
      outputStream.write(nullBytes);
    } else {
      int index = hiveDecimalWritable.toFormatBytes(scale, scratchBuffer);
      outputStream.write(scratchBuffer, index, scratchBuffer.length - index);
    }
  }
}
