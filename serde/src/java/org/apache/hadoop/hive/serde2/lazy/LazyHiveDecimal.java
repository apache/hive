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
package org.apache.hadoop.hive.serde2.lazy;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.io.Text;

public class LazyHiveDecimal extends LazyPrimitive<LazyHiveDecimalObjectInspector, HiveDecimalWritable> {
  static final private Log LOG = LogFactory.getLog(LazyHiveDecimal.class);

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
    String byteData = null;
    try {
      byteData = Text.decode(bytes.getData(), start, length);
    } catch (CharacterCodingException e) {
      isNull = true;
      LOG.debug("Data not in the HiveDecimal data type range so converted to null.", e);
      return;
    }

    HiveDecimal dec = HiveDecimal.create(byteData);
    dec = enforcePrecisionScale(dec);
    if (dec != null) {
      data.set(dec);
      isNull = false;
    } else {
      LOG.debug("Data not in the HiveDecimal data type range so converted to null. Given data is :"
          + byteData);
      isNull = true;
    }
  }

  private HiveDecimal enforcePrecisionScale(HiveDecimal dec) {
    return HiveDecimalUtils.enforcePrecisionScale(dec, precision, scale);
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
  public static void writeUTF8(OutputStream outputStream, HiveDecimal hiveDecimal)
    throws IOException {
    if (hiveDecimal == null) {
      outputStream.write(nullBytes);
    } else {
      ByteBuffer b = Text.encode(hiveDecimal.toString());
      outputStream.write(b.array(), 0, b.limit());
    }
  }
}
