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

import java.math.BigDecimal;
import java.nio.charset.CharacterCodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.io.BigDecimalWritable;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyBigDecimalObjectInspector;
import org.apache.hadoop.io.Text;

public class LazyBigDecimal extends LazyPrimitive<LazyBigDecimalObjectInspector, BigDecimalWritable> {
  static final private Log LOG = LogFactory.getLog(LazyBigDecimal.class);

  public LazyBigDecimal(LazyBigDecimalObjectInspector oi) {
    super(oi);
    data = new BigDecimalWritable();
  }

  public LazyBigDecimal(LazyBigDecimal copy) {
    super(copy);
    data = new BigDecimalWritable(copy.data);
  }

  /**
   * Initilizes LazyBigDecimal object by interpreting the input bytes
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
      data.set(new BigDecimal(byteData));
      isNull = false;
    } catch (NumberFormatException e) {
      isNull = true;
      LOG.debug("Data not in the BigDecimal data type range so converted to null. Given data is :"
          + byteData, e);
    } catch (CharacterCodingException e) {
      isNull = true;
      LOG.debug("Data not in the BigDecimal data type range so converted to null.", e);
    }
  }

  @Override
  public BigDecimalWritable getWritableObject() {
    return data;
  }
}
