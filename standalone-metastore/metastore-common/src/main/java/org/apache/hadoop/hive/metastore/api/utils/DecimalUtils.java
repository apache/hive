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

package org.apache.hadoop.hive.metastore.api.utils;

import java.nio.ByteBuffer;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.hadoop.hive.metastore.api.Decimal;

/**
 * This class contains helper methods for handling thrift api's Decimal
 */
public class DecimalUtils {

  public static Decimal getDecimal(int number, int scale) {
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.asIntBuffer().put(number);
    return new Decimal((short) scale, bb);
  }

  public static Decimal getDecimal(ByteBuffer unscaled, short scale) {
    return new Decimal((short) scale, unscaled);
  }

  public static Decimal createThriftDecimal(String s) {
    BigDecimal d = new BigDecimal(s);
    return new Decimal((short) d.scale(), ByteBuffer.wrap(d.unscaledValue().toByteArray()));
  }

  public static String createJdoDecimalString(Decimal d) {
    return new BigDecimal(new BigInteger(d.getUnscaled()), d.getScale()).toString();
  }
}
