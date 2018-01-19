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

package org.apache.hadoop.hive.accumulo;

import org.apache.accumulo.core.client.lexicoder.BigIntegerLexicoder;
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.hadoop.hive.serde.serdeConstants;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Utility class to encode index values for accumulo.
 */
public final class AccumuloIndexLexicoder {
  private static final IntegerLexicoder INTEGER_LEXICODER = new IntegerLexicoder();
  private static final DoubleLexicoder DOUBLE_LEXICODER = new DoubleLexicoder();
  private static final LongLexicoder LONG_LEXICODER = new LongLexicoder();
  private static final BigIntegerLexicoder BIG_INTEGER_LEXICODER = new BigIntegerLexicoder();
  private static final String DIM_PAT = "[(]+.*";


  private AccumuloIndexLexicoder() {
    // hide constructor
  }

  public static String getRawType(String hiveType) {
    if (hiveType != null) {
      return hiveType.toLowerCase().replaceFirst(DIM_PAT, "").trim();
    }
    return hiveType;
  }

  public static byte[] encodeValue(byte[] value, String hiveType, boolean stringEncoded) {
    if (stringEncoded) {
      return encodeStringValue(value, hiveType);
    } else {
      return encodeBinaryValue(value, hiveType);
    }
  }

  public static byte[] encodeStringValue(byte[] value, String hiveType) {
    String rawType = getRawType(hiveType);

    switch(rawType) {
      case serdeConstants.BOOLEAN_TYPE_NAME:
        return Boolean.valueOf(new String(value)).toString().getBytes(UTF_8);
      case serdeConstants.SMALLINT_TYPE_NAME :
      case serdeConstants.TINYINT_TYPE_NAME :
      case serdeConstants.INT_TYPE_NAME :
        return INTEGER_LEXICODER.encode(Integer.valueOf(new String(value)));
      case serdeConstants.FLOAT_TYPE_NAME :
      case serdeConstants.DOUBLE_TYPE_NAME :
        return DOUBLE_LEXICODER.encode(Double.valueOf(new String(value)));
      case serdeConstants.BIGINT_TYPE_NAME :
        return BIG_INTEGER_LEXICODER.encode(new BigInteger(new String(value), 10));
      case serdeConstants.DECIMAL_TYPE_NAME :
        return new String(value).getBytes(UTF_8);
      default :
        // return the passed in string value
        return value;
    }
  }

  public static byte[] encodeBinaryValue(byte[] value, String hiveType) {
    String rawType = getRawType(hiveType);

    switch(rawType) {
      case serdeConstants.BOOLEAN_TYPE_NAME :
        return String.valueOf(value[0] == 1).getBytes();
      case serdeConstants.INT_TYPE_NAME :
        return INTEGER_LEXICODER.encode(ByteBuffer.wrap(value).asIntBuffer().get());
      case serdeConstants.SMALLINT_TYPE_NAME :
        return INTEGER_LEXICODER.encode((int)(ByteBuffer.wrap(value).asShortBuffer().get()));
      case serdeConstants.TINYINT_TYPE_NAME :
        return INTEGER_LEXICODER.encode((int)value[0]);
      case serdeConstants.FLOAT_TYPE_NAME :
        return DOUBLE_LEXICODER.encode((double)ByteBuffer.wrap(value).asFloatBuffer().get());
      case serdeConstants.DOUBLE_TYPE_NAME :
        return DOUBLE_LEXICODER.encode(ByteBuffer.wrap(value).asDoubleBuffer().get());
      case serdeConstants.BIGINT_TYPE_NAME :
        return BIG_INTEGER_LEXICODER.encode(new BigInteger(value));
      case serdeConstants.DECIMAL_TYPE_NAME :
        return new String(value).getBytes(UTF_8);
      default :
        return value;
    }
  }
}
