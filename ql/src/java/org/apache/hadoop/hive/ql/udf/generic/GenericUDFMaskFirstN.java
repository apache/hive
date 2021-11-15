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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;


@Description(name = "mask_first_n",
             value = "masks the first n characters of the value",
             extended = "Examples:\n "
                      + "  mask_first_n(ccn, 8)\n "
                      + "  mask_first_n(ccn, 8, 'x', 'x', 'x')\n "
                      + "Arguments:\n "
                      + "  mask(value, charCount, upperChar, lowerChar, digitChar, otherChar, numberChar)\n "
                      + "    value      - value to mask. Supported types: TINYINT, SMALLINT, INT, BIGINT, STRING, VARCHAR, CHAR\n "
                      + "    charCount  - number of characters. Default value: 4\n "
                      + "    upperChar  - character to replace upper-case characters with. Specify -1 to retain original character. Default value: 'X'\n "
                      + "    lowerChar  - character to replace lower-case characters with. Specify -1 to retain original character. Default value: 'x'\n "
                      + "    digitChar  - character to replace digit characters with. Specify -1 to retain original character. Default value: 'n'\n "
                      + "    otherChar  - character to replace all other characters with. Specify -1 to retain original character. Default value: -1\n "
                      + "    numberChar - character to replace digits in a number with. Valid values: 0-9. Default value: '1'\n "
            )
public class GenericUDFMaskFirstN extends BaseMaskUDF {
  public static final String UDF_NAME = "mask_first_n";

  public GenericUDFMaskFirstN() {
    super(new MaskFirstNTransformer(), UDF_NAME);
  }
}

class MaskFirstNTransformer extends MaskTransformer {
  int charCount = 4;

  public MaskFirstNTransformer() {
    super();
  }

  @Override
  public void init(ObjectInspector[] arguments, int argsStartIdx) {
    super.init(arguments, argsStartIdx + 1); // first argument is charCount, which is consumed in this method below

    charCount = getIntArg(arguments, argsStartIdx, 4);

    if(charCount < 0) {
      charCount = 0;
    }
  }

  @Override
  String transform(final String value) {
    final StringBuilder ret    = new StringBuilder(value.length());
    final int           endIdx = value.length() < charCount ? value.length() : charCount;

    for(int i = 0; i < endIdx; i++) {
      ret.appendCodePoint(transformChar(value.charAt(i)));
    }

    for(int i = endIdx; i < value.length(); i++) {
      ret.appendCodePoint(value.charAt(i));
    }

    return ret.toString();
  }

  @Override
  Byte transform(final Byte value) {
    if (value == 0) {
      return charCount > 0 ? (byte) maskedNumber : 0;
    }
    byte val = value;

    if(value < 0) {
      val *= -1;
    }

    // count number of digits in the value
    int digitCount = 0;
    for(byte v = val; v != 0; v /= 10) {
      digitCount++;
    }

    // number of digits to retain from the end
    final int retainCount = digitCount < charCount ? 0 : (digitCount - charCount);

    byte ret = 0;
    int  pos = 1;
    for(int i = 0; val != 0; i++) {
      if(i >= retainCount) { // mask this digit
        ret += maskedNumber * pos;
      } else { //retain this digit
        ret += (val % 10) * pos;
      }

      val /= 10;
      pos *= 10;
    }

    if(value < 0) {
      ret *= -1;
    }

    return ret;
  }

  @Override
  Short transform(final Short value) {
    if (value == 0) {
      return charCount > 0 ? (short) maskedNumber : 0;
    }
    short val = value;

    if(value < 0) {
      val *= -1;
    }

    // count number of digits in the value
    int digitCount = 0;
    for(short v = val; v != 0; v /= 10) {
      digitCount++;
    }

    // number of digits to retain from the end
    final int retainCount = digitCount < charCount ? 0 : (digitCount - charCount);

    short ret = 0;
    int   pos = 1;
    for(int i = 0; val != 0; i++) {
      if(i >= retainCount) { // mask this digit
        ret += maskedNumber * pos;
      } else { // retain this digit
        ret += (val % 10) * pos;
      }

      val /= 10;
      pos *= 10;
    }

    if(value < 0) {
      ret *= -1;
    }

    return ret;
  }

  @Override
  Integer transform(final Integer value) {
    if (value == 0) {
      return charCount > 0 ? maskedNumber : 0;
    }
    int val = value;

    if(value < 0) {
      val *= -1;
    }

    // count number of digits in the value
    int digitCount = 0;
    for(int v = val; v != 0; v /= 10) {
      digitCount++;
    }

    // number of digits to retain from the end
    final int retainCount = digitCount < charCount ? 0 : (digitCount - charCount);

    int ret = 0;
    int pos = 1;
    for(int i = 0; val != 0; i++) {
      if(i >= retainCount) { // mask this digit
        ret += maskedNumber * pos;
      } else { // retain this digit
        ret += (val % 10) * pos;
      }

      val /= 10;
      pos *= 10;
    }

    if(value < 0) {
      ret *= -1;
    }

    return ret;
  }

  @Override
  Long transform(final Long value) {
    if (value == 0) {
      return charCount > 0 ? maskedNumber : 0L;
    }
    long val = value;

    if(value < 0) {
      val *= -1;
    }

    // count number of digits in the value
    int digitCount = 0;
    for(long v = val; v != 0; v /= 10) {
      digitCount++;
    }

    // number of digits to retain from the end
    final int retainCount = digitCount < charCount ? 0 : (digitCount - charCount);

    long ret = 0;
    long pos = 1;
    for(int i = 0; val != 0; i++) {
      if(i >= retainCount) { // mask this digit
        ret += maskedNumber * pos;
      } else { // retain this digit
        ret += (val % 10) * pos;
      }

      val /= 10;
      pos *= 10;
    }

    if(value < 0) {
      ret *= -1;
    }

    return ret;
  }
}
