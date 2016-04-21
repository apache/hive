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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;


@Description(name = "mask_show_last_n",
             value = "masks all but last n characters of the value",
             extended = "Examples:\n "
                      + "  mask_show_last_n(ccn, 8)\n "
                      + "  mask_show_last_n(ccn, 8, 'x', 'x', 'x')\n "
                      + "Arguments:\n "
                      + "  mask_show_last_n(value, charCount, upperChar, lowerChar, digitChar, otherChar, numberChar)\n "
                      + "    value      - value to mask. Supported types: TINYINT, SMALLINT, INT, BIGINT, STRING, VARCHAR, CHAR\n "
                      + "    charCount  - number of characters. Default value: 4\n "
                      + "    upperChar  - character to replace upper-case characters with. Specify -1 to retain original character. Default value: 'X'\n "
                      + "    lowerChar  - character to replace lower-case characters with. Specify -1 to retain original character. Default value: 'x'\n "
                      + "    digitChar  - character to replace digit characters with. Specify -1 to retain original character. Default value: 'n'\n "
                      + "    otherChar  - character to replace all other characters with. Specify -1 to retain original character. Default value: -1\n "
                      + "    numberChar - character to replace digits in a number with. Valid values: 0-9. Default value: '1'\n "
            )
public class GenericUDFMaskShowLastN extends BaseMaskUDF {
  public static final String UDF_NAME = "mask_show_last_n";

  public GenericUDFMaskShowLastN() {
    super(new MaskShowLastNTransformer(), UDF_NAME);
  }
}

class MaskShowLastNTransformer extends MaskTransformer {
  int charCount = 4;

  public MaskShowLastNTransformer() {
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
    if(value.length() <= charCount) {
      return value;
    }

    final StringBuilder ret    = new StringBuilder(value.length());
    final int           endIdx = value.length() - charCount;

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
    byte val = value;

    if(value < 0) {
      val *= -1;
    }

    byte ret = 0;
    int  pos = 1;
    for(int i = 0; val != 0; i++) {
      if(i >= charCount) { // mask this digit
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
    short val = value;

    if(value < 0) {
      val *= -1;
    }

    short ret = 0;
    int   pos = 1;
    for(int i = 0; val != 0; i++) {
      if(i >= charCount) { // mask this digit
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
    int val = value;

    if(value < 0) {
      val *= -1;
    }

    int ret = 0;
    int pos = 1;
    for(int i = 0; val != 0; i++) {
      if(i >= charCount) { // mask this digit
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
    long val = value;

    if(value < 0) {
      val *= -1;
    }

    long ret = 0;
    long pos = 1;
    for(int i = 0; val != 0; i++) {
      if(i >= charCount) { // mask this digit
        ret += (maskedNumber * pos);
      } else { // retain this digit
        ret += ((val % 10) * pos);
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
