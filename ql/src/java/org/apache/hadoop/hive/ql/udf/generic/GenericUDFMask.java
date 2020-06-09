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


import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


@Description(name = "mask",
             value = "masks the given value",
             extended = "Examples:\n "
                      + "  mask(ccn)\n "
                      + "  mask(ccn, 'X', 'x', '0')\n "
                      + "  mask(ccn, 'x', 'x', 'x')\n "
                      + "Arguments:\n "
                      + "  mask(value, upperChar, lowerChar, digitChar, otherChar, numberChar, dayValue, monthValue, yearValue)\n "
                      + "    value      - value to mask. Supported types: TINYINT, SMALLINT, INT, BIGINT, STRING, VARCHAR, CHAR, DATE\n "
                      + "    upperChar  - character to replace upper-case characters with. Specify -1 to retain original character. Default value: 'X'\n "
                      + "    lowerChar  - character to replace lower-case characters with. Specify -1 to retain original character. Default value: 'x'\n "
                      + "    digitChar  - character to replace digit characters with. Specify -1 to retain original character. Default value: 'n'\n "
                      + "    otherChar  - character to replace all other characters with. Specify -1 to retain original character. Default value: -1\n "
                      + "    numberChar - character to replace digits in a number with. Valid values: 0-9. Default value: '1'\n "
                      + "    dayValue   - value to replace day field in a date with.  Specify -1 to retain original value. Valid values: 1-31. Default value: 1\n "
                      + "    monthValue - value to replace month field in a date with. Specify -1 to retain original value. Valid values: 0-11. Default value: 0\n "
                      + "    yearValue  - value to replace year field in a date with. Specify -1 to retain original value. Default value: 0\n "
           )
public class GenericUDFMask extends BaseMaskUDF {
  public static final String UDF_NAME = "mask";

  public GenericUDFMask() {
    super(new MaskTransformer(), UDF_NAME);
  }
}

class MaskTransformer extends AbstractTransformer {
  final static int MASKED_UPPERCASE           = 'X';
  final static int MASKED_LOWERCASE           = 'x';
  final static int MASKED_DIGIT               = 'n';
  final static int MASKED_OTHER_CHAR          = -1;
  final static int MASKED_NUMBER              = 1;
  final static int MASKED_DAY_COMPONENT_VAL   = 1;
  final static int MASKED_MONTH_COMPONENT_VAL = 0;
  final static int MASKED_YEAR_COMPONENT_VAL  = 0;
  final static int UNMASKED_VAL               = -1;

  int maskedUpperChar  = MASKED_UPPERCASE;
  int maskedLowerChar  = MASKED_LOWERCASE;
  int maskedDigitChar  = MASKED_DIGIT;
  int maskedOtherChar  = MASKED_OTHER_CHAR;
  int maskedNumber     = MASKED_NUMBER;
  int maskedDayValue   = MASKED_DAY_COMPONENT_VAL;
  int maskedMonthValue = MASKED_MONTH_COMPONENT_VAL;
  int maskedYearValue  = MASKED_YEAR_COMPONENT_VAL;

  public MaskTransformer() {
  }

  @Override
  public void init(ObjectInspector[] arguments, int startIdx) {
    int idx = startIdx;

    maskedUpperChar  = getCharArg(arguments, idx++, MASKED_UPPERCASE);
    maskedLowerChar  = getCharArg(arguments, idx++, MASKED_LOWERCASE);
    maskedDigitChar  = getCharArg(arguments, idx++, MASKED_DIGIT);
    maskedOtherChar  = getCharArg(arguments, idx++, MASKED_OTHER_CHAR);
    maskedNumber     = getIntArg(arguments, idx++, MASKED_NUMBER);
    maskedDayValue   = getIntArg(arguments, idx++, MASKED_DAY_COMPONENT_VAL);
    maskedMonthValue = getIntArg(arguments, idx++, MASKED_MONTH_COMPONENT_VAL);
    maskedYearValue  = getIntArg(arguments, idx++, MASKED_YEAR_COMPONENT_VAL);

    if(maskedNumber < 0 || maskedNumber > 9) {
      maskedNumber = MASKED_NUMBER;
    }

    if(maskedDayValue != UNMASKED_VAL) {
      if(maskedDayValue < 1 || maskedDayValue > 31) {
        maskedDayValue = MASKED_DAY_COMPONENT_VAL;
      }
    }

    if(maskedMonthValue != UNMASKED_VAL) {
      if(maskedMonthValue < 0 || maskedMonthValue > 11) {
        maskedMonthValue = MASKED_MONTH_COMPONENT_VAL;
      }
    }
  }

  @Override
  String transform(final String val) {
    StringBuilder ret = new StringBuilder(val.length());

    for(int i = 0; i < val.length(); i++) {
      ret.appendCodePoint(transformChar(val.charAt(i)));
    }

    return ret.toString();
  }

  @Override
  Byte transform(final Byte value) {
    if (value == 0) {
      return (byte) maskedNumber;
    }
    byte val = value;

    if(value < 0) {
      val *= -1;
    }

    byte ret = 0;
    int  pos = 1;
    while(val != 0) {
      ret += maskedNumber * pos;

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
      return (short) maskedNumber;
    }
    short val = value;

    if(value < 0) {
      val *= -1;
    }

    short ret = 0;
    int   pos = 1;
    while(val != 0) {
      ret += maskedNumber * pos;

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
      return maskedNumber;
    }
    int val = value;

    if(value < 0) {
      val *= -1;
    }

    int ret = 0;
    int pos = 1;
    while(val != 0) {
      ret += maskedNumber * pos;

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
      return (long) maskedNumber;
    }
    long val = value;

    if(value < 0) {
      val *= -1;
    }

    long ret = 0;
    long pos = 1;
    for(int i = 0; val != 0; i++) {
      ret += maskedNumber * pos;

      val /= 10;
      pos *= 10;
    }

    if(value < 0) {
      ret *= -1;
    }

    return ret;
  }

  @Override
  Date transform(final Date value) {
    int actualMonthValue = maskedMonthValue + 1;
    int year  = maskedYearValue  == UNMASKED_VAL ? value.getYear()  : maskedYearValue;
    int month = maskedMonthValue == UNMASKED_VAL ? value.getMonth() : actualMonthValue;
    int day   = maskedDayValue   == UNMASKED_VAL ? value.getDay()  : maskedDayValue;

    return Date.of(year, month, day);
  }

  protected int transformChar(final int c) {
    switch(Character.getType(c)) {
      case Character.UPPERCASE_LETTER:
        if(maskedUpperChar != UNMASKED_VAL) {
          return maskedUpperChar;
        }
        break;

      case Character.LOWERCASE_LETTER:
        if(maskedLowerChar != UNMASKED_VAL) {
          return maskedLowerChar;
        }
        break;

      case Character.DECIMAL_DIGIT_NUMBER:
        if(maskedDigitChar != UNMASKED_VAL) {
          return maskedDigitChar;
        }
        break;

      default:
        if(maskedOtherChar != UNMASKED_VAL) {
          return maskedOtherChar;
        }
        break;
    }

    return c;
  }

  int getCharArg(ObjectInspector[] arguments, int index, int defaultValue) {
    int ret = defaultValue;

    ObjectInspector arg = (arguments != null && arguments.length > index) ? arguments[index] : null;

    if (arg != null) {
      if(arg instanceof WritableConstantIntObjectInspector) {
        IntWritable value = ((WritableConstantIntObjectInspector)arg).getWritableConstantValue();

        if(value != null) {
          ret = value.get();
        }
      } else if(arg instanceof WritableConstantLongObjectInspector) {
        LongWritable value = ((WritableConstantLongObjectInspector)arg).getWritableConstantValue();

        if(value != null) {
          ret = (int)value.get();
        }
      } else if(arg instanceof WritableConstantShortObjectInspector) {
        ShortWritable value = ((WritableConstantShortObjectInspector)arg).getWritableConstantValue();

        if(value != null) {
          ret = value.get();
        }
      } else if(arg instanceof ConstantObjectInspector) {
        Object value = ((ConstantObjectInspector) arg).getWritableConstantValue();

        if (value != null) {
          String strValue = value.toString();

          if (strValue != null && strValue.length() > 0) {
            ret = strValue.charAt(0);
          }
        }
      }
    }

    return ret;
  }

  int getIntArg(ObjectInspector[] arguments, int index, int defaultValue) {
    int ret = defaultValue;

    ObjectInspector arg = (arguments != null && arguments.length > index) ? arguments[index] : null;

    if (arg != null) {
      if (arg instanceof WritableConstantIntObjectInspector) {
        IntWritable value = ((WritableConstantIntObjectInspector) arg).getWritableConstantValue();

        if (value != null) {
          ret = value.get();
        }
      } else if (arg instanceof WritableConstantLongObjectInspector) {
        LongWritable value = ((WritableConstantLongObjectInspector) arg).getWritableConstantValue();

        if (value != null) {
          ret = (int) value.get();
        }
      } else if (arg instanceof WritableConstantShortObjectInspector) {
        ShortWritable value = ((WritableConstantShortObjectInspector) arg).getWritableConstantValue();

        if (value != null) {
          ret = value.get();
        }
      } else if (arg instanceof ConstantObjectInspector) {
        Object value = ((ConstantObjectInspector) arg).getWritableConstantValue();

        if (value != null) {
          String strValue = value.toString();

          if (strValue != null && strValue.length() > 0) {
            ret = Integer.parseInt(value.toString());
          }
        }
      }
    }

    return ret;
  }
}

