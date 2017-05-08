/**
 * Licensed to the Apache Software Foundation (ASF) under one
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

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TimestampConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * GenericUDFTrunc.
 *
 * Returns the first day of the month which the date belongs to. The time part of the date will be
 * ignored.
 *
 */
@Description(name = "trunc", value = "_FUNC_(date, fmt) / _FUNC_(N,D) - Returns If input is date returns date with the time portion of the day truncated "
    + "to the unit specified by the format model fmt. If you omit fmt, then date is truncated to "
    + "the nearest day. It currently only supports 'MONTH'/'MON'/'MM', 'QUARTER'/'Q' and 'YEAR'/'YYYY'/'YY' as format."
    + "If input is a number group returns N truncated to D decimal places. If D is omitted, then N is truncated to 0 places."
    + "D can be negative to truncate (make zero) D digits left of the decimal point."
    , extended = "date is a string in the format 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'."
        + " The time part of date is ignored.\n" + "Example:\n "
        + " > SELECT _FUNC_('2009-02-12', 'MM');\n" + "OK\n" + " '2009-02-01'" + "\n"
        + " > SELECT _FUNC_('2017-03-15', 'Q');\n" + "OK\n" + " '2017-01-01'" + "\n"
        + " > SELECT _FUNC_('2015-10-27', 'YEAR');\n" + "OK\n" + " '2015-01-01'"
        + " > SELECT _FUNC_(1234567891.1234567891,4);\n" + "OK\n" + " 1234567891.1234" + "\n"
        + " > SELECT _FUNC_(1234567891.1234567891,-4);\n" + "OK\n" + " 1234560000"
        + " > SELECT _FUNC_(1234567891.1234567891,0);\n" + "OK\n" + " 1234567891" + "\n"
        + " > SELECT _FUNC_(1234567891.1234567891);\n" + "OK\n" + " 1234567891")
public class GenericUDFTrunc extends GenericUDF {

  private transient SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private transient TimestampConverter timestampConverter;
  private transient Converter textConverter1;
  private transient Converter textConverter2;
  private transient Converter dateWritableConverter;
  private transient Converter byteConverter;
  private transient Converter shortConverter;
  private transient Converter intConverter;
  private transient Converter longConverter;
  private transient PrimitiveCategory inputType1;
  private transient PrimitiveCategory inputType2;
  private final Calendar calendar = Calendar.getInstance();
  private final Text output = new Text();
  private transient String fmtInput;
  private transient PrimitiveObjectInspector inputOI;
  private transient PrimitiveObjectInspector inputScaleOI;
  private int scale = 0;
  private boolean inputSacleConst;
  private boolean dateTypeArg;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length == 2) {
      inputType1 = ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory();
      inputType2 = ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory();
      if ((PrimitiveObjectInspectorUtils
          .getPrimitiveGrouping(inputType1) == PrimitiveGrouping.DATE_GROUP
          || PrimitiveObjectInspectorUtils
              .getPrimitiveGrouping(inputType1) == PrimitiveGrouping.STRING_GROUP)
          && PrimitiveObjectInspectorUtils
              .getPrimitiveGrouping(inputType2) == PrimitiveGrouping.STRING_GROUP) {
        dateTypeArg = true;
        return initializeDate(arguments);
      } else if (PrimitiveObjectInspectorUtils
          .getPrimitiveGrouping(inputType1) == PrimitiveGrouping.NUMERIC_GROUP
          && PrimitiveObjectInspectorUtils
              .getPrimitiveGrouping(inputType2) == PrimitiveGrouping.NUMERIC_GROUP) {
        dateTypeArg = false;
        return initializeNumber(arguments);
      }
      throw new UDFArgumentException("Got wrong argument types : first argument type : "
          + arguments[0].getTypeName() + ", second argument type : " + arguments[1].getTypeName());
    } else if (arguments.length == 1) {
      inputType1 = ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory();
      if (PrimitiveObjectInspectorUtils
          .getPrimitiveGrouping(inputType1) == PrimitiveGrouping.NUMERIC_GROUP) {
        dateTypeArg = false;
        return initializeNumber(arguments);
      } else {
        throw new UDFArgumentException(
            "Only primitive type arguments are accepted, when arguments length is one, got "
                + arguments[1].getTypeName());
      }
    }
    throw new UDFArgumentException("TRUNC requires one or two argument, got " + arguments.length);
  }

  private ObjectInspector initializeNumber(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length < 1 || arguments.length > 2) {
      throw new UDFArgumentLengthException(
          "TRUNC requires one or two argument, got " + arguments.length);
    }

    if (arguments[0].getCategory() != Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "TRUNC input only takes primitive types, got " + arguments[0].getTypeName());
    }
    inputOI = (PrimitiveObjectInspector) arguments[0];

    if (arguments.length == 2) {
      if (arguments[1].getCategory() != Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(1,
            "TRUNC second argument only takes primitive types, got " + arguments[1].getTypeName());
      }

      inputScaleOI = (PrimitiveObjectInspector) arguments[1];
      inputSacleConst = arguments[1] instanceof ConstantObjectInspector;
      if (inputSacleConst) {
        try {
          Object obj = ((ConstantObjectInspector) arguments[1]).getWritableConstantValue();
          fmtInput = obj != null ? obj.toString() : null;
          scale = Integer.parseInt(fmtInput);
        } catch (Exception e) {
          throw new UDFArgumentException("TRUNC input only takes integer values, got " + fmtInput);
        }
      } else {
        switch (inputScaleOI.getPrimitiveCategory()) {
        case BYTE:
          byteConverter = ObjectInspectorConverters.getConverter(arguments[1],
              PrimitiveObjectInspectorFactory.writableByteObjectInspector);
          break;
        case SHORT:
          shortConverter = ObjectInspectorConverters.getConverter(arguments[1],
              PrimitiveObjectInspectorFactory.writableShortObjectInspector);
          break;
        case INT:
          intConverter = ObjectInspectorConverters.getConverter(arguments[1],
              PrimitiveObjectInspectorFactory.writableIntObjectInspector);
          break;
        case LONG:
          longConverter = ObjectInspectorConverters.getConverter(arguments[1],
              PrimitiveObjectInspectorFactory.writableLongObjectInspector);
          break;
        default:
          throw new UDFArgumentTypeException(1,
              getFuncName().toUpperCase() + " second argument only takes integer values");
        }
      }
    }

    inputType1 = inputOI.getPrimitiveCategory();
    ObjectInspector outputOI = null;
    switch (inputType1) {
    case DECIMAL:
      outputOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(inputType1);
      break;
    case VOID:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
      outputOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(inputType1);
      break;
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric or string group data types are allowed for TRUNC function. Got "
              + inputType1.name());
    }

    return outputOI;
  }

  private ObjectInspector initializeDate(ObjectInspector[] arguments)
      throws UDFArgumentLengthException, UDFArgumentTypeException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("trunc() requires 2 argument, got " + arguments.length);
    }

    if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but "
          + arguments[0].getTypeName() + " is passed. as first arguments");
    }

    if (arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(1, "Only primitive type arguments are accepted but "
          + arguments[1].getTypeName() + " is passed. as second arguments");
    }

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    inputType1 = ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory();
    switch (inputType1) {
    case STRING:
    case VARCHAR:
    case CHAR:
    case VOID:
      inputType1 = PrimitiveCategory.STRING;
      textConverter1 = ObjectInspectorConverters.getConverter(arguments[0],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      break;
    case TIMESTAMP:
      timestampConverter = new TimestampConverter((PrimitiveObjectInspector) arguments[0],
          PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
      break;
    case DATE:
      dateWritableConverter = ObjectInspectorConverters.getConverter(arguments[0],
          PrimitiveObjectInspectorFactory.writableDateObjectInspector);
      break;
    default:
      throw new UDFArgumentTypeException(0,
          "TRUNC() only takes STRING/TIMESTAMP/DATEWRITABLE types as first argument, got "
              + inputType1);
    }

    inputType2 = ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory();
    if (PrimitiveObjectInspectorUtils
        .getPrimitiveGrouping(inputType2) != PrimitiveGrouping.STRING_GROUP
        && PrimitiveObjectInspectorUtils
            .getPrimitiveGrouping(inputType2) != PrimitiveGrouping.VOID_GROUP) {
      throw new UDFArgumentTypeException(1,
          "trunk() only takes STRING/CHAR/VARCHAR types as second argument, got " + inputType2);
    }

    inputType2 = PrimitiveCategory.STRING;

    if (arguments[1] instanceof ConstantObjectInspector) {
      Object obj = ((ConstantObjectInspector) arguments[1]).getWritableConstantValue();
      fmtInput = obj != null ? obj.toString() : null;
    } else {
      textConverter2 = ObjectInspectorConverters.getConverter(arguments[1],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (dateTypeArg) {
      return evaluateDate(arguments);
    } else {
      return evaluateNumber(arguments);
    }
  }

  private Object evaluateDate(DeferredObject[] arguments) throws UDFArgumentLengthException,
      HiveException, UDFArgumentTypeException, UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("trunc() requires 2 argument, got " + arguments.length);
    }

    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }

    if (textConverter2 != null) {
      fmtInput = textConverter2.convert(arguments[1].get()).toString();
    }

    Date date;
    switch (inputType1) {
    case STRING:
      String dateString = textConverter1.convert(arguments[0].get()).toString();
      try {
        date = formatter.parse(dateString.toString());
      } catch (ParseException e) {
        return null;
      }
      break;
    case TIMESTAMP:
      Timestamp ts =
          ((TimestampWritable) timestampConverter.convert(arguments[0].get())).getTimestamp();
      date = ts;
      break;
    case DATE:
      DateWritable dw = (DateWritable) dateWritableConverter.convert(arguments[0].get());
      date = dw.get();
      break;
    default:
      throw new UDFArgumentTypeException(0,
          "TRUNC() only takes STRING/TIMESTAMP/DATEWRITABLE types, got " + inputType1);
    }

    if (evalDate(date) == null) {
      return null;
    }

    Date newDate = calendar.getTime();
    output.set(formatter.format(newDate));
    return output;
  }

  private Object evaluateNumber(DeferredObject[] arguments)
      throws HiveException, UDFArgumentTypeException {

    if (arguments[0] == null) {
      return null;
    }

    Object input = arguments[0].get();
    if (input == null) {
      return null;
    }

    if (arguments.length == 2 && arguments[1] != null && arguments[1].get() != null
        && !inputSacleConst) {
      Object scaleObj = null;
      switch (inputScaleOI.getPrimitiveCategory()) {
      case BYTE:
        scaleObj = byteConverter.convert(arguments[1].get());
        scale = ((ByteWritable) scaleObj).get();
        break;
      case SHORT:
        scaleObj = shortConverter.convert(arguments[1].get());
        scale = ((ShortWritable) scaleObj).get();
        break;
      case INT:
        scaleObj = intConverter.convert(arguments[1].get());
        scale = ((IntWritable) scaleObj).get();
        break;
      case LONG:
        scaleObj = longConverter.convert(arguments[1].get());
        long l = ((LongWritable) scaleObj).get();
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
          throw new UDFArgumentException(
              getFuncName().toUpperCase() + " scale argument out of allowed range");
        }
        scale = (int) l;
      default:
        break;
      }
    }

    switch (inputType1) {
    case VOID:
      return null;
    case DECIMAL:
      HiveDecimalWritable decimalWritable =
          (HiveDecimalWritable) inputOI.getPrimitiveWritableObject(input);
      HiveDecimal dec = trunc(decimalWritable.getHiveDecimal(), scale);
      if (dec == null) {
        return null;
      }
      return new HiveDecimalWritable(dec);
    case BYTE:
      ByteWritable byteWritable = (ByteWritable) inputOI.getPrimitiveWritableObject(input);
      if (scale >= 0) {
        return byteWritable;
      } else {
        return new ByteWritable((byte) trunc(byteWritable.get(), scale));
      }
    case SHORT:
      ShortWritable shortWritable = (ShortWritable) inputOI.getPrimitiveWritableObject(input);
      if (scale >= 0) {
        return shortWritable;
      } else {
        return new ShortWritable((short) trunc(shortWritable.get(), scale));
      }
    case INT:
      IntWritable intWritable = (IntWritable) inputOI.getPrimitiveWritableObject(input);
      if (scale >= 0) {
        return intWritable;
      } else {
        return new IntWritable((int) trunc(intWritable.get(), scale));
      }
    case LONG:
      LongWritable longWritable = (LongWritable) inputOI.getPrimitiveWritableObject(input);
      if (scale >= 0) {
        return longWritable;
      } else {
        return new LongWritable(trunc(longWritable.get(), scale));
      }
    case FLOAT:
      float f = ((FloatWritable) inputOI.getPrimitiveWritableObject(input)).get();
      return new FloatWritable((float) trunc(f, scale));
    case DOUBLE:
      return trunc(((DoubleWritable) inputOI.getPrimitiveWritableObject(input)), scale);
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric or string group data types are allowed for TRUNC function. Got "
              + inputType1.name());
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("trunc", children);
  }

  private Calendar evalDate(Date d) throws UDFArgumentException {
    calendar.setTime(d);
    if ("MONTH".equals(fmtInput) || "MON".equals(fmtInput) || "MM".equals(fmtInput)) {
      calendar.set(Calendar.DAY_OF_MONTH, 1);
      return calendar;
    } else if ("QUARTER".equals(fmtInput) || "Q".equals(fmtInput)) {
      int month = calendar.get(Calendar.MONTH);
      int quarter = month / 3;
      int monthToSet = quarter * 3;
      calendar.set(Calendar.MONTH, monthToSet);
      calendar.set(Calendar.DAY_OF_MONTH, 1);
      return calendar;
    } else if ("YEAR".equals(fmtInput) || "YYYY".equals(fmtInput) || "YY".equals(fmtInput)) {
      calendar.set(Calendar.MONTH, 0);
      calendar.set(Calendar.DAY_OF_MONTH, 1);
      return calendar;
    } else {
      return null;
    }
  }

  protected HiveDecimal trunc(HiveDecimal input, int scale) {
    BigDecimal bigDecimal = trunc(input.bigDecimalValue(), scale);
    return HiveDecimal.create(bigDecimal);
  }

  protected long trunc(long input, int scale) {
    return trunc(BigDecimal.valueOf(input), scale).longValue();
  }

  protected double trunc(double input, int scale) {
    return trunc(BigDecimal.valueOf(input), scale).doubleValue();
  }

  protected DoubleWritable trunc(DoubleWritable input, int scale) {
    BigDecimal bigDecimal = new BigDecimal(input.get());
    BigDecimal trunc = trunc(bigDecimal, scale);
    DoubleWritable doubleWritable = new DoubleWritable(trunc.doubleValue());
    return doubleWritable;
  }

  protected BigDecimal trunc(BigDecimal input, int scale) {
    BigDecimal output = new BigDecimal(0);
    BigDecimal pow = BigDecimal.valueOf(Math.pow(10, Math.abs(scale)));
    if (scale >= 0) {
      pow = BigDecimal.valueOf(Math.pow(10, scale));
      if (scale != 0) {
        long longValue = input.multiply(pow).longValue();
        output = BigDecimal.valueOf(longValue).divide(pow);
      } else {
        output = BigDecimal.valueOf(input.longValue());
      }
    } else {
      long longValue2 = input.divide(pow).longValue();
      output = BigDecimal.valueOf(longValue2).multiply(pow);
    }
    return output;
  }
  
}
