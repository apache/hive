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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FuncRoundWithNumDigitsDecimalToDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.RoundWithNumDigitsDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncRoundDecimalToDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncRoundDoubleToDouble;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantShortObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Note: rounding function permits rounding off integer digits in decimal numbers, which essentially
 * downgrades the scale to negative territory. However, Hive decimal only allow non-negative scales.
 * This can cause confusion, for example, when a decimal number 1234.567 of type decimal(7,3) is
 * rounded with -2 scale, which produces a decimal number, 1200. The type of the output is
 * decimal(5,0), which does not exactly represents what the number means. Thus, user should
 * be aware of this, and use negative rounding for decimal with caution. However, Hive is in line
 * with the behavior that MYSQL demonstrates.
 *
 * At a certain point, we should probably support negative scale for decimal type.
 *
 * GenericUDFRound.
 *
 */
@Description(name = "round",
  value = "_FUNC_(x[, d]) - round x to d decimal places",
  extended = "Example:\n"
    + "  > SELECT _FUNC_(12.3456, 1) FROM src LIMIT 1;\n" + "  12.3'")
@VectorizedExpressions({FuncRoundDoubleToDouble.class, RoundWithNumDigitsDoubleToDouble.class,
    FuncRoundWithNumDigitsDecimalToDecimal.class, FuncRoundDecimalToDecimal.class})
public class GenericUDFRound extends GenericUDF {
  private transient PrimitiveObjectInspector inputOI;
  private int scale = 0;

  private transient PrimitiveCategory inputType;
  private transient Converter converterFromString;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 1 || arguments.length > 2) {
      throw new UDFArgumentLengthException(
          "ROUND requires one or two argument, got " + arguments.length);
    }

    if (arguments[0].getCategory() != Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "ROUND input only takes primitive types, got " + arguments[0].getTypeName());
    }
    inputOI = (PrimitiveObjectInspector) arguments[0];

    if (arguments.length == 2) {
      if (arguments[1].getCategory() != Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(1,
            "ROUND second argument only takes primitive types, got " + arguments[1].getTypeName());
      }
      PrimitiveObjectInspector scaleOI = (PrimitiveObjectInspector) arguments[1];
      switch (scaleOI.getPrimitiveCategory()) {
      case VOID:
        break;
      case BYTE:
        if (!(scaleOI instanceof WritableConstantByteObjectInspector)) {
          throw new UDFArgumentTypeException(1, getFuncName().toUpperCase() + " second argument only takes constant");
        }
        scale = ((WritableConstantByteObjectInspector)scaleOI).getWritableConstantValue().get();
        break;
      case SHORT:
        if (!(scaleOI instanceof WritableConstantShortObjectInspector)) {
          throw new UDFArgumentTypeException(1, getFuncName().toUpperCase() + " second argument only takes constant");
        }
        scale = ((WritableConstantShortObjectInspector)scaleOI).getWritableConstantValue().get();
        break;
      case INT:
        if (!(scaleOI instanceof WritableConstantIntObjectInspector)) {
          throw new UDFArgumentTypeException(1, getFuncName().toUpperCase() + " second argument only takes constant");
        }
        scale = ((WritableConstantIntObjectInspector)scaleOI).getWritableConstantValue().get();
        break;
      case LONG:
        if (!(scaleOI instanceof WritableConstantLongObjectInspector)) {
          throw new UDFArgumentTypeException(1, getFuncName().toUpperCase()
              + " second argument only takes constant");
        }
        long l = ((WritableConstantLongObjectInspector)scaleOI).getWritableConstantValue().get();
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
          throw new UDFArgumentException(getFuncName().toUpperCase()
              + " scale argument out of allowed range");
        }
        scale = (int)l;
        break;
      default:
        throw new UDFArgumentTypeException(1, getFuncName().toUpperCase()
            + " second argument only takes integer constant");
      }
    }

    inputType = inputOI.getPrimitiveCategory();
    ObjectInspector outputOI = null;
    switch (inputType) {
    case DECIMAL:
      DecimalTypeInfo inputTypeInfo = (DecimalTypeInfo) inputOI.getTypeInfo();
      DecimalTypeInfo typeInfo = getOutputTypeInfo(inputTypeInfo, scale);
      outputOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(typeInfo);
      break;
    case VOID:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
      outputOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(inputType);
      break;
    case STRING:
    case VARCHAR:
    case CHAR:
      outputOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.DOUBLE);
      converterFromString = ObjectInspectorConverters.getConverter(inputOI, outputOI);
      break;
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric or string group data types are allowed for ROUND function. Got "
              + inputType.name());
    }

    return outputOI;
  }

  private static DecimalTypeInfo getOutputTypeInfo(DecimalTypeInfo inputTypeInfo, int dec) {
    int prec = inputTypeInfo.precision();
    int scale = inputTypeInfo.scale();
    int intParts = prec - scale;

    // If we are rounding, we may introduce one more integer digit.
    int newIntParts = dec < scale ? intParts + 1 : intParts;

    int newScale = dec < 0 ? 0 : Math.min(dec, HiveDecimal.MAX_SCALE);

    int newPrec = Math.min(newIntParts + newScale, HiveDecimal.MAX_PRECISION);

    return TypeInfoFactory.getDecimalTypeInfo(newPrec, newScale);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments.length == 2 && (arguments[1] == null || arguments[1].get() == null)) {
      return null;
    }

    if (arguments[0] == null) {
      return null;
    }

    Object input = arguments[0].get();
    if (input == null) {
      return null;
    }

    switch (inputType) {
    case VOID:
      return null;
    case DECIMAL:
      {
        // The getPrimitiveWritableObject method returns a new writable.
        HiveDecimalWritable decimalWritable = (HiveDecimalWritable) inputOI.getPrimitiveWritableObject(input);
        // Call the different round flavor.
        return round(decimalWritable, scale);
      }
    case BYTE:
      ByteWritable byteWritable = (ByteWritable)inputOI.getPrimitiveWritableObject(input);
      if (scale >= 0) {
        return byteWritable;
      } else {
        return new ByteWritable((byte)round(byteWritable.get(), scale));
      }
    case SHORT:
      ShortWritable shortWritable = (ShortWritable)inputOI.getPrimitiveWritableObject(input);
      if (scale >= 0) {
        return shortWritable;
      } else {
        return new ShortWritable((short)round(shortWritable.get(), scale));
      }
    case INT:
      IntWritable intWritable = (IntWritable)inputOI.getPrimitiveWritableObject(input);
      if (scale >= 0) {
        return intWritable;
      } else {
        return new IntWritable((int)round(intWritable.get(), scale));
      }
    case LONG:
      LongWritable longWritable = (LongWritable)inputOI.getPrimitiveWritableObject(input);
      if (scale >= 0) {
        return longWritable;
      } else {
        return new LongWritable(round(longWritable.get(), scale));
      }
    case FLOAT:
      float f = ((FloatWritable)inputOI.getPrimitiveWritableObject(input)).get();
      return new FloatWritable((float)round(f, scale));
     case DOUBLE:
       return round(((DoubleWritable)inputOI.getPrimitiveWritableObject(input)), scale);
    case STRING:
    case VARCHAR:
    case CHAR:
       DoubleWritable doubleValue = (DoubleWritable) converterFromString.convert(input);
       if (doubleValue == null) {
         return null;
       }
       return round(doubleValue, scale);
     default:
      throw new UDFArgumentTypeException(0,
          "Only numeric or string group data types are allowed for ROUND function. Got "
              + inputType.name());
    }
  }

  protected HiveDecimalWritable round(HiveDecimalWritable inputDecWritable, int scale) {
    HiveDecimalWritable result = new HiveDecimalWritable(inputDecWritable);
    result.mutateSetScale(scale, HiveDecimal.ROUND_HALF_UP);
    return result;
  }

  protected long round(long input, int scale) {
    return RoundUtils.round(input, scale);
  }

  protected double round(double input, int scale) {
    return RoundUtils.round(input, scale);
  }

  protected DoubleWritable round(DoubleWritable input, int scale) {
    double d = input.get();
    if (Double.isNaN(d) || Double.isInfinite(d)) {
      return new DoubleWritable(d);
    } else {
      return new DoubleWritable(RoundUtils.round(d, scale));
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("round", children);
  }

}
