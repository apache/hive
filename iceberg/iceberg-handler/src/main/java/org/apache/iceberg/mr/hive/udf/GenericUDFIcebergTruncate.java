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

package org.apache.iceberg.mr.hive.udf;

import java.math.BigDecimal;
import java.util.function.Function;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * GenericUDFIcebergTruncate - UDF that wraps around Iceberg's truncate transform function
 */
@Description(name = "iceberg_truncate",
    value = "_FUNC_(value, truncateLength) - " +
     "Returns the bucket value calculated by Iceberg bucket transform function ",
    extended = "Example:\n  > SELECT _FUNC_('abcdefgh', 5);\n  abcde")
public class GenericUDFIcebergTruncate extends GenericUDF {
  private final Text result = new Text();
  private int truncateLength = 0;
  private transient PrimitiveObjectInspector argumentOI;
  private transient ObjectInspectorConverters.Converter converter;

  @FunctionalInterface
  private interface UDFEvalFunction<T> {
    void apply(T argument) throws HiveException;
  }

  private transient UDFEvalFunction<DeferredObject> evaluator;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
        "ICEBERG_BUCKET requires 2 arguments (value, bucketCount), but got " + arguments.length);
    }

    truncateLength = getTruncateLength(arguments[1]);

    if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentException(
        "ICEBERG_BUCKET first argument takes primitive types, got " + argumentOI.getTypeName());
    }
    argumentOI = (PrimitiveObjectInspector) arguments[0];

    PrimitiveObjectInspector.PrimitiveCategory inputType = argumentOI.getPrimitiveCategory();
    ObjectInspector outputOI;
    switch (inputType) {
      case CHAR:
      case VARCHAR:
      case STRING:
        converter = new PrimitiveObjectInspectorConverter.StringConverter(argumentOI);
        Function<Object, Object> stringTransform = Transforms.truncate(truncateLength).bind(Types.StringType.get());
        evaluator = arg -> {
          String val = (String) converter.convert(arg.get());
          result.set(String.valueOf(stringTransform.apply(val)));
        };
        break;

      case INT:
        converter = new PrimitiveObjectInspectorConverter.IntConverter(argumentOI,
          PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        Function<Object, Object> intTransform = Transforms.truncate(truncateLength).bind(Types.IntegerType.get());
        evaluator = arg -> {
          IntWritable val = (IntWritable) converter.convert(arg.get());
          result.set(String.valueOf(intTransform.apply(val.get())));
        };
        break;

      case LONG:
        converter = new PrimitiveObjectInspectorConverter.LongConverter(argumentOI,
          PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        Function<Object, Object> longTransform = Transforms.truncate(truncateLength).bind(Types.LongType.get());
        evaluator = arg -> {
          LongWritable val = (LongWritable) converter.convert(arg.get());
          result.set(String.valueOf(longTransform.apply(val.get())));
        };
        break;

      case DECIMAL:
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) TypeInfoUtils.getTypeInfoFromObjectInspector(argumentOI);
        Type.PrimitiveType decimalIcebergType = Types.DecimalType.of(decimalTypeInfo.getPrecision(),
            decimalTypeInfo.getScale());

        converter = new PrimitiveObjectInspectorConverter.HiveDecimalConverter(argumentOI,
          PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector);
        Function<Object, Object> bigDecimalTransform = Transforms.truncate(truncateLength).bind(decimalIcebergType);
        evaluator = arg -> {
          HiveDecimalWritable val = (HiveDecimalWritable) converter.convert(arg.get());
          result.set(((BigDecimal) bigDecimalTransform.apply(val.getHiveDecimal().bigDecimalValue())).toPlainString());
        };
        break;

      case FLOAT:
        converter = new PrimitiveObjectInspectorConverter.FloatConverter(argumentOI,
          PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        Function<Object, Object> floatTransform = Transforms.truncate(truncateLength).bind(Types.FloatType.get());
        evaluator = arg -> {
          FloatWritable val = (FloatWritable) converter.convert(arg.get());
          result.set(String.valueOf(floatTransform.apply(val.get())));
        };
        break;

      case DOUBLE:
        converter = new PrimitiveObjectInspectorConverter.DoubleConverter(argumentOI,
          PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        Function<Object, Object> doubleTransform = Transforms.truncate(truncateLength).bind(Types.DoubleType.get());
        evaluator = arg -> {
          DoubleWritable val = (DoubleWritable) converter.convert(arg.get());
          result.set(String.valueOf(doubleTransform.apply(val.get())));
        };
        break;

      default:
        throw new UDFArgumentException(
          " ICEBERG_TRUNCATE() only takes STRING/CHAR/VARCHAR/INT/LONG/DECIMAL/FLOAT/DOUBLE" +
          " types as first argument, got " + inputType);
    }
    outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    return outputOI;
  }

  private static int getTruncateLength(ObjectInspector arg) throws UDFArgumentException {
    UDFArgumentException udfArgumentException = new UDFArgumentException("ICEBERG_TRUNCATE() second argument can " +
        " only take an int type, but got " + arg.getTypeName());
    if (arg.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw udfArgumentException;
    }
    PrimitiveObjectInspector.PrimitiveCategory inputType = ((PrimitiveObjectInspector) arg).getPrimitiveCategory();
    if (inputType != PrimitiveObjectInspector.PrimitiveCategory.INT) {
      throw udfArgumentException;
    }
    return ((WritableConstantIntObjectInspector) arg).getWritableConstantValue().get();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    DeferredObject argument = arguments[0];
    if (argument == null || argument.get() == null) {
      return null;
    } else {
      evaluator.apply(argument);
    }
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("iceberg_truncate", children);
  }
}
