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

import java.util.function.Function;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;

/**
 * GenericUDFIcebergYear - UDF that wraps around Iceberg's year transform function
 */
@Description(name = "iceberg_year",
    value = "_FUNC_(value) - " +
     "Returns the bucket value calculated by Iceberg year transform function ",
    extended = "Example:\n  > SELECT _FUNC_('2023-01-01');\n  2023")
public class GenericUDFIcebergYear extends GenericUDF {
  private final IntWritable result = new IntWritable();
  private transient PrimitiveObjectInspector argumentOI;
  private transient ObjectInspectorConverters.Converter converter;

  @FunctionalInterface
  private interface UDFEvalFunction<T> {
    void apply(T argument) throws HiveException;
  }

  private transient UDFEvalFunction<DeferredObject> evaluator;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException(
        "ICEBERG_YEAR requires 1 arguments (value), but got " + arguments.length);
    }

    if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentException(
        "ICEBERG_YEAR first argument takes primitive types, got " + argumentOI.getTypeName());
    }
    argumentOI = (PrimitiveObjectInspector) arguments[0];

    PrimitiveObjectInspector.PrimitiveCategory inputType = argumentOI.getPrimitiveCategory();
    ObjectInspector outputOI;
    switch (inputType) {
      case DATE:
        converter = new PrimitiveObjectInspectorConverter.DateConverter(argumentOI,
          PrimitiveObjectInspectorFactory.writableDateObjectInspector);
        Function<Object, Integer> dateTransform = Transforms.year().bind(Types.DateType.get());
        evaluator = arg -> {
          DateWritableV2 val = (DateWritableV2) converter.convert(arg.get());
          result.set(dateTransform.apply(val.getDays()));
        };
        break;

      case TIMESTAMP:
        converter = new PrimitiveObjectInspectorConverter.TimestampConverter(argumentOI,
          PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
        Function<Object, Integer> timestampTransform = Transforms.year().bind(Types.TimestampType.withoutZone());
        evaluator = arg -> {
          TimestampWritableV2 val = (TimestampWritableV2) converter.convert(arg.get());
          result.set(timestampTransform.apply(Double.valueOf(val.getMicros()).longValue()));
        };
        break;

      case TIMESTAMPLOCALTZ:
        converter = new PrimitiveObjectInspectorConverter.TimestampLocalTZConverter(argumentOI,
          PrimitiveObjectInspectorFactory.writableTimestampTZObjectInspector);
        Function<Object, Integer> timestampLocalTzTransform = Transforms.year().bind(Types.TimestampType.withZone());
        evaluator = arg -> {
          TimestampLocalTZWritable val = (TimestampLocalTZWritable) converter.convert(arg.get());
          result.set(timestampLocalTzTransform.apply(Double.valueOf(val.getMicros()).longValue()));
        };
        break;

      default:
        throw new UDFArgumentException(
          " ICEBERG_YEAR() only takes DATE/TIMESTAMP/TIMESTAMPLOCALTZ" +
          " types as first argument, got " + inputType);
    }
    outputOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    return outputOI;
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
    return getStandardDisplayString("iceberg_year", children);
  }

}
