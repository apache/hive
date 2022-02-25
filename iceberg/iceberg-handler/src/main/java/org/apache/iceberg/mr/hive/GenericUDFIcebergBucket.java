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

package org.apache.iceberg.mr.hive;

import java.nio.ByteBuffer;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * GenericUDFIcebergBucket - UDF that wraps around Iceberg's bucket transform function
 */
@Description(name = "iceberg_bucket",
    value = "_FUNC_(value, bucketCount) - " +
        "Returns the bucket value calculated by Iceberg bucket transform function ",
    extended = "Example:\n  > SELECT _FUNC_('A bucket full of ice!', 5);\n  4")
//@VectorizedExpressions({StringLength.class})
public class GenericUDFIcebergBucket extends GenericUDF {
  private final IntWritable result = new IntWritable();
  private transient PrimitiveObjectInspector argumentOI;
  private transient PrimitiveObjectInspectorConverter.StringConverter stringConverter;
  private transient PrimitiveObjectInspectorConverter.BinaryConverter binaryConverter;
  private transient PrimitiveObjectInspectorConverter.IntConverter intConverter;
  private transient PrimitiveObjectInspectorConverter.LongConverter longConverter;
  private transient PrimitiveObjectInspectorConverter.HiveDecimalConverter decimalConverter;
  private transient PrimitiveObjectInspectorConverter.FloatConverter floatConverter;
  private transient PrimitiveObjectInspectorConverter.DoubleConverter doubleConverter;
  private transient Type.PrimitiveType icebergType;
  private int numBuckets = -1;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
          "ICEBERG_BUCKET requires 2 argument, got " + arguments.length);
    }

    if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentException(
          "ICEBERG_BUCKET first argument takes primitive types, got " + argumentOI.getTypeName());
    }
    argumentOI = (PrimitiveObjectInspector) arguments[0];

    PrimitiveObjectInspector.PrimitiveCategory inputType = argumentOI.getPrimitiveCategory();
    ObjectInspector outputOI = null;
    switch (inputType) {
      case CHAR:
      case VARCHAR:
      case STRING:
        icebergType = Types.StringType.get();
        stringConverter = new PrimitiveObjectInspectorConverter.StringConverter(argumentOI);
        break;

      case BINARY:
        icebergType = Types.BinaryType.get();
        binaryConverter = new PrimitiveObjectInspectorConverter.BinaryConverter(argumentOI,
            PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
        break;

      case INT:
        icebergType = Types.IntegerType.get();
        intConverter = new PrimitiveObjectInspectorConverter.IntConverter(argumentOI,
            PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        break;

      case LONG:
        icebergType = Types.LongType.get();
        longConverter = new PrimitiveObjectInspectorConverter.LongConverter(argumentOI,
            PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        break;

      case DECIMAL:
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) TypeInfoUtils.getTypeInfoFromObjectInspector(argumentOI);
        icebergType = Types.DecimalType.of(decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());

        decimalConverter = new PrimitiveObjectInspectorConverter.HiveDecimalConverter(argumentOI,
            PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector);
        break;

      case FLOAT:
        icebergType = Types.FloatType.get();
        floatConverter = new PrimitiveObjectInspectorConverter.FloatConverter(argumentOI,
            PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        break;

      case DOUBLE:
        icebergType = Types.DoubleType.get();
        doubleConverter = new PrimitiveObjectInspectorConverter.DoubleConverter(argumentOI,
            PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        break;


      default:
        throw new UDFArgumentException(
            " ICEBERG_BUCKET() only takes STRING/CHAR/VARCHAR/BINARY/INT/LONG/DECIMAL/FLOAT/DOUBLE" +
                " types as first argument, got " + inputType);
    }

    argumentOI = (PrimitiveObjectInspector) arguments[1];
    if (!isValid2ndArgumentType(arguments[1])) {
      throw new UDFArgumentTypeException(1,
          "ICEBERG_BUCKET() second argument only takes an int types, got " + arguments[1].getTypeName());
    }
    numBuckets = ((WritableConstantIntObjectInspector) argumentOI).getWritableConstantValue().get();

    outputOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    return outputOI;
  }

  private boolean isValid2ndArgumentType(ObjectInspector arg) {
    if (arg.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      return false;
    } else {
      PrimitiveObjectInspector.PrimitiveCategory inputType = argumentOI.getPrimitiveCategory();
      if (inputType != PrimitiveObjectInspector.PrimitiveCategory.INT) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    DeferredObject argument = arguments[0];
    if (argument == null) {
      return null;
    }

    if (Types.StringType.get().equals(icebergType)) {
      String val = (String) stringConverter.convert(argument.get());
      applyBucketTransform(val);

    } else if (Types.BinaryType.get().equals(icebergType)) {
      BytesWritable val = (BytesWritable) binaryConverter.convert(argument.get());
      ByteBuffer byteBuffer = ByteBuffer.wrap(val.getBytes(), 0, val.getLength());
      applyBucketTransform(byteBuffer);

    } else if (Types.IntegerType.get().equals(icebergType)) {
      IntWritable val = (IntWritable) intConverter.convert(argument.get());
      applyBucketTransform(val.get());

    } else if (Types.LongType.get().equals(icebergType)) {
      LongWritable val = (LongWritable) longConverter.convert(argument.get());
      applyBucketTransform(val.get());

    } else if (icebergType instanceof Types.DecimalType) {
      HiveDecimalWritable val = (HiveDecimalWritable) decimalConverter.convert(argument.get());
      applyBucketTransform(val.getHiveDecimal().bigDecimalValue());

    } else if (Types.FloatType.get().equals(icebergType)) {
      FloatWritable val = (FloatWritable) floatConverter.convert(argument.get());
      applyBucketTransform(val.get());

    } else if (Types.DoubleType.get().equals(icebergType)) {
      DoubleWritable val = (DoubleWritable) doubleConverter.convert(argument.get());
      applyBucketTransform(val.get());

    }

    return result;
  }

  private <T> void applyBucketTransform(T value) {
    Transform<T, Integer> transform = Transforms.bucket(icebergType, numBuckets);
    result.set(transform.apply(value));
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("iceberg_bucket", children);
  }
}
