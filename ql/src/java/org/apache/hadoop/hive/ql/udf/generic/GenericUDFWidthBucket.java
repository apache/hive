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

import com.google.common.base.Preconditions;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.VOID_GROUP;


@Description(name = "width_bucket",
        value = "_FUNC_(expr, min_value, max_value, num_buckets) - Returns an integer between 0 and num_buckets+1 by "
                + "mapping the expr into buckets defined by the range [min_value, max_value]",
        extended = "Returns an integer between 0 and num_buckets+1 by "
                + "mapping expr into the ith equally sized bucket. Buckets are made by dividing [min_value, max_value] into "
                + "equally sized regions. If expr < min_value, return 1, if expr > max_value return num_buckets+1\n"
                + "Example: expr is an integer column withs values 1, 10, 20, 30.\n"
                + "  > SELECT _FUNC_(expr, 5, 25, 4) FROM src;\n1\n1\n3\n5")
public class GenericUDFWidthBucket extends GenericUDF {

  private transient ObjectInspector[] objectInspectors;
  private transient ObjectInspector commonExprMinMaxOI;
  private transient ObjectInspectorConverters.Converter epxrConverterOI;
  private transient ObjectInspectorConverters.Converter minValueConverterOI;
  private transient ObjectInspectorConverters.Converter maxValueConverterOI;

  private final IntWritable output = new IntWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    this.objectInspectors = arguments;

    checkArgsSize(arguments, 4, 4);

    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);
    checkArgPrimitive(arguments, 2);
    checkArgPrimitive(arguments, 3);

    PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[4];
    checkArgGroups(arguments, 0, inputTypes, NUMERIC_GROUP, VOID_GROUP);
    checkArgGroups(arguments, 1, inputTypes, NUMERIC_GROUP, VOID_GROUP);
    checkArgGroups(arguments, 2, inputTypes, NUMERIC_GROUP, VOID_GROUP);
    checkArgGroups(arguments, 3, inputTypes, NUMERIC_GROUP, VOID_GROUP);

    TypeInfo exprTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(this.objectInspectors[0]);
    TypeInfo minValueTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(this.objectInspectors[1]);
    TypeInfo maxValueTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(this.objectInspectors[2]);

    TypeInfo commonExprMinMaxTypeInfo = FunctionRegistry.getCommonClassForComparison(exprTypeInfo,
            FunctionRegistry.getCommonClassForComparison(minValueTypeInfo, maxValueTypeInfo));

    this.commonExprMinMaxOI = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(commonExprMinMaxTypeInfo);

    this.epxrConverterOI = ObjectInspectorConverters.getConverter(this.objectInspectors[0], this.commonExprMinMaxOI);
    this.minValueConverterOI = ObjectInspectorConverters.getConverter(this.objectInspectors[1], this.commonExprMinMaxOI);
    this.maxValueConverterOI = ObjectInspectorConverters.getConverter(this.objectInspectors[2], this.commonExprMinMaxOI);

    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null || arguments[1].get() == null || arguments[2].get() == null || arguments[3].get() == null) {
      return null;
    }

    Object exprValue = this.epxrConverterOI.convert(arguments[0].get());
    Object minValue = this.minValueConverterOI.convert(arguments[1].get());
    Object maxValue = this.maxValueConverterOI.convert(arguments[2].get());

    int numBuckets = PrimitiveObjectInspectorUtils.getInt(arguments[3].get(),
            (PrimitiveObjectInspector) this.objectInspectors[3]);

    switch (((PrimitiveObjectInspector) this.commonExprMinMaxOI).getPrimitiveCategory()) {
      case SHORT:
        return evaluate(((ShortWritable) exprValue).get(), ((ShortWritable) minValue).get(),
                ((ShortWritable) maxValue).get(), numBuckets);
      case INT:
        return evaluate(((IntWritable) exprValue).get(), ((IntWritable) minValue).get(),
                ((IntWritable) maxValue).get(), numBuckets);
      case LONG:
        return evaluate(((LongWritable) exprValue).get(), ((LongWritable) minValue).get(),
                ((LongWritable) maxValue).get(), numBuckets);
      case FLOAT:
        return evaluate(((FloatWritable) exprValue).get(), ((FloatWritable) minValue).get(),
                ((FloatWritable) maxValue).get(), numBuckets);
      case DOUBLE:
        return evaluate(((DoubleWritable) exprValue).get(), ((DoubleWritable) minValue).get(),
                ((DoubleWritable) maxValue).get(), numBuckets);
      case DECIMAL:
        return evaluate(((HiveDecimalWritable) exprValue).getHiveDecimal(),
                ((HiveDecimalWritable) minValue).getHiveDecimal(), ((HiveDecimalWritable) maxValue).getHiveDecimal(),
                numBuckets);
      case BYTE:
        return evaluate(((ByteWritable) exprValue).get(), ((ByteWritable) minValue).get(),
                ((ByteWritable) maxValue).get(), numBuckets);
      default:
        throw new IllegalStateException(
                "Error: width_bucket could not determine a common primitive type for all inputs");
    }
  }

  private IntWritable evaluate(short exprValue, short minValue, short maxValue, int numBuckets) {

    Preconditions.checkArgument(numBuckets > 0, "numBuckets in width_bucket function must be above 0");
    Preconditions.checkArgument(maxValue != minValue, "maxValue cannot be equal to minValue in width_bucket function");

    if (maxValue > minValue) {
      if (exprValue < minValue) {
        output.set(0);
      } else if (exprValue >= maxValue) {
        output.set(numBuckets + 1);
      } else {
        output.set((int) Math.floor((numBuckets * (exprValue - minValue) / (maxValue - minValue)) + 1));
      }
    } else {
      if (exprValue > minValue) {
        output.set(0);
      } else if (exprValue <= maxValue) {
        output.set(numBuckets + 1);
      } else {
        output.set((int) Math.floor((numBuckets * (minValue - exprValue) / (minValue - maxValue)) + 1));
      }
    }

    return output;
  }

  private IntWritable evaluate(int exprValue, int minValue, int maxValue, int numBuckets) {

    Preconditions.checkArgument(numBuckets > 0, "numBuckets in width_bucket function must be above 0");
    Preconditions.checkArgument(maxValue != minValue, "maxValue cannot be equal to minValue in width_bucket function");

    if (maxValue > minValue) {
      if (exprValue < minValue) {
        output.set(0);
      } else if (exprValue >= maxValue) {
        output.set(numBuckets + 1);
      } else {
        output.set((int) Math.floor((numBuckets * (exprValue - minValue) / (maxValue - minValue)) + 1));
      }
    } else {
      if (exprValue > minValue) {
        output.set(0);
      } else if (exprValue <= maxValue) {
        output.set(numBuckets + 1);
      } else {
        output.set((int) Math.floor((numBuckets * (minValue - exprValue) / (minValue - maxValue)) + 1));
      }
    }

    return output;
  }

  private IntWritable evaluate(long exprValue, long minValue, long maxValue, int numBuckets) {

    Preconditions.checkArgument(numBuckets > 0, "numBuckets in width_bucket function must be above 0");
    Preconditions.checkArgument(maxValue != minValue, "maxValue cannot be equal to minValue in width_bucket function");

    if (maxValue > minValue) {
      if (exprValue < minValue) {
        output.set(0);
      } else if (exprValue >= maxValue) {
        output.set(numBuckets + 1);
      } else {
        output.set((int) Math.floor((numBuckets * (exprValue - minValue) / (maxValue - minValue)) + 1));
      }
    } else {
      if (exprValue > minValue) {
        output.set(0);
      } else if (exprValue <= maxValue) {
        output.set(numBuckets + 1);
      } else {
        output.set((int) Math.floor((numBuckets * (minValue - exprValue) / (minValue - maxValue)) + 1));
      }
    }

    return output;
  }

  private IntWritable evaluate(float exprValue, float minValue, float maxValue, int numBuckets) {

    Preconditions.checkArgument(numBuckets > 0, "numBuckets in width_bucket function must be above 0");
    Preconditions.checkArgument(maxValue != minValue, "maxValue cannot be equal to minValue in width_bucket function");

    if (maxValue > minValue) {
      if (exprValue < minValue) {
        output.set(0);
      } else if (exprValue >= maxValue) {
        output.set(numBuckets + 1);
      } else {
        output.set((int) Math.floor((numBuckets * (exprValue - minValue) / (maxValue - minValue)) + 1));
      }
    } else {
      if (exprValue > minValue) {
        output.set(0);
      } else if (exprValue <= maxValue) {
        output.set(numBuckets + 1);
      } else {
        output.set((int) Math.floor((numBuckets * (minValue - exprValue) / (minValue - maxValue)) + 1));
      }
    }

    return output;
  }

  private IntWritable evaluate(double exprValue, double minValue, double maxValue, int numBuckets) {

    Preconditions.checkArgument(numBuckets > 0, "numBuckets in width_bucket function must be above 0");
    Preconditions.checkArgument(maxValue != minValue, "maxValue cannot be equal to minValue in width_bucket function");

    if (maxValue > minValue) {
      if (exprValue < minValue) {
        output.set(0);
      } else if (exprValue >= maxValue) {
        output.set(numBuckets + 1);
      } else {
        output.set((int) Math.floor((numBuckets * (exprValue - minValue) / (maxValue - minValue)) + 1));
      }
    } else {
      if (exprValue > minValue) {
        output.set(0);
      } else if (exprValue <= maxValue) {
        output.set(numBuckets + 1);
      } else {
        output.set((int) Math.floor((numBuckets * (minValue - exprValue) / (minValue - maxValue)) + 1));
      }
    }

    return output;
  }

  private IntWritable evaluate(HiveDecimal exprValue, HiveDecimal minValue, HiveDecimal maxValue,
                                      int numBuckets) {

    Preconditions.checkArgument(numBuckets > 0, "numBuckets in width_bucket function must be above 0");
    Preconditions.checkArgument(!maxValue.equals(minValue),
            "maxValue cannot be equal to minValue in width_bucket function");

    if (maxValue.compareTo(minValue) > 0) {
      if (exprValue.compareTo(minValue) < 0) {
        output.set(0);
      } else if (exprValue.compareTo(maxValue) >= 0) {
        output.set(numBuckets + 1);
      } else {
        output.set(HiveDecimal.create(numBuckets).multiply(exprValue.subtract(minValue)).divide(
                maxValue.subtract(minValue)).add(HiveDecimal.ONE).intValue());
      }
    } else {
      if (exprValue.compareTo(minValue) > 0) {
        output.set(0);
      } else if (exprValue.compareTo(maxValue) <= 0) {
        output.set(numBuckets + 1);
      } else {
        output.set(HiveDecimal.create(numBuckets).multiply(minValue.subtract(exprValue)).divide(
                minValue.subtract(maxValue)).add(HiveDecimal.ONE).intValue());
      }
    }

    return output;
  }

   private Object evaluate(byte exprValue, byte minValue, byte maxValue, int numBuckets) {
         Preconditions.checkArgument(numBuckets > 0, "numBuckets in width_bucket function must be above 0");
    Preconditions.checkArgument(maxValue != minValue, "maxValue cannot be equal to minValue in width_bucket function");

    if (maxValue > minValue) {
      if (exprValue < minValue) {
        output.set(0);
      } else if (exprValue >= maxValue) {
        output.set(numBuckets + 1);
      } else {
        output.set((int) Math.floor((numBuckets * (exprValue - minValue) / (maxValue - minValue)) + 1));
      }
    } else {
      if (exprValue > minValue) {
        output.set(0);
      } else if (exprValue <= maxValue) {
        output.set(numBuckets + 1);
      } else {
        output.set((int) Math.floor((numBuckets * (minValue - exprValue) / (minValue - maxValue)) + 1));
      }
    }

    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("width_bucket", children);
  }
}
