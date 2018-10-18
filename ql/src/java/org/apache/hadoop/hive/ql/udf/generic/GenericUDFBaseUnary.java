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

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public abstract class GenericUDFBaseUnary extends GenericUDF {
  protected String opName;
  protected String opDisplayName;

  private transient PrimitiveObjectInspector inputOI;
  protected transient PrimitiveObjectInspector resultOI;

  protected transient Converter converter;

  protected ByteWritable byteWritable = new ByteWritable();
  protected ShortWritable shortWritable = new ShortWritable();
  protected IntWritable intWritable = new IntWritable();
  protected LongWritable longWritable = new LongWritable();
  protected FloatWritable floatWritable = new FloatWritable();
  protected DoubleWritable doubleWritable = new DoubleWritable();
  protected HiveDecimalWritable decimalWritable = new HiveDecimalWritable();
  protected HiveIntervalYearMonthWritable intervalYearMonthWritable =
      new HiveIntervalYearMonthWritable();
  protected HiveIntervalDayTimeWritable intervalDayTimeWritable =
      new HiveIntervalDayTimeWritable();

  public GenericUDFBaseUnary() {
    opName = getClass().getSimpleName();
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException(opName + " requires one argument.");
    }

    Category category = arguments[0].getCategory();
    if (category != Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "The "
          + GenericUDFUtils.getOrdinal(1)
          + " argument of " + opName + "  is expected to a "
          + Category.PRIMITIVE.toString().toLowerCase() + " type, but "
          + category.toString().toLowerCase() + " is found");
    }

    inputOI = (PrimitiveObjectInspector) arguments[0];
    if (!FunctionRegistry.isNumericType(inputOI.getTypeInfo())
        && (inputOI.getTypeInfo() != TypeInfoFactory.intervalDayTimeTypeInfo)
        && (inputOI.getTypeInfo() != TypeInfoFactory.intervalYearMonthTypeInfo)) {
      throw new UDFArgumentTypeException(0, "The "
          + GenericUDFUtils.getOrdinal(1)
          + " argument of " + opName + "  is expected to be a "
          + "numeric or interval type, but "
          + inputOI.getTypeName() + " is found");
    }

    PrimitiveTypeInfo resultTypeInfo = deriveResultTypeInfo(inputOI.getTypeInfo());
    resultOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(resultTypeInfo);
    converter = ObjectInspectorConverters.getConverter(inputOI, resultOI);
    return resultOI;
  }

  private PrimitiveTypeInfo deriveResultTypeInfo(PrimitiveTypeInfo typeInfo) {
    switch(typeInfo.getPrimitiveCategory()) {
    case STRING:
    case VARCHAR:
    case CHAR:
      return TypeInfoFactory.doubleTypeInfo;
    default:
      return typeInfo;
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    return "(" + opDisplayName + " " + children[0] + ")";
  }

}
