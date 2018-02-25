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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * A simple generic udf to call java functions via reflection.
 */
@Description(name = "reflect2",
  value = "_FUNC_(arg0,method[,arg1[,arg2..]]) calls method of arg0 with reflection",
  extended = "Use this UDF to call Java methods by matching the argument signature\n")
@UDFType(deterministic = true)
public class GenericUDFReflect2 extends AbstractGenericUDFReflect {

  private PrimitiveObjectInspector targetOI;
  private PrimitiveObjectInspector returnOI;
  private transient Method method;

  private transient Writable returnObj;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException(
          "The function GenericUDFReflect2(arg0,method[,arg1[,arg2]...])"
          + " accepts 2 or more arguments.");
    }
    if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(1, "The target instance should be a primitive type.");
    }
    targetOI = (PrimitiveObjectInspector) arguments[0];

    if (!(arguments[1] instanceof StringObjectInspector)) {
      throw new UDFArgumentTypeException(1, "The method name should be string type.");
    }
    if (!(arguments[1] instanceof ConstantObjectInspector)) {
      throw new UDFArgumentTypeException(1, "The method name should be a constant.");
    }
    Text methodName = (Text) ((ConstantObjectInspector)arguments[1]).getWritableConstantValue();
    if (methodName.toString().equals("hashCode") && arguments.length == 2) {
      // it's non-deterministic
      throw new UDFArgumentTypeException(1, "Use hash() UDF instead of this.");
    }
    setupParameterOIs(arguments, 2);

    Class<?> targetClass = PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveCategory(
        targetOI.getPrimitiveCategory()).primitiveJavaClass;

    try {
      method = findMethod(targetClass, methodName.toString(), null, true);
      // Note: type param is not available here.
      PrimitiveTypeEntry typeEntry = getTypeFor(method.getReturnType());
      returnOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          typeEntry.primitiveCategory);
      returnObj = (Writable) returnOI.getPrimitiveWritableClass().newInstance();
    } catch (Exception e) {
      throw new UDFArgumentException(e);
    }
    return returnOI;
  }

  private PrimitiveObjectInspectorUtils.PrimitiveTypeEntry getTypeFor(Class<?> retType)
      throws UDFArgumentException {
    PrimitiveObjectInspectorUtils.PrimitiveTypeEntry entry =
        PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveJavaType(retType);
    if (entry == null) {
      entry = PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveJavaClass(retType);
    }
    if (entry == null) {
      throw new UDFArgumentException("Invalid return type " + retType);
    }
    return entry;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object targetObject = targetOI.getPrimitiveJavaObject(arguments[0].get());
    if (targetObject == null) {
      return null;
    }
    Object result = null;
    try {
      result = method.invoke(targetObject, setupParameters(arguments, 2));
    } catch (InvocationTargetException e) {
      throw new HiveException(e.getCause());
    } catch (Exception e) {
      throw new HiveException(e);
    }
    if (result == null) {
      return null;
    }
    switch (returnOI.getPrimitiveCategory()) {
      case VOID:
        return null;
      case BOOLEAN:
        ((BooleanWritable)returnObj).set((Boolean)result);
        return returnObj;
      case BYTE:
        ((ByteWritable)returnObj).set((Byte)result);
        return returnObj;
      case SHORT:
        ((ShortWritable)returnObj).set((Short)result);
        return returnObj;
      case INT:
        ((IntWritable)returnObj).set((Integer)result);
        return returnObj;
      case LONG:
        ((LongWritable)returnObj).set((Long)result);
        return returnObj;
      case FLOAT:
        ((FloatWritable)returnObj).set((Float)result);
        return returnObj;
      case DOUBLE:
        ((DoubleWritable)returnObj).set((Double)result);
        return returnObj;
      case STRING:
        ((Text)returnObj).set((String)result);
        return returnObj;
      case TIMESTAMP:
        ((TimestampWritable)returnObj).set((Timestamp)result);
        return returnObj;
      case BINARY:
        ((BytesWritable)returnObj).set((byte[])result, 0, ((byte[]) result).length);
        return returnObj;
      case DECIMAL:
        ((HiveDecimalWritable)returnObj).set((HiveDecimal)result);
        return returnObj;
    }
    throw new HiveException("Invalid type " + returnOI.getPrimitiveCategory());
  }

  @Override
  protected String functionName() {
    return "reflect2";
  }
}
