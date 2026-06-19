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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

/**
 * GenericUDFSQCountCheck.
 *
 */
@UDFType(deterministic = false)
@Description(name = "sq_count_check",
    value = "_FUNC_(x) - Internal check on scalar subquery expression to make sure atmost one row is returned",
    extended = "For internal use only")
public class GenericUDFSQCountCheck extends GenericUDF {
  private final BooleanWritable resultBool = new BooleanWritable();
  private transient Converter[] converters = new Converter[1];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length > 2) {
      throw new UDFArgumentLengthException(
          "Invalid scalar subquery expression. Subquery count check expected two argument but received: " + arguments.length);
    }

    checkArgPrimitive(arguments, 0);
    converters[0] = ObjectInspectorConverters.getConverter(arguments[0],
            PrimitiveObjectInspectorFactory.writableLongObjectInspector);

    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    switch (arguments.length){
      case 1: //Scalar queries, should expect value/count less than 1
        if (getLongValue(arguments, 0, converters) > 1) {
          throw new UDFArgumentException(
                  " Scalar subquery expression returns more than one row.");
        }
        break;
      case 2:
        Object valObject = arguments[0].get();
        if( valObject != null
                && getLongValue(arguments, 0, converters) == 0){
          throw new UDFArgumentException(
                  " IN/NOT IN subquery with aggregate returning zero result. Currently this is not supported.");
        }
        else if(valObject == null) {
          throw new UDFArgumentException(
                  " IN/NOT IN subquery with aggregate returning zero result. Currently this is not supported.");
        }
        break;
    }

    resultBool.set(true);
    return resultBool;
  }

  @Override
  protected String getFuncName() {
    return "sq_count_check";
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

}
 