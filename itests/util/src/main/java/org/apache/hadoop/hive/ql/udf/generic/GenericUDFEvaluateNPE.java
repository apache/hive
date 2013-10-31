/**
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

import java.lang.NullPointerException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * GenericUDFEvaluateNPE
 * This UDF is to throw an Null Pointer Exception
 * It is used to test hive failure handling
 *
 */
@Description(name = "evaluate_npe", value = "_FUNC_(string) - "
  + "Throws an NPE in the GenericUDF.evaluate() method. "
  + "Used for testing GenericUDF error handling.")
public class GenericUDFEvaluateNPE extends GenericUDF {
  private ObjectInspector[] argumentOIs;
  private final Text result= new Text();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException(
          "The function evaluate_npe(string)"
            + "needs only one argument.");
    }

    if (!arguments[0].getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
      throw new UDFArgumentTypeException(0,
        "Argument 1 of function evaluate_npe must be \""
        + serdeConstants.STRING_TYPE_NAME + "but \""
        + arguments[0].getTypeName() + "\" was found.");
    }

    argumentOIs = arguments;
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (true) {
        throw new NullPointerException("evaluate null pointer exception");
    }
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    return "evaluate_npe(" + children[0] + ")";
  }
}
