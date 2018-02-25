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
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

@Description(name = "nvl",
value = "_FUNC_(value,default_value) - Returns default value if value is null else returns value",
extended = "Example:\n"
+ "  > SELECT _FUNC_(null,'bla') FROM src LIMIT 1;\n" + "  bla")
public class GenericUDFNvl extends GenericUDF{
  private transient GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
  private transient ObjectInspector[] argumentOIs;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    argumentOIs = arguments;
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
          "The operator 'NVL'  accepts 2 arguments.");
    }
    returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
    if (!(returnOIResolver.update(arguments[0]) && returnOIResolver
        .update(arguments[1]))) {
      throw new UDFArgumentTypeException(1,
          "The first and seconds arguments of function NLV should have the same type, "
          + "but they are different: \"" + arguments[0].getTypeName()
          + "\" and \"" + arguments[1].getTypeName() + "\"");
    }
    return returnOIResolver.get();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object retVal = returnOIResolver.convertIfNecessary(arguments[0].get(),
        argumentOIs[0]);
    if (retVal == null ){
      retVal = returnOIResolver.convertIfNecessary(arguments[1].get(),
          argumentOIs[1]);
    }
    return retVal;
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append("NVL(");
    sb.append(children[0]);
    sb.append(',');
    sb.append(children[1]);
    sb.append(')');
    return sb.toString() ;
  }

}
