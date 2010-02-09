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

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

/**
 * GenericUDF Class for SQL construct
 * "CASE WHEN a THEN b WHEN c THEN d [ELSE f] END".
 * 
 * NOTES: 1. a and c should be boolean, or an exception will be thrown. 2. b, d
 * and f should have the same TypeInfo, or an exception will be thrown.
 */
public class GenericUDFCase extends GenericUDF {
  private ObjectInspector[] argumentOIs;
  private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
  private GenericUDFUtils.ReturnObjectInspectorResolver caseOIResolver;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentTypeException {

    argumentOIs = arguments;
    caseOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver();
    returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver();

    boolean r = caseOIResolver.update(arguments[0]);
    assert (r);
    for (int i = 1; i + 1 < arguments.length; i += 2) {
      if (!caseOIResolver.update(arguments[i])) {
        throw new UDFArgumentTypeException(i,
            "The expressions after WHEN should have the same type with that after CASE: \""
            + caseOIResolver.get().getTypeName() + "\" is expected but \""
            + arguments[i].getTypeName() + "\" is found");
      }
      if (!returnOIResolver.update(arguments[i + 1])) {
        throw new UDFArgumentTypeException(i + 1,
            "The expressions after THEN should have the same type: \""
            + returnOIResolver.get().getTypeName()
            + "\" is expected but \"" + arguments[i + 1].getTypeName()
            + "\" is found");
      }
    }
    if (arguments.length % 2 == 0) {
      int i = arguments.length - 2;
      if (!returnOIResolver.update(arguments[i + 1])) {
        throw new UDFArgumentTypeException(i + 1,
            "The expression after ELSE should have the same type as those after THEN: \""
            + returnOIResolver.get().getTypeName()
            + "\" is expected but \"" + arguments[i + 1].getTypeName()
            + "\" is found");
      }
    }

    return returnOIResolver.get();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object exprValue = arguments[0].get();
    for (int i = 1; i + 1 < arguments.length; i += 2) {
      Object caseKey = arguments[i].get();
      if (PrimitiveObjectInspectorUtils.comparePrimitiveObjects(exprValue,
          (PrimitiveObjectInspector) argumentOIs[0], caseKey,
          (PrimitiveObjectInspector) argumentOIs[i])) {
        Object caseValue = arguments[i + 1].get();
        return returnOIResolver.convertIfNecessary(caseValue,
            argumentOIs[i + 1]);
      }
    }
    // Process else statement
    if (arguments.length % 2 == 0) {
      int i = arguments.length - 2;
      Object elseValue = arguments[i + 1].get();
      return returnOIResolver.convertIfNecessary(elseValue, argumentOIs[i + 1]);
    }
    return null;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length >= 3);
    StringBuilder sb = new StringBuilder();
    sb.append("CASE (");
    sb.append(children[0]);
    sb.append(")");
    for (int i = 1; i + 1 < children.length; i += 2) {
      sb.append(" WHEN (");
      sb.append(children[i]);
      sb.append(") THEN (");
      sb.append(children[i + 1]);
      sb.append(")");
    }
    if (children.length % 2 == 0) {
      sb.append(" ELSE (");
      sb.append(children[children.length - 1]);
      sb.append(")");
    }
    sb.append(" END");
    return sb.toString();
  }

}
