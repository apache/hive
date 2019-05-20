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
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

/**
 * GenericUDF Class for SQL construct "CASE a WHEN b THEN c [ELSE f] END".
 * 
 * NOTES: 1. a and b should be compatible, or an exception will be
 * thrown. 2. c and f should be compatible types, or an exception will be
 * thrown.
 */
@Description(
    name = "case",
    value = "CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END - "
        + "When a = b, returns c; when a = d, return e; else return f",
    extended = "Example:\n "
    + "SELECT\n"
    + " CASE deptno\n"
    + "   WHEN 1 THEN Engineering\n"
    + "   WHEN 2 THEN Finance\n"
    + "   ELSE admin\n"
    + " END,\n"
    + " CASE zone\n"
    + "   WHEN 7 THEN Americas\n"
    + "   ELSE Asia-Pac\n"
    + " END\n"
    + " FROM emp_details")

public class GenericUDFCase extends GenericUDF {
  private transient ObjectInspector[] argumentOIs;
  private transient GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
  private transient GenericUDFUtils.ReturnObjectInspectorResolver caseOIResolver;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentTypeException {

    argumentOIs = arguments;
    caseOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
    returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);

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
      // May need to convert to common type to compare
      PrimitiveObjectInspector caseOI = (PrimitiveObjectInspector) caseOIResolver.get();
      if (PrimitiveObjectInspectorUtils.comparePrimitiveObjects(
            caseOIResolver.convertIfNecessary(exprValue, argumentOIs[0]), caseOI,
            caseOIResolver.convertIfNecessary(caseKey, argumentOIs[i], false), caseOI)) {
        Object caseValue = arguments[i + 1].get();
        return returnOIResolver.convertIfNecessary(caseValue, argumentOIs[i + 1]);
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
