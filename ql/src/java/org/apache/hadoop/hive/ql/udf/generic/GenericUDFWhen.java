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
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;

/**
 * GenericUDF Class for SQL construct
 * "CASE WHEN a THEN b WHEN c THEN d [ELSE f] END".
 * 
 * NOTES: 1. a and c should be boolean, or an exception will be thrown. 2. b, d
 * and f should be common types, or an exception will be thrown.
 */
public class GenericUDFWhen extends GenericUDF {
  private transient ObjectInspector[] argumentOIs;
  private transient GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentTypeException {

    argumentOIs = arguments;
    returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);

    for (int i = 0; i + 1 < arguments.length; i += 2) {
      if (!arguments[i].getTypeName().equals(serdeConstants.BOOLEAN_TYPE_NAME)) {
        throw new UDFArgumentTypeException(i, "\""
            + serdeConstants.BOOLEAN_TYPE_NAME + "\" is expected after WHEN, "
            + "but \"" + arguments[i].getTypeName() + "\" is found");
      }
      if (!returnOIResolver.update(arguments[i + 1])) {
        throw new UDFArgumentTypeException(i + 1,
            "The expressions after THEN should have the same type: \""
            + returnOIResolver.get().getTypeName()
            + "\" is expected but \"" + arguments[i + 1].getTypeName()
            + "\" is found");
      }
    }
    if (arguments.length % 2 == 1) {
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
    for (int i = 0; i + 1 < arguments.length; i += 2) {
      Object caseKey = arguments[i].get();
      if (caseKey != null
          && ((BooleanObjectInspector) argumentOIs[i]).get(caseKey)) {
        Object caseValue = arguments[i + 1].get();
        return returnOIResolver.convertIfNecessary(caseValue,
            argumentOIs[i + 1]);
      }
    }
    // Process else statement
    if (arguments.length % 2 == 1) {
      int i = arguments.length - 2;
      Object elseValue = arguments[i + 1].get();
      return returnOIResolver.convertIfNecessary(elseValue, argumentOIs[i + 1]);
    }
    return null;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length >= 2);
    StringBuilder sb = new StringBuilder();
    sb.append("CASE");
    for (int i = 0; i + 1 < children.length; i += 2) {
      sb.append(" WHEN (");
      sb.append(children[i]);
      sb.append(") THEN (");
      sb.append(children[i + 1]);
      sb.append(")");
    }
    if (children.length % 2 == 1) {
      sb.append(" ELSE (");
      sb.append(children[children.length - 1]);
      sb.append(")");
    }
    sb.append(" END");
    return sb.toString();
  }

}
