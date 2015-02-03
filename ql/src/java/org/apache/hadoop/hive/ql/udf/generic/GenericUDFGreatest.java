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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

/**
 * GenericUDF Class for SQL construct "greatest(v1, v2, .. vn)".
 *
 * NOTES: 1. v1, v2 and vn should have the same TypeInfo, or an exception will
 * be thrown.
 */
@Description(name = "greatest",
    value = "_FUNC_(v1, v2, ...) - Returns the greatest value in a list of values",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(2, 3, 1) FROM src LIMIT 1;\n" + "  3")
public class GenericUDFGreatest extends GenericUDF {
  private transient ObjectInspector[] argumentOIs;
  private transient GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException(getFuncName() + " requires at least 2 arguments, got "
          + arguments.length);
    }
    if (arguments[0].getCategory() != Category.PRIMITIVE) {
      throw new UDFArgumentException(getFuncName() + " only takes primitive types, got "
          + arguments[0].getTypeName());
    }

    argumentOIs = arguments;

    returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(false);
    for (int i = 0; i < arguments.length; i++) {
      if (!returnOIResolver.update(arguments[i])) {
        throw new UDFArgumentTypeException(i, "The expressions after " + getFuncName()
            + " should all have the same type: \"" + returnOIResolver.get().getTypeName()
            + "\" is expected but \"" + arguments[i].getTypeName() + "\" is found");
      }
    }
    return returnOIResolver.get();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Comparable maxV = null;
    int maxIndex = 0;
    for (int i = 0; i < arguments.length; i++) {
      Object ai = arguments[i].get();
      if (ai == null) {
        continue;
      }
      // all PRIMITIVEs are Comparable
      Comparable v = (Comparable) ai;
      if (maxV == null) {
        maxV = v;
        maxIndex = i;
        continue;
      }
      if ((isGreatest() ? 1 : -1) * v.compareTo(maxV) > 0) {
        maxV = v;
        maxIndex = i;
      }
    }
    if (maxV != null) {
      return returnOIResolver.convertIfNecessary(maxV, argumentOIs[maxIndex]);
    }
    return null;
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append(getFuncName()).append("(");
    if (children.length > 0) {
      sb.append(children[0]);
      for (int i = 1; i < children.length; i++) {
        sb.append(",");
        sb.append(children[i]);
      }
    }
    sb.append(")");
    return sb.toString();
  }

  protected String getFuncName() {
    return "greatest";
  }

  protected boolean isGreatest() {
    return true;
  }
}
