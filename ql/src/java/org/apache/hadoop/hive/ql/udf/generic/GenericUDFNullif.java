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
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;

/**
 * GenericUDF Class for SQL construct "nullif(a,b)".
 */
@Description(
    name = "nullif",
    value = "_FUNC_(a1, a2) - shorthand for: case when a1 = a2 then null else a1",
    extended = "Example:\n "
        + "SELECT _FUNC_(1,1),_FUNC_(1,2)")

public class GenericUDFNullif extends GenericUDF {
  private transient ObjectInspector[] argumentOIs;
  private transient GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    argumentOIs = arguments;
    checkArgsSize(arguments, 2, 2);

    returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
    returnOIResolver.update(arguments[0]);

    boolean isPrimitive = (arguments[0] instanceof PrimitiveObjectInspector);
    if (isPrimitive)
    {
      PrimitiveObjectInspector primitive0 = (PrimitiveObjectInspector) arguments[0];
      PrimitiveObjectInspector primitive1 = (PrimitiveObjectInspector) arguments[1];
      PrimitiveGrouping pcat0 =
          PrimitiveObjectInspectorUtils.getPrimitiveGrouping(primitive0.getPrimitiveCategory());
      PrimitiveGrouping pcat1 =
          PrimitiveObjectInspectorUtils.getPrimitiveGrouping(primitive1.getPrimitiveCategory());

      if (pcat0 == PrimitiveGrouping.VOID_GROUP) {
        throw new UDFArgumentTypeException(0,
            "NULLIF may not accept types belonging to " + pcat0 + " as first argument");
      }

      if (pcat1 != PrimitiveGrouping.VOID_GROUP && pcat0 != pcat1) {
        throw new UDFArgumentTypeException(1,
            "The expressions after NULLIF should belong to the same category: \"" + pcat0
                + "\" is expected but \"" + pcat1 + "\" is found");
      }
    } else {
      String typeName0 = arguments[0].getTypeName();
      String typeName1 = arguments[1].getTypeName();
      if (!typeName0.equals(typeName1)) {
        throw new UDFArgumentTypeException(1,
            "The expressions after NULLIF should all have the same type: \"" + typeName0
                + "\" is expected but \"" + typeName1 + "\" is found");
      }
    }

    return returnOIResolver.get();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object arg0 = arguments[0].get();
    Object arg1 = arguments[1].get();
    Object value0 = null;
    if (arg0 != null) {
      value0 = returnOIResolver.convertIfNecessary(arg0, argumentOIs[0], false);
    }
    if (arg0 == null || arg1 == null) {
      return value0;
    }
    PrimitiveObjectInspector compareOI = (PrimitiveObjectInspector) returnOIResolver.get();
    if (PrimitiveObjectInspectorUtils.comparePrimitiveObjects(
        value0, compareOI,
        returnOIResolver.convertIfNecessary(arg1, argumentOIs[1], false), compareOI)) {
      return null;
    }
    return value0;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("NULLIF", children, ",");
  }

}
