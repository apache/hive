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

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ReturnObjectInspectorResolver;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

/**
 * GenericUDFIn
 *
 * Example usage:
 * SELECT key FROM src WHERE key IN ("238", "1");
 *
 * From MySQL page on IN(): To comply with the SQL standard, IN returns NULL
 * not only if the expression on the left hand side is NULL, but also if no
 * match is found in the list and one of the expressions in the list is NULL.
 *
 * Also noteworthy: type conversion behavior is different from MySQL. With
 * expr IN expr1, expr2... in MySQL, exprN will each be converted into the same
 * type as expr. In the Hive implementation, all expr(N) will be converted into
 * a common type for conversion consistency with other UDF's, and to prevent
 * conversions from a big type to a small type (e.g. int to tinyint)
 */
@Description(name = "in",
    value = "test _FUNC_(val1, val2...) - returns true if test equals any valN ")

public class GenericUDFIn extends GenericUDF {

  private transient ObjectInspector[] argumentOIs;
  private Set<Object> constantInSet;
  private boolean isInSetConstant = true; //are variables from IN(...) constant

  private final BooleanWritable bw = new BooleanWritable();

  private transient ReturnObjectInspectorResolver conversionHelper;
  private transient ObjectInspector compareOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException(
          "The function IN requires at least two arguments, got "
          + arguments.length);
    }
    argumentOIs = arguments;

    // We want to use the ReturnObjectInspectorResolver because otherwise
    // ObjectInspectorUtils.compare() will return != for two objects that have
    // different object inspectors, e.g. 238 and "238". The ROIR will help convert
    // both values to a common type so that they can be compared reasonably.
    conversionHelper = new GenericUDFUtils.ReturnObjectInspectorResolver(true);

    for (ObjectInspector oi : arguments) {
      if(!conversionHelper.update(oi)) {
        StringBuilder sb = new StringBuilder();
        sb.append("The arguments for IN should be the same type! Types are: {");
        sb.append(arguments[0].getTypeName());
        sb.append(" IN (");
        for(int i=1; i<arguments.length; i++) {
          if (i != 1) {
            sb.append(", ");
          }
          sb.append(arguments[i].getTypeName());
        }
        sb.append(")}");
        throw new UDFArgumentException(sb.toString());
      }
    }
    compareOI = conversionHelper.get();

    checkIfInSetConstant();

    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  private void checkIfInSetConstant(){
    for (int i = 1; i < argumentOIs.length; ++i){
      if (!(argumentOIs[i] instanceof ConstantObjectInspector)){
        isInSetConstant = false;
        return;
      }
    }
  }

  // we start at index 1, since at 0 is the variable from table column
  // (and those from IN(...) follow it)
  private void prepareInSet(DeferredObject[] arguments) throws HiveException {
    constantInSet = new HashSet<Object>();
    if (compareOI.getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
      for (int i = 1; i < arguments.length; ++i) {
        constantInSet.add(((PrimitiveObjectInspector) compareOI)
            .getPrimitiveJavaObject(conversionHelper
                .convertIfNecessary(arguments[i].get(), argumentOIs[i])));
      }
    } else {
      for (int i = 1; i < arguments.length; ++i) {
        constantInSet.add(((ConstantObjectInspector) argumentOIs[i]).getWritableConstantValue());
      }
    }
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    bw.set(false);

    if (arguments[0].get() == null) {
      return null;
    }

    if (isInSetConstant) {
      if (constantInSet == null) {
        prepareInSet(arguments);
      }
      switch (compareOI.getCategory()) {
      case PRIMITIVE: {
        if (constantInSet.contains(((PrimitiveObjectInspector) compareOI)
            .getPrimitiveJavaObject(conversionHelper.convertIfNecessary(arguments[0].get(),
                argumentOIs[0])))) {
          bw.set(true);
          return bw;
        }
        break;
      }
      case LIST: {
        if (constantInSet.contains(((ListObjectInspector) compareOI).getList(conversionHelper
            .convertIfNecessary(arguments[0].get(), argumentOIs[0])))) {
          bw.set(true);
          return bw;
        }
        break;
      }
      case MAP: {
        if (constantInSet.contains(((MapObjectInspector) compareOI).getMap(conversionHelper
            .convertIfNecessary(arguments[0].get(), argumentOIs[0])))) {
          bw.set(true);
          return bw;
        }
        break;
      }
      default:
        throw new RuntimeException("Compare of unsupported constant type: "
            + compareOI.getCategory());
      }
      if (constantInSet.contains(null)) {
        return null;
      }
    } else {
      for (int i = 1; i < arguments.length; i++) {
        if (ObjectInspectorUtils.compare(
            conversionHelper.convertIfNecessary(
                arguments[0].get(), argumentOIs[0]), compareOI,
            conversionHelper.convertIfNecessary(
                arguments[i].get(), argumentOIs[i]), compareOI) == 0) {
          bw.set(true);
          return bw;
        }
      }
      // Nothing matched. See comment at top.
      for (int i = 1; i < arguments.length; i++) {
        if (arguments[i].get() == null) {
          return null;
        }
      }
    }
    return bw;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length >= 2);
    StringBuilder sb = new StringBuilder();

    sb.append("(");
    sb.append(children[0]);
    sb.append(") ");
    sb.append("IN (");
    for(int i=1; i<children.length; i++) {
      sb.append(children[i]);
      if (i+1 != children.length) {
        sb.append(", ");
      }
    }
    sb.append(")");
    return sb.toString();
  }

}
