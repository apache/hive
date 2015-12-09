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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ColAndCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterColAndScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterExprAndExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterScalarAndColumn;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

/**
 * GenericUDF Class for computing and.
 */
@Description(name = "and", value = "a1 _FUNC_ a2 _FUNC_ ... _FUNC_ an - Logical and")
@VectorizedExpressions({ColAndCol.class, FilterExprAndExpr.class, FilterColAndScalar.class,
    FilterScalarAndColumn.class})
@NDV(maxNdv = 2)
public class GenericUDFOPAnd extends GenericUDF {
  private final BooleanWritable result = new BooleanWritable();
  private transient BooleanObjectInspector boi[];
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException(
          "The operator 'AND' accepts at least 2 argument.");
    }
    boi = new BooleanObjectInspector[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      boi[i] = (BooleanObjectInspector) arguments[i];
    }
    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    boolean notNull = true;
    for (int i = 0; i < arguments.length; i++) {
      Object a = arguments[i].get();
      if (a != null) {
        boolean bool_a = boi[i].get(a);
        if (bool_a == false) {
          result.set(false);
          return result;
        }
      } else {
        notNull = false;
      }
    }

    if (notNull) {
      result.set(true);
      return result;
    }

    return null;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length >= 2);
    StringBuilder sb = new StringBuilder();
    sb.append("(");
    boolean first = true;
    for (String and : children) {
      if (!first) {
        sb.append(" and ");
      } else {
        first = false;
      }
      sb.append(and);
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public GenericUDF negative() {
    return new GenericUDFOPOr();
  }
}
