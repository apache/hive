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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressionsSupportDecimal64;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprDecimalColumnColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprDecimalColumnScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprDecimalScalarColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprDecimalScalarScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprLongColumnLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprDoubleColumnDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprDoubleColumnLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprLongColumnDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprLongScalarLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprDoubleScalarDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprDoubleScalarLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprLongScalarDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprLongScalarLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprDoubleScalarDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprDoubleScalarLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprLongScalarDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprDoubleColumnDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprIntervalDayTimeColumnColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprIntervalDayTimeColumnScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprIntervalDayTimeScalarColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprIntervalDayTimeScalarScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprDecimal64ColumnDecimal64Column;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprDecimal64ColumnDecimal64Scalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprDecimal64ScalarDecimal64Column;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprDecimal64ScalarDecimal64Scalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprLongColumnLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringGroupColumnStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringGroupColumnStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringGroupColumnCharScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringGroupColumnVarCharScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringScalarStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprCharScalarStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprTimestampColumnColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprTimestampColumnScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprTimestampScalarColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprTimestampScalarScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprVarCharScalarStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringScalarStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringScalarCharScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringScalarVarCharScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprCharScalarStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprVarCharScalarStringScalar;



/**
 * IF(expr1,expr2,expr3) <br>
 * If expr1 is TRUE (expr1 <> 0 and expr1 <> NULL) then IF() returns expr2;
 * otherwise it returns expr3. IF() returns a numeric or string value, depending
 * on the context in which it is used.
 */
@Description(
    name = "if",
    value = "IF(expr1,expr2,expr3) - If expr1 is TRUE (expr1 <> 0 and expr1 <> NULL) then"
    + " IF() returns expr2; otherwise it returns expr3. IF() returns a numeric or string value,"
    + " depending on the context in which it is used.")
@VectorizedExpressions({
  IfExprLongColumnLongColumn.class, IfExprDoubleColumnDoubleColumn.class,
  IfExprLongColumnLongScalar.class, IfExprDoubleColumnDoubleScalar.class,
  IfExprLongColumnDoubleScalar.class, IfExprDoubleColumnLongScalar.class,
  IfExprLongScalarLongColumn.class, IfExprDoubleScalarDoubleColumn.class,
  IfExprLongScalarDoubleColumn.class, IfExprDoubleScalarLongColumn.class,
  IfExprLongScalarLongScalar.class, IfExprDoubleScalarDoubleScalar.class,
  IfExprLongScalarDoubleScalar.class, IfExprDoubleScalarLongScalar.class,

  IfExprDecimal64ColumnDecimal64Column.class, IfExprDecimal64ColumnDecimal64Scalar.class,
  IfExprDecimal64ScalarDecimal64Column.class, IfExprDecimal64ScalarDecimal64Scalar.class,

  IfExprStringGroupColumnStringGroupColumn.class,
  IfExprStringGroupColumnStringScalar.class,
  IfExprStringGroupColumnCharScalar.class, IfExprStringGroupColumnVarCharScalar.class,
  IfExprStringScalarStringGroupColumn.class,
  IfExprCharScalarStringGroupColumn.class, IfExprVarCharScalarStringGroupColumn.class,
  IfExprStringScalarStringScalar.class,
  IfExprStringScalarCharScalar.class, IfExprStringScalarVarCharScalar.class,
  IfExprCharScalarStringScalar.class, IfExprVarCharScalarStringScalar.class,

  IfExprDecimalColumnColumn.class, IfExprDecimalColumnScalar.class,
  IfExprDecimalScalarColumn.class, IfExprDecimalScalarScalar.class,

  IfExprIntervalDayTimeColumnColumn.class, IfExprIntervalDayTimeColumnScalar.class,
  IfExprIntervalDayTimeScalarColumn.class, IfExprIntervalDayTimeScalarScalar.class,
  IfExprTimestampColumnColumn.class, IfExprTimestampColumnScalar.class,
  IfExprTimestampScalarColumn.class, IfExprTimestampScalarScalar.class,
})
@VectorizedExpressionsSupportDecimal64()
public class GenericUDFIf extends GenericUDF {
  private transient ObjectInspector[] argumentOIs;
  private transient GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    argumentOIs = arguments;
    returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);

    if (arguments.length != 3) {
      throw new UDFArgumentLengthException(
          "The function IF(expr1,expr2,expr3) accepts exactly 3 arguments.");
    }

    boolean conditionTypeIsOk = (arguments[0].getCategory() == ObjectInspector.Category.PRIMITIVE);
    if (conditionTypeIsOk) {
      PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) arguments[0]);
      conditionTypeIsOk = (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN
          || poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.VOID);
    }
    if (!conditionTypeIsOk) {
      throw new UDFArgumentTypeException(0,
          "The first argument of function IF should be \""
          + serdeConstants.BOOLEAN_TYPE_NAME + "\", but \""
          + arguments[0].getTypeName() + "\" is found");
    }

    if (!(returnOIResolver.update(arguments[1]) && returnOIResolver
        .update(arguments[2]))) {
      throw new UDFArgumentTypeException(2,
          "The second and the third arguments of function IF should have the same type, "
          + "but they are different: \"" + arguments[1].getTypeName()
          + "\" and \"" + arguments[2].getTypeName() + "\"");
    }

    return returnOIResolver.get();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object condition = arguments[0].get();
    if (condition != null
        && ((BooleanObjectInspector) argumentOIs[0]).get(condition)) {
      return returnOIResolver.convertIfNecessary(arguments[1].get(),
          argumentOIs[1]);
    } else {
      return returnOIResolver.convertIfNecessary(arguments[2].get(),
          argumentOIs[2]);
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 3);
    return getStandardDisplayString("if", children);
  }
}
