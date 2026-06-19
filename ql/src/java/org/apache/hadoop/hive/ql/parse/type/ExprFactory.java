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
package org.apache.hadoop.hive.ql.parse.type;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.SubqueryType;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hive.common.util.DateUtils;

/**
 * Generic expressions factory.
 */
public abstract class ExprFactory<T> {

  static final BigDecimal NANOS_PER_SEC_BD =
      new BigDecimal(DateUtils.NANOS_PER_SEC);

  /**
   * Returns whether the input is an instance of the expression class.
   */
  protected abstract boolean isExprInstance(Object o);

  /**
   * Generates an expression from the input column. This may not necessarily
   * be a column expression, e.g., if the column is a constant.
   *
   * For position based expression factories (e.g., Calcite), an offset that
   * will be added to the input references position can be provided. For
   * instance, this is useful to generate expressions for right side of a
   * join output.
   */
  protected abstract T toExpr(ColumnInfo colInfo, RowResolver rowResolver, int offset)
      throws SemanticException;

  /* FIELD REFERENCES */
  /**
   * Returns whether the input object is a column reference expression.
   */
  protected abstract boolean isColumnRefExpr(Object o);

  /**
   * Creates column expression.
   *
   * For position based expression factories (e.g., Calcite), an offset that
   * will be added to the input references position can be provided. For
   * instance, this is useful to generate expressions for right side of a
   * join output.
   */
  protected abstract T createColumnRefExpr(ColumnInfo colInfo, RowResolver rowResolver, int offset)
      throws SemanticException;

  /**
   * Creates column expression.
   */
  protected abstract T createColumnRefExpr(ColumnInfo colInfo, List<RowResolver> rowResolverList)
      throws SemanticException;

  /**
   * Returns column name referenced by a column expression.
   */
  protected abstract String getColumnName(T expr, RowResolver rowResolver);

  /* CONSTANT EXPRESSIONS */
  /**
   * Returns whether the input expression is a constant expression.
   */
  protected abstract boolean isConstantExpr(Object o);

  /**
   * Returns whether all input expressions are constant expressions.
   */
  protected boolean isAllConstants(List<T> exprs) {
    for (T expr : exprs) {
      if (!isConstantExpr(expr)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns whether the input expression is a struct of
   * constant expressions (all of them).
   */
  protected abstract boolean isConstantStruct(T expr);

  /**
   * Creates a null constant expression with void type.
   */
  protected abstract T createNullConstantExpr();

  /**
   * Creates a dynamic parameter expression with void type.
   */
  protected abstract T createDynamicParamExpr(int index);

  /**
   * Creates a boolean constant expression from input value.
   */
  protected abstract T createBooleanConstantExpr(String value);

  /**
   * Creates a bigint constant expression from input value.
   */
  protected abstract T createBigintConstantExpr(String value);

  /**
   * Creates a int constant expression from input value.
   */
  protected abstract T createIntConstantExpr(String value);

  /**
   * Creates a smallint constant expression from input value.
   */
  protected abstract T createSmallintConstantExpr(String value);

  /**
   * Creates a tinyint constant expression from input value.
   */
  protected abstract T createTinyintConstantExpr(String value);

  /**
   * Creates a float constant expression from input value.
   */
  protected abstract T createFloatConstantExpr(String value);

  /**
   * Creates a double constant expression from input value.
   */
  protected abstract T createDoubleConstantExpr(String value) throws SemanticException;

  /**
   * Creates a decimal constant expression from input value.
   * If the constant created from the input value is null, we return:
   * 1) a constant expression containing null value if allowNullValueConstantExpr is true, or
   * 2) null if allowNullValueConstantExpr is false.
   */
  protected abstract T createDecimalConstantExpr(String value, boolean allowNullValueConstantExpr);

  /**
   * Creates a string constant expression from input value.
   */
  protected abstract T createStringConstantExpr(String value);

  /**
   * Creates a date constant expression from input value.
   */
  protected abstract T createDateConstantExpr(String value);

  /**
   * Creates a timestamp constant expression from input value.
   */
  protected abstract T createTimestampConstantExpr(String value);

  /**
   * Creates a timestamp with local time zone constant expression from input value.
   * ZoneId is the local time zone.
   */
  protected abstract T createTimestampLocalTimeZoneConstantExpr(String value, ZoneId zoneId);

  /**
   * Creates a interval year-month constant expression from input value.
   */
  protected abstract T createIntervalYearMonthConstantExpr(String value);

  /**
   * Creates a interval day-time constant expression from input value.
   */
  protected abstract T createIntervalDayTimeConstantExpr(String value);

  /**
   * Creates a interval year constant expression from input value.
   */
  protected abstract T createIntervalYearConstantExpr(String value);

  /**
   * Creates a interval month constant expression from input value.
   */
  protected abstract T createIntervalMonthConstantExpr(String value);

  /**
   * Creates a interval day constant expression from input value.
   */
  protected abstract T createIntervalDayConstantExpr(String value);

  /**
   * Creates a interval hour constant expression from input value.
   */
  protected abstract T createIntervalHourConstantExpr(String value);

  /**
   * Creates a interval minute constant expression from input value.
   */
  protected abstract T createIntervalMinuteConstantExpr(String value);

  /**
   * Creates a interval second constant expression from input value.
   */
  protected abstract T createIntervalSecondConstantExpr(String value);

  /**
   * Default generator for constant expression when type cannot be inferred
   * from input query.
   */
  protected T createConstantExpr(String value) throws SemanticException {
    // The expression can be any one of Double, Long and Integer. We
    // try to parse the expression in that order to ensure that the
    // most specific type is used for conversion.
    T result = null;
    T result2 = null;
    try {
      result = createDoubleConstantExpr(value);
      if (value != null && !value.toLowerCase().contains("e")) {
        result2 = createDecimalConstantExpr(value, false);
        if (result2 != null) {
          result = null; // We will use decimal if all else fails.
        }
      }
      result = createBigintConstantExpr(value);
      result = createIntConstantExpr(value);
    } catch (NumberFormatException e) {
      // do nothing here, we will throw an exception in the following block
    }
    return result != null ? result : result2;
  }

  /**
   * Creates a struct with given type.
   */
  protected abstract T createStructExpr(TypeInfo typeInfo, List<T> operands)
      throws SemanticException;

  /**
   * Creates a constant expression from input value with given type.
   */
  protected abstract T createConstantExpr(TypeInfo typeInfo, Object constantValue)
      throws SemanticException;

  /**
   * Adjust type of constant value based on input type, e.g., adjust precision and scale
   * of decimal value based on type information.
   */
  protected abstract TypeInfo adjustConstantType(PrimitiveTypeInfo targetType, Object constantValue);

  /**
   * Interpret the input constant value of source type as target type.
   */
  protected abstract Object interpretConstantAsPrimitive(PrimitiveTypeInfo targetType,
      Object constantValue, PrimitiveTypeInfo sourceType, boolean isEqual);

  /**
   * Returns value stored in a constant expression.
   */
  protected abstract Object getConstantValue(T expr);

  /**
   * Returns value stored in a constant expression as String.
   */
  protected abstract String getConstantValueAsString(T expr);

  /* METHODS FOR NESTED FIELD REFERENCES CREATION */
  /**
   * Creates a reference to a nested field.
   */
  protected abstract T createNestedColumnRefExpr(
      TypeInfo typeInfo, T expr, String fieldName, Boolean isList)
      throws SemanticException;

  /* FUNCTIONS */
  /**
   * Returns whether the input expression is a function call.
   */
  protected abstract boolean isFuncCallExpr(Object o);

  /**
   * Creates function call expression.
   */
  protected abstract T createFuncCallExpr(TypeInfo typeInfo, FunctionInfo fi, String funcText,
      List<T> inputs) throws SemanticException;

  /**
   * Returns whether the input expression is an OR function call.
   */
  protected abstract boolean isORFuncCallExpr(T expr);

  /**
   * Returns whether the input expression is an AND function call.
   */
  protected abstract boolean isANDFuncCallExpr(T expr);

  /**
   * Returns whether the input expression is a POSITIVE function call.
   */
  protected abstract boolean isPOSITIVEFuncCallExpr(T expr);

  /**
   * Returns whether the input expression is a NEGATIVE function call.
   */
  protected abstract boolean isNEGATIVEFuncCallExpr(T expr);

  /**
   * Returns whether the input expression is a STRUCT function call.
   */
  protected abstract boolean isSTRUCTFuncCallExpr(T expr);

  protected abstract boolean isAndFunction(FunctionInfo fi);

  protected abstract boolean isOrFunction(FunctionInfo fi);

  protected abstract boolean isInFunction(FunctionInfo fi);

  protected abstract boolean isCompareFunction(FunctionInfo fi);

  protected abstract boolean isEqualFunction(FunctionInfo fi);

  protected abstract boolean isNSCompareFunction(FunctionInfo fi);

  protected abstract boolean isConsistentWithinQuery(FunctionInfo fi);

  protected abstract boolean isStateful(FunctionInfo fi);
  /**
   * Returns true if a CASE expression can be converted into a COALESCE function call.
   */
  protected abstract boolean convertCASEIntoCOALESCEFuncCallExpr(FunctionInfo fi, List<T> inputs);
  /**
   * Returns true if a CASE expression can be converted into an IF function call.
   */
  protected boolean convertCASEIntoIFFuncCallExpr(FunctionInfo fi, List<T> inputs) {
    return false;
  }

  /* SUBQUERIES */
  /**
   * Creates subquery expression.
   */
  protected abstract T createSubqueryExpr(TypeCheckCtx ctx, ASTNode subqueryOp, SubqueryType subqueryType,
      Object[] inputs) throws SemanticException;

  /* LIST OF EXPRESSIONS */
  /**
   * Returns whether the input expression is a list of expressions.
   */
  protected abstract boolean isExprsListExpr(Object o);

  /**
   * Creates list of expressions.
   */
  protected abstract T createExprsListExpr();

  /**
   * Adds expression to list of expressions (list needs to be
   * mutable).
   */
  protected abstract void addExprToExprsList(T columnList, T expr);

  /* TYPE SYSTEM */
  /**
   * Returns the type for the input expression.
   */
  protected abstract TypeInfo getTypeInfo(T expr);

  /**
   * Returns the list of types in the input struct expression.
   */
  protected abstract List<TypeInfo> getStructTypeInfoList(T expr);

  /**
   * Changes the type of the input expression to the input type and
   * returns resulting expression.
   * If the input expression is mutable, it will not create a copy
   * of the expression.
   */
  protected abstract T setTypeInfo(T expr, TypeInfo type) throws SemanticException;

  /* MISC */
  /**
   * Folds the input expression and returns resulting expression.
   * If the input expression is mutable, it will not create a copy
   * of the expression.
   */
  protected abstract T foldExpr(T expr);

  /**
   * Returns the children from the input expression (if any).
   */
  protected abstract List<T> getExprChildren(T expr);

  /**
   * Returns the list of names in the input struct expression.
   */
  protected abstract List<String> getStructNameList(T expr);

  /**
   * Returns the FunctionInfo given the name
   */
  protected abstract FunctionInfo getFunctionInfo(String funcName) throws SemanticException;

  protected abstract T replaceFieldNamesInStruct(T expr, List<String> newFieldNames);
}
