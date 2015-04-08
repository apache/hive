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
package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException.UnsupportedFeature;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.SettableUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNegative;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPositive;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class SqlFunctionConverter {
  private static final Log LOG = LogFactory.getLog(SqlFunctionConverter.class);

  static final Map<String, SqlOperator>    hiveToCalcite;
  static final Map<SqlOperator, HiveToken> calciteToHiveToken;
  static final Map<SqlOperator, String>    reverseOperatorMap;

  static {
    StaticBlockBuilder builder = new StaticBlockBuilder();
    hiveToCalcite = ImmutableMap.copyOf(builder.hiveToCalcite);
    calciteToHiveToken = ImmutableMap.copyOf(builder.calciteToHiveToken);
    reverseOperatorMap = ImmutableMap.copyOf(builder.reverseOperatorMap);
  }

  public static SqlOperator getCalciteOperator(String funcTextName, GenericUDF hiveUDF,
      ImmutableList<RelDataType> calciteArgTypes, RelDataType retType)
      throws SemanticException {
    // handle overloaded methods first
    if (hiveUDF instanceof GenericUDFOPNegative) {
      return SqlStdOperatorTable.UNARY_MINUS;
    } else if (hiveUDF instanceof GenericUDFOPPositive) {
      return SqlStdOperatorTable.UNARY_PLUS;
    } // do generic lookup
    String name = null;
    if (StringUtils.isEmpty(funcTextName)) {
      name = getName(hiveUDF); // this should probably never happen, see
      // getName
      // comment
      LOG.warn("The function text was empty, name from annotation is " + name);
    } else {
      // We could just do toLowerCase here and let SA qualify it, but
      // let's be proper...
      name = FunctionRegistry.getNormalizedFunctionName(funcTextName);
    }
    return getCalciteFn(name, calciteArgTypes, retType, FunctionRegistry.isDeterministic(hiveUDF));
  }

  public static GenericUDF getHiveUDF(SqlOperator op, RelDataType dt, int argsLength) {
    String name = reverseOperatorMap.get(op);
    if (name == null) {
      name = op.getName();
    }
    // Make sure we handle unary + and - correctly.
    if (argsLength == 1) {
      if (name == "+") {
        name = FunctionRegistry.UNARY_PLUS_FUNC_NAME;
      } else if (name == "-") {
        name = FunctionRegistry.UNARY_MINUS_FUNC_NAME;
      }
    }
    FunctionInfo hFn;
    try {
      hFn = name != null ? FunctionRegistry.getFunctionInfo(name) : null;
    } catch (SemanticException e) {
      LOG.warn("Failed to load udf " + name, e);
      hFn = null;
    }
    if (hFn == null) {
      try {
        hFn = handleExplicitCast(op, dt);
      } catch (SemanticException e) {
        LOG.warn("Failed to load udf " + name, e);
        hFn = null;
      }
    }
    return hFn == null ? null : hFn.getGenericUDF();
  }

  private static FunctionInfo handleExplicitCast(SqlOperator op, RelDataType dt)
      throws SemanticException {
    FunctionInfo castUDF = null;

    if (op.kind == SqlKind.CAST) {
      TypeInfo castType = TypeConverter.convert(dt);

      if (castType.equals(TypeInfoFactory.byteTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("tinyint");
      } else if (castType instanceof CharTypeInfo) {
        castUDF = handleCastForParameterizedType(castType, FunctionRegistry.getFunctionInfo("char"));
      } else if (castType instanceof VarcharTypeInfo) {
        castUDF = handleCastForParameterizedType(castType,
            FunctionRegistry.getFunctionInfo("varchar"));
      } else if (castType.equals(TypeInfoFactory.stringTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("string");
      } else if (castType.equals(TypeInfoFactory.booleanTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("boolean");
      } else if (castType.equals(TypeInfoFactory.shortTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("smallint");
      } else if (castType.equals(TypeInfoFactory.intTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("int");
      } else if (castType.equals(TypeInfoFactory.longTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("bigint");
      } else if (castType.equals(TypeInfoFactory.floatTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("float");
      } else if (castType.equals(TypeInfoFactory.doubleTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("double");
      } else if (castType.equals(TypeInfoFactory.timestampTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("timestamp");
      } else if (castType.equals(TypeInfoFactory.dateTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("date");
      } else if (castType instanceof DecimalTypeInfo) {
        castUDF = handleCastForParameterizedType(castType,
            FunctionRegistry.getFunctionInfo("decimal"));
      } else if (castType.equals(TypeInfoFactory.binaryTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("binary");
      } else
        throw new IllegalStateException("Unexpected type : " + castType.getQualifiedName());
    }

    return castUDF;
  }

  private static FunctionInfo handleCastForParameterizedType(TypeInfo ti, FunctionInfo fi) {
    SettableUDF udf = (SettableUDF) fi.getGenericUDF();
    try {
      udf.setTypeInfo(ti);
    } catch (UDFArgumentException e) {
      throw new RuntimeException(e);
    }
    return new FunctionInfo(
        fi.isNative(), fi.getDisplayName(), (GenericUDF) udf, fi.getResources());
  }

  // TODO: 1) handle Agg Func Name translation 2) is it correct to add func
  // args as child of func?
  public static ASTNode buildAST(SqlOperator op, List<ASTNode> children) {
    HiveToken hToken = calciteToHiveToken.get(op);
    ASTNode node;
    if (hToken != null) {
      node = (ASTNode) ParseDriver.adaptor.create(hToken.type, hToken.text);
    } else {
      node = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
      if (op.kind != SqlKind.CAST) {
        if (op.kind == SqlKind.MINUS_PREFIX) {
          node = (ASTNode) ParseDriver.adaptor.create(HiveParser.MINUS, "MINUS");
        } else if (op.kind == SqlKind.PLUS_PREFIX) {
          node = (ASTNode) ParseDriver.adaptor.create(HiveParser.PLUS, "PLUS");
        } else {
          if (op.getName().toUpperCase().equals(SqlStdOperatorTable.COUNT.getName())
              && children.size() == 0) {
            node = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_FUNCTIONSTAR,
                "TOK_FUNCTIONSTAR");
          }
          node.addChild((ASTNode) ParseDriver.adaptor.create(HiveParser.Identifier, op.getName()));
        }
      }
    }

    for (ASTNode c : children) {
      ParseDriver.adaptor.addChild(node, c);
    }
    return node;
  }

  /**
   * Build AST for flattened Associative expressions ('and', 'or'). Flattened
   * expressions is of the form or[x,y,z] which is originally represented as
   * "or[x, or[y, z]]".
   */
  public static ASTNode buildAST(SqlOperator op, List<ASTNode> children, int i) {
    if (i + 1 < children.size()) {
      HiveToken hToken = calciteToHiveToken.get(op);
      ASTNode curNode = ((ASTNode) ParseDriver.adaptor.create(hToken.type, hToken.text));
      ParseDriver.adaptor.addChild(curNode, children.get(i));
      ParseDriver.adaptor.addChild(curNode, buildAST(op, children, i + 1));
      return curNode;
    } else {
      return children.get(i);
    }

  }

  // TODO: this is not valid. Function names for built-in UDFs are specified in
  // FunctionRegistry, and only happen to match annotations. For user UDFs, the
  // name is what user specifies at creation time (annotation can be absent,
  // different, or duplicate some other function).
  private static String getName(GenericUDF hiveUDF) {
    String udfName = null;
    if (hiveUDF instanceof GenericUDFBridge) {
      udfName = ((GenericUDFBridge) hiveUDF).getUdfName();
    } else {
      Class<? extends GenericUDF> udfClass = hiveUDF.getClass();
      Annotation udfAnnotation = udfClass.getAnnotation(Description.class);

      if (udfAnnotation != null && udfAnnotation instanceof Description) {
        Description udfDescription = (Description) udfAnnotation;
        udfName = udfDescription.name();
        if (udfName != null) {
          String[] aliases = udfName.split(",");
          if (aliases.length > 0)
            udfName = aliases[0];
        }
      }

      if (udfName == null || udfName.isEmpty()) {
        udfName = hiveUDF.getClass().getName();
        int indx = udfName.lastIndexOf(".");
        if (indx >= 0) {
          indx += 1;
          udfName = udfName.substring(indx);
        }
      }
    }

    return udfName;
  }

  /**
   * This class is used to build immutable hashmaps in the static block above.
   */
  private static class StaticBlockBuilder {
    final Map<String, SqlOperator>    hiveToCalcite      = Maps.newHashMap();
    final Map<SqlOperator, HiveToken> calciteToHiveToken = Maps.newHashMap();
    final Map<SqlOperator, String>    reverseOperatorMap = Maps.newHashMap();

    StaticBlockBuilder() {
      registerFunction("+", SqlStdOperatorTable.PLUS, hToken(HiveParser.PLUS, "+"));
      registerFunction("-", SqlStdOperatorTable.MINUS, hToken(HiveParser.MINUS, "-"));
      registerFunction("*", SqlStdOperatorTable.MULTIPLY, hToken(HiveParser.STAR, "*"));
      registerFunction("/", SqlStdOperatorTable.DIVIDE, hToken(HiveParser.STAR, "/"));
      registerFunction("%", SqlStdOperatorTable.MOD, hToken(HiveParser.STAR, "%"));
      registerFunction("and", SqlStdOperatorTable.AND, hToken(HiveParser.KW_AND, "and"));
      registerFunction("or", SqlStdOperatorTable.OR, hToken(HiveParser.KW_OR, "or"));
      registerFunction("=", SqlStdOperatorTable.EQUALS, hToken(HiveParser.EQUAL, "="));
      registerFunction("<", SqlStdOperatorTable.LESS_THAN, hToken(HiveParser.LESSTHAN, "<"));
      registerFunction("<=", SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
          hToken(HiveParser.LESSTHANOREQUALTO, "<="));
      registerFunction(">", SqlStdOperatorTable.GREATER_THAN, hToken(HiveParser.GREATERTHAN, ">"));
      registerFunction(">=", SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
          hToken(HiveParser.GREATERTHANOREQUALTO, ">="));
      registerFunction("!", SqlStdOperatorTable.NOT, hToken(HiveParser.KW_NOT, "not"));
      registerFunction("<>", SqlStdOperatorTable.NOT_EQUALS, hToken(HiveParser.NOTEQUAL, "<>"));
    }

    private void registerFunction(String name, SqlOperator calciteFn, HiveToken hiveToken) {
      reverseOperatorMap.put(calciteFn, name);
      FunctionInfo hFn;
      try {
        hFn = FunctionRegistry.getFunctionInfo(name);
      } catch (SemanticException e) {
        LOG.warn("Failed to load udf " + name, e);
        hFn = null;
      }
      if (hFn != null) {
        String hFnName = getName(hFn.getGenericUDF());
        hiveToCalcite.put(hFnName, calciteFn);

        if (hiveToken != null) {
          calciteToHiveToken.put(calciteFn, hiveToken);
        }
      }
    }
  }

  private static HiveToken hToken(int type, String text) {
    return new HiveToken(type, text);
  }

  // UDAF is assumed to be deterministic
  public static class CalciteUDAF extends SqlAggFunction {
    public CalciteUDAF(String opName, SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker,
        ImmutableList<RelDataType> argTypes, RelDataType retType) {
      super(opName, SqlKind.OTHER_FUNCTION, returnTypeInference, operandTypeInference,
          operandTypeChecker, SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }
  }

  private static class CalciteSqlFn extends SqlFunction {
    private final boolean deterministic;

    public CalciteSqlFn(String name, SqlKind kind, SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker,
        SqlFunctionCategory category, boolean deterministic) {
      super(name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, category);
      this.deterministic = deterministic;
    }

    @Override
    public boolean isDeterministic() {
      return deterministic;
    }
  }

  private static class CalciteUDFInfo {
    private String                     udfName;
    private SqlReturnTypeInference     returnTypeInference;
    private SqlOperandTypeInference    operandTypeInference;
    private SqlOperandTypeChecker      operandTypeChecker;
    private ImmutableList<RelDataType> argTypes;
    private RelDataType                retType;
  }

  private static CalciteUDFInfo getUDFInfo(String hiveUdfName,
      ImmutableList<RelDataType> calciteArgTypes, RelDataType calciteRetType) {
    CalciteUDFInfo udfInfo = new CalciteUDFInfo();
    udfInfo.udfName = hiveUdfName;
    udfInfo.returnTypeInference = ReturnTypes.explicit(calciteRetType);
    udfInfo.operandTypeInference = InferTypes.explicit(calciteArgTypes);
    ImmutableList.Builder<SqlTypeFamily> typeFamilyBuilder = new ImmutableList.Builder<SqlTypeFamily>();
    for (RelDataType at : calciteArgTypes) {
      typeFamilyBuilder.add(Util.first(at.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
    }
    udfInfo.operandTypeChecker = OperandTypes.family(typeFamilyBuilder.build());

    udfInfo.argTypes = ImmutableList.<RelDataType> copyOf(calciteArgTypes);
    udfInfo.retType = calciteRetType;

    return udfInfo;
  }

  public static SqlOperator getCalciteFn(String hiveUdfName,
      ImmutableList<RelDataType> calciteArgTypes, RelDataType calciteRetType, boolean deterministic)
      throws CalciteSemanticException {

    if (hiveUdfName != null && hiveUdfName.trim().equals("<=>")) {
      // We can create Calcite IS_DISTINCT_FROM operator for this. But since our
      // join reordering algo cant handle this anyway there is no advantage of
      // this.So, bail out for now.
      throw new CalciteSemanticException("<=> is not yet supported for cbo.", UnsupportedFeature.Less_than_equal_greater_than);
    }
    SqlOperator calciteOp = hiveToCalcite.get(hiveUdfName);
    if (calciteOp == null) {
      CalciteUDFInfo uInf = getUDFInfo(hiveUdfName, calciteArgTypes, calciteRetType);
      calciteOp = new CalciteSqlFn(uInf.udfName, SqlKind.OTHER_FUNCTION, uInf.returnTypeInference,
          uInf.operandTypeInference, uInf.operandTypeChecker,
          SqlFunctionCategory.USER_DEFINED_FUNCTION, deterministic);
    }

    return calciteOp;
  }

  public static SqlAggFunction getCalciteAggFn(String hiveUdfName,
      ImmutableList<RelDataType> calciteArgTypes, RelDataType calciteRetType) {
    SqlAggFunction calciteAggFn = (SqlAggFunction) hiveToCalcite.get(hiveUdfName);
    if (calciteAggFn == null) {
      CalciteUDFInfo uInf = getUDFInfo(hiveUdfName, calciteArgTypes, calciteRetType);

      calciteAggFn = new CalciteUDAF(uInf.udfName, uInf.returnTypeInference,
          uInf.operandTypeInference, uInf.operandTypeChecker, uInf.argTypes, uInf.retType);
    }

    return calciteAggFn;
  }

  static class HiveToken {
    int      type;
    String   text;
    String[] args;

    HiveToken(int type, String text, String... args) {
      this.type = type;
      this.text = text;
      this.args = args;
    }
  }
}
