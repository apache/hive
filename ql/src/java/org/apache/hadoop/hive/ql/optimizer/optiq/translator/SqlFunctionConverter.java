package org.apache.hadoop.hive.ql.optimizer.optiq.translator;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlAggFunction;
import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlFunctionCategory;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.InferTypes;
import org.eigenbase.sql.type.OperandTypes;
import org.eigenbase.sql.type.ReturnTypes;
import org.eigenbase.sql.type.SqlOperandTypeChecker;
import org.eigenbase.sql.type.SqlOperandTypeInference;
import org.eigenbase.sql.type.SqlReturnTypeInference;
import org.eigenbase.sql.type.SqlTypeFamily;
import org.eigenbase.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class SqlFunctionConverter {
  static final Map<String, SqlOperator>    hiveToOptiq;
  static final Map<SqlOperator, HiveToken> optiqToHiveToken;
  static final Map<SqlOperator, String>    reverseOperatorMap;

  static {
    Builder builder = new Builder();
    hiveToOptiq = ImmutableMap.copyOf(builder.hiveToOptiq);
    optiqToHiveToken = ImmutableMap.copyOf(builder.optiqToHiveToken);
    reverseOperatorMap = ImmutableMap.copyOf(builder.reverseOperatorMap);
  }

  public static SqlOperator getOptiqOperator(GenericUDF hiveUDF,
      ImmutableList<RelDataType> optiqArgTypes, RelDataType retType) {
    return getOptiqFn(getName(hiveUDF), optiqArgTypes, retType);
  }

  public static GenericUDF getHiveUDF(SqlOperator op, RelDataType dt) {
    String name = reverseOperatorMap.get(op);
    if (name == null)
      name = op.getName();
    FunctionInfo hFn = name != null ? FunctionRegistry.getFunctionInfo(name) : null;
    if (hFn == null)
      hFn = handleExplicitCast(op, dt);
    return hFn == null ? null : hFn.getGenericUDF();
  }

  private static FunctionInfo handleExplicitCast(SqlOperator op, RelDataType dt) {
    FunctionInfo castUDF = null;

    if (op.kind == SqlKind.CAST) {
      TypeInfo castType = TypeConverter.convert(dt);

      if (castType.equals(TypeInfoFactory.byteTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("tinyint");
      } else if (castType.equals(TypeInfoFactory.charTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("char");
      } else if (castType.equals(TypeInfoFactory.varcharTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("varchar");
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
        castUDF = FunctionRegistry.getFunctionInfo("datetime");
      } else if (castType.equals(TypeInfoFactory.decimalTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("decimal");
      } else if (castType.equals(TypeInfoFactory.binaryTypeInfo)) {
        castUDF = FunctionRegistry.getFunctionInfo("binary");
      }
    }

    return castUDF;
  }

  // TODO: 1) handle Agg Func Name translation 2) is it correct to add func args
  // as child of func?
  public static ASTNode buildAST(SqlOperator op, List<ASTNode> children) {
    HiveToken hToken = optiqToHiveToken.get(op);
    ASTNode node;
    if (hToken != null) {
      node = (ASTNode) ParseDriver.adaptor.create(hToken.type, hToken.text);
    } else {
      node = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
      if (op.kind != SqlKind.CAST)
        node.addChild((ASTNode) ParseDriver.adaptor.create(HiveParser.Identifier, op.getName()));
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
      HiveToken hToken = optiqToHiveToken.get(op);
      ASTNode curNode = ((ASTNode) ParseDriver.adaptor.create(hToken.type, hToken.text));
      ParseDriver.adaptor.addChild(curNode, children.get(i));
      ParseDriver.adaptor.addChild(curNode, buildAST(op, children, i + 1));
      return curNode;
    } else {
      return children.get(i);
    }

  }

  private static String getName(GenericUDF hiveUDF) {
    String udfName = null;
    if (hiveUDF instanceof GenericUDFBridge) {
      udfName = ((GenericUDFBridge) hiveUDF).getUdfName();
    } else {
      Class udfClass = hiveUDF.getClass();
      Annotation udfAnnotation = udfClass.getAnnotation(Description.class);

      if (udfAnnotation != null && udfAnnotation instanceof Description) {
        Description udfDescription = (Description) udfAnnotation;
        udfName = udfDescription.name();
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

  private static class Builder {
    final Map<String, SqlOperator>    hiveToOptiq        = Maps.newHashMap();
    final Map<SqlOperator, HiveToken> optiqToHiveToken   = Maps.newHashMap();
    final Map<SqlOperator, String>    reverseOperatorMap = Maps.newHashMap();

    Builder() {
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
    }

    private void registerFunction(String name, SqlOperator optiqFn, HiveToken hiveToken) {
      reverseOperatorMap.put(optiqFn, name);
      FunctionInfo hFn = FunctionRegistry.getFunctionInfo(name);
      if (hFn != null) {
        String hFnName = getName(hFn.getGenericUDF());
        hiveToOptiq.put(hFnName, optiqFn);

        if (hiveToken != null) {
          optiqToHiveToken.put(optiqFn, hiveToken);
        }
      }
    }
  }

  private static HiveToken hToken(int type, String text) {
    return new HiveToken(type, text);
  }

  public static class OptiqUDAF extends SqlAggFunction {
    final ImmutableList<RelDataType> m_argTypes;
    final RelDataType                m_retType;

    public OptiqUDAF(String opName, SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker,
        ImmutableList<RelDataType> argTypes, RelDataType retType) {
      super(opName, SqlKind.OTHER_FUNCTION, returnTypeInference, operandTypeInference,
          operandTypeChecker, SqlFunctionCategory.USER_DEFINED_FUNCTION);
      m_argTypes = argTypes;
      m_retType = retType;
    }

    @Override
    public List<RelDataType> getParameterTypes(final RelDataTypeFactory typeFactory) {
      return m_argTypes;
    }

    @Override
    public RelDataType getReturnType(final RelDataTypeFactory typeFactory) {
      return m_retType;
    }
  }

  private static class OptiqUDFInfo {
    private String                     m_udfName;
    private SqlReturnTypeInference     m_returnTypeInference;
    private SqlOperandTypeInference    m_operandTypeInference;
    private SqlOperandTypeChecker      m_operandTypeChecker;
    private ImmutableList<RelDataType> m_argTypes;
    private RelDataType                m_retType;
  }

  private static OptiqUDFInfo getUDFInfo(String hiveUdfName,
      ImmutableList<RelDataType> optiqArgTypes, RelDataType optiqRetType) {
    OptiqUDFInfo udfInfo = new OptiqUDFInfo();
    udfInfo.m_udfName = hiveUdfName;
    udfInfo.m_returnTypeInference = ReturnTypes.explicit(optiqRetType);
    udfInfo.m_operandTypeInference = InferTypes.explicit(optiqArgTypes);
    ImmutableList.Builder<SqlTypeFamily> typeFamilyBuilder = new ImmutableList.Builder<SqlTypeFamily>();
    for (RelDataType at : optiqArgTypes) {
      typeFamilyBuilder.add(Util.first(at.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
    }
    udfInfo.m_operandTypeChecker = OperandTypes.family(typeFamilyBuilder.build());

    udfInfo.m_argTypes = ImmutableList.<RelDataType> copyOf(optiqArgTypes);
    udfInfo.m_retType = optiqRetType;

    return udfInfo;
  }

  public static SqlOperator getOptiqFn(String hiveUdfName,
      ImmutableList<RelDataType> optiqArgTypes, RelDataType optiqRetType) {
    SqlOperator optiqOp = hiveToOptiq.get(hiveUdfName);
    if (optiqOp == null) {
      OptiqUDFInfo uInf = getUDFInfo(hiveUdfName, optiqArgTypes, optiqRetType);
      optiqOp = new SqlFunction(uInf.m_udfName, SqlKind.OTHER_FUNCTION, uInf.m_returnTypeInference,
          uInf.m_operandTypeInference, uInf.m_operandTypeChecker,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    return optiqOp;
  }

  public static SqlAggFunction getOptiqAggFn(String hiveUdfName,
      ImmutableList<RelDataType> optiqArgTypes, RelDataType optiqRetType) {
    SqlAggFunction optiqAggFn = (SqlAggFunction) hiveToOptiq.get(hiveUdfName);
    if (optiqAggFn == null) {
      OptiqUDFInfo uInf = getUDFInfo(hiveUdfName, optiqArgTypes, optiqRetType);

      optiqAggFn = new OptiqUDAF(uInf.m_udfName, uInf.m_returnTypeInference,
          uInf.m_operandTypeInference, uInf.m_operandTypeChecker, uInf.m_argTypes, uInf.m_retType);
    }

    return optiqAggFn;
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
