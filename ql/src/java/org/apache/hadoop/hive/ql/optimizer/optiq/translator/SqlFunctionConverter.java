package org.apache.hadoop.hive.ql.optimizer.optiq.translator;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
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
import com.google.common.collect.Maps;

public class SqlFunctionConverter {
  static final Map<String, SqlOperator>    hiveToOptiq;
  static final Map<SqlOperator, HiveToken> optiqToHiveToken;

  static {
    Builder builder = new Builder();
    hiveToOptiq = builder.hiveToOptiq;
    optiqToHiveToken = builder.optiqToHiveToken;
  }

  public static SqlOperator getOptiqOperator(GenericUDF hiveUDF,
      ImmutableList<RelDataType> optiqArgTypes, RelDataType retType) {
    return getOptiqFn(getName(hiveUDF), optiqArgTypes, retType);
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
    if (hiveUDF instanceof GenericUDFBridge) {
      return ((GenericUDFBridge) hiveUDF).getUdfName();
    } else {
      return hiveUDF.getClass().getName();
    }
  }

  private static class Builder {
    final Map<String, SqlOperator>    hiveToOptiq      = Maps.newHashMap();
    final Map<SqlOperator, HiveToken> optiqToHiveToken = Maps.newHashMap();

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

    public List<RelDataType> getParameterTypes(final RelDataTypeFactory typeFactory) {
      return m_argTypes;
    }

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
      hiveToOptiq.put(hiveUdfName, optiqOp);
      HiveToken ht = hToken(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
      optiqToHiveToken.put(optiqOp, ht);
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
      hiveToOptiq.put(hiveUdfName, optiqAggFn);
      HiveToken ht = hToken(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
      optiqToHiveToken.put(optiqAggFn, ht);
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
