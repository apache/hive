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
import org.apache.hadoop.hive.serde.serdeConstants;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlAggFunction;
import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlFunctionCategory;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.OperandTypes;
import org.eigenbase.sql.type.ReturnTypes;
import org.eigenbase.sql.type.SqlReturnTypeInference;
import org.eigenbase.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class SqlFunctionConverter {
  static final Map<String, SqlOperator>    operatorMap;
  static final Map<String, SqlOperator>    hiveToOptiq;
  static final Map<SqlOperator, HiveToken> optiqToHiveToken;

  static {
    Builder builder = new Builder();
    operatorMap = ImmutableMap.copyOf(builder.operatorMap);
    hiveToOptiq = ImmutableMap.copyOf(builder.hiveToOptiq);
    optiqToHiveToken = ImmutableMap.copyOf(builder.optiqToHiveToken);
  }

  public static SqlOperator getOptiqOperator(GenericUDF hiveUDF) {
    return hiveToOptiq.get(getName(hiveUDF));
  }

  public static ASTNode buildAST(SqlOperator op, List<ASTNode> children) {
    HiveToken hToken = optiqToHiveToken.get(op);
    ASTNode node;
    if (hToken != null) {
      node = (ASTNode) ParseDriver.adaptor.create(hToken.type, hToken.text);
    } else {
      node = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
      if (op.kind != SqlKind.CAST)
        node.addChild((ASTNode) ParseDriver.adaptor.create(
            HiveParser.Identifier, op.getName()));
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
    final Map<String, SqlOperator>    operatorMap      = Maps.newHashMap();
    final Map<String, SqlOperator>    hiveToOptiq      = Maps.newHashMap();
    final Map<SqlOperator, HiveToken> optiqToHiveToken = Maps.newHashMap();

    Builder() {
      registerFunction("concat", SqlStdOperatorTable.CONCAT, null);
      registerFunction("substr", SqlStdOperatorTable.SUBSTRING, null);
      registerFunction("substring", SqlStdOperatorTable.SUBSTRING, null);
      stringFunction("space");
      stringFunction("repeat");
      numericFunction("ascii");
      stringFunction("repeat");

      numericFunction("size");

      numericFunction("round");
      registerFunction("floor", SqlStdOperatorTable.FLOOR, null);
      registerFunction("sqrt", SqlStdOperatorTable.SQRT, null);
      registerFunction("ceil", SqlStdOperatorTable.CEIL, null);
      registerFunction("ceiling", SqlStdOperatorTable.CEIL, null);
      numericFunction("rand");
      operatorMap.put("abs", SqlStdOperatorTable.ABS);
      numericFunction("pmod");

      numericFunction("ln");
      numericFunction("log2");
      numericFunction("sin");
      numericFunction("asin");
      numericFunction("cos");
      numericFunction("acos");
      registerFunction("log10", SqlStdOperatorTable.LOG10, null);
      numericFunction("log");
      numericFunction("exp");
      numericFunction("power");
      numericFunction("pow");
      numericFunction("sign");
      numericFunction("pi");
      numericFunction("degrees");
      numericFunction("atan");
      numericFunction("tan");
      numericFunction("e");

      registerFunction("upper", SqlStdOperatorTable.UPPER, null);
      registerFunction("lower", SqlStdOperatorTable.LOWER, null);
      registerFunction("ucase", SqlStdOperatorTable.UPPER, null);
      registerFunction("lcase", SqlStdOperatorTable.LOWER, null);
      registerFunction("trim", SqlStdOperatorTable.TRIM, null);
      stringFunction("ltrim");
      stringFunction("rtrim");
      numericFunction("length");

      stringFunction("like");
      stringFunction("rlike");
      stringFunction("regexp");
      stringFunction("regexp_replace");

      stringFunction("regexp_extract");
      stringFunction("parse_url");

      numericFunction("day");
      numericFunction("dayofmonth");
      numericFunction("month");
      numericFunction("year");
      numericFunction("hour");
      numericFunction("minute");
      numericFunction("second");

      registerFunction("+", SqlStdOperatorTable.PLUS, hToken(HiveParser.PLUS, "+"));
      registerFunction("-", SqlStdOperatorTable.MINUS, hToken(HiveParser.MINUS, "-"));
      registerFunction("*", SqlStdOperatorTable.MULTIPLY, hToken(HiveParser.STAR, "*"));
      registerFunction("/", SqlStdOperatorTable.DIVIDE, hToken(HiveParser.STAR, "/"));
      registerFunction("%", SqlStdOperatorTable.MOD, hToken(HiveParser.STAR, "%"));
      numericFunction("div");

      numericFunction("isnull");
      numericFunction("isnotnull");

      numericFunction("if");
      numericFunction("in");
      registerFunction("and", SqlStdOperatorTable.AND, hToken(HiveParser.KW_AND, "and"));
      registerFunction("or", SqlStdOperatorTable.OR, hToken(HiveParser.KW_OR, "or"));
      registerFunction("=", SqlStdOperatorTable.EQUALS, hToken(HiveParser.EQUAL, "="));
//      numericFunction("==");
      numericFunction("<=>");
      numericFunction("!=");

      numericFunction("<>");
      registerFunction("<", SqlStdOperatorTable.LESS_THAN, hToken(HiveParser.LESSTHAN, "<"));
      registerFunction("<=", SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
          hToken(HiveParser.LESSTHANOREQUALTO, "<="));
      registerFunction(">", SqlStdOperatorTable.GREATER_THAN, hToken(HiveParser.GREATERTHAN, ">"));
      registerFunction(">=", SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
          hToken(HiveParser.GREATERTHANOREQUALTO, ">="));
      numericFunction("not");
      registerFunction("!", SqlStdOperatorTable.NOT, hToken(HiveParser.KW_NOT, "not"));
      numericFunction("between");

      registerFunction("case", SqlStdOperatorTable.CASE, null);
      numericFunction("when");

      // implicit convert methods
      numericFunction(serdeConstants.BOOLEAN_TYPE_NAME);
      numericFunction(serdeConstants.TINYINT_TYPE_NAME);
      numericFunction(serdeConstants.SMALLINT_TYPE_NAME);
      numericFunction(serdeConstants.INT_TYPE_NAME);
      numericFunction(serdeConstants.BIGINT_TYPE_NAME);
      numericFunction(serdeConstants.FLOAT_TYPE_NAME);
      numericFunction(serdeConstants.DOUBLE_TYPE_NAME);
      stringFunction(serdeConstants.STRING_TYPE_NAME);
    }

    private void stringFunction(String name) {
      registerFunction(name, SqlFunctionCategory.STRING, ReturnTypes.explicit(SqlTypeName.VARCHAR));
    }

    private void numericFunction(String name) {
      registerFunction(name, SqlFunctionCategory.NUMERIC, ReturnTypes.explicit(SqlTypeName.DECIMAL));
    }

    private void registerFunction(String name, SqlFunctionCategory cat, SqlReturnTypeInference rti) {
      SqlOperator optiqFn = new SqlFunction(name.toUpperCase(), SqlKind.OTHER_FUNCTION, rti, null,
          null, cat);
      registerFunction(name, optiqFn, null);
    }

    private void registerFunction(String name, SqlOperator optiqFn,
        HiveToken hiveToken) {
      operatorMap.put(name, optiqFn);

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

  public static SqlAggFunction hiveAggFunction(String name) {
    return new HiveAggFunction(name);
  }

  static class HiveAggFunction extends SqlAggFunction {

    public HiveAggFunction(String name) {
      super(name, SqlKind.OTHER_FUNCTION, ReturnTypes.BIGINT, null,
          OperandTypes.ANY, SqlFunctionCategory.NUMERIC);
    }

    public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
      return ImmutableList.of(typeFactory.createSqlType(SqlTypeName.ANY));
    }

    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
      return typeFactory.createSqlType(SqlTypeName.BIGINT);
    }

  }

  static class HiveToken {
    int    type;
    String text;
    String[] args;

    HiveToken(int type, String text, String... args) {
      this.type = type;
      this.text = text;
      this.args = args;
    }
  }  
}
