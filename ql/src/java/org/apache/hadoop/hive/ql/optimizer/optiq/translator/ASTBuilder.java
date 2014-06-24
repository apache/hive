package org.apache.hadoop.hive.ql.optimizer.optiq.translator;

import org.apache.hadoop.hive.ql.optimizer.optiq.RelOptHiveTable;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.sql.type.SqlTypeName;

class ASTBuilder {

  static ASTBuilder construct(int tokenType, String text) {
    ASTBuilder b = new ASTBuilder();
    b.curr = createAST(tokenType, text);
    return b;
  }

  static ASTNode createAST(int tokenType, String text) {
    return (ASTNode) ParseDriver.adaptor.create(tokenType, text);
  }

  static ASTNode destNode() {
    return ASTBuilder
        .construct(HiveParser.TOK_DESTINATION, "TOK_DESTINATION")
        .add(
            ASTBuilder.construct(HiveParser.TOK_DIR, "TOK_DIR").add(HiveParser.TOK_TMP_FILE,
                "TOK_TMP_FILE")).node();
  }

  static ASTNode table(TableAccessRelBase scan) {
    RelOptHiveTable hTbl = (RelOptHiveTable) scan.getTable();
    ASTBuilder b = ASTBuilder
        .construct(HiveParser.TOK_TABREF, "TOK_TABREF")
        .add(
            ASTBuilder.construct(HiveParser.TOK_TABNAME, "TOK_TABNAME")
                .add(HiveParser.Identifier, hTbl.getHiveTableMD().getDbName())
                .add(HiveParser.Identifier, hTbl.getHiveTableMD().getTableName()))
        .add(HiveParser.Identifier, hTbl.getName());
    return b.node();
  }

  static ASTNode join(ASTNode left, ASTNode right, JoinRelType joinType,
      ASTNode cond, boolean semiJoin) {
    ASTBuilder b = null;

    switch (joinType) {
    case INNER:
      if (semiJoin) {
        b = ASTBuilder.construct(HiveParser.TOK_LEFTSEMIJOIN,
            "TOK_LEFTSEMIJOIN");
      } else {
        b = ASTBuilder.construct(HiveParser.TOK_JOIN, "TOK_JOIN");
      }
      break;
    case LEFT:
      b = ASTBuilder.construct(HiveParser.TOK_LEFTOUTERJOIN,
          "TOK_LEFTOUTERJOIN");
      break;
    case RIGHT:
      b = ASTBuilder.construct(HiveParser.TOK_RIGHTOUTERJOIN,
          "TOK_RIGHTOUTERJOIN");
      break;
    case FULL:
      b = ASTBuilder.construct(HiveParser.TOK_FULLOUTERJOIN,
          "TOK_FULLOUTERJOIN");
      break;
    }

    b.add(left).add(right).add(cond);
    return b.node();
  }

  static ASTNode subQuery(ASTNode qry, String alias) {
    return ASTBuilder.construct(HiveParser.TOK_SUBQUERY, "TOK_SUBQUERY").add(qry)
        .add(HiveParser.Identifier, alias).node();
  }

  static ASTNode qualifiedName(String tableName, String colName) {
    ASTBuilder b = ASTBuilder
        .construct(HiveParser.DOT, ".")
        .add(
            ASTBuilder.construct(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL").add(
                HiveParser.Identifier, tableName)).add(HiveParser.Identifier, colName);
    return b.node();
  }

  static ASTNode unqualifiedName(String colName) {
    ASTBuilder b = ASTBuilder
.construct(HiveParser.TOK_TABLE_OR_COL,
        "TOK_TABLE_OR_COL").add(HiveParser.Identifier, colName);
    return b.node();
  }

  static ASTNode where(ASTNode cond) {
    return ASTBuilder.construct(HiveParser.TOK_WHERE, "TOK_WHERE").add(cond).node();
  }

  static ASTNode having(ASTNode cond) {
    return ASTBuilder.construct(HiveParser.TOK_HAVING, "TOK_HAVING").add(cond).node();
  }

  static ASTNode limit(Object value) {
    return ASTBuilder.construct(HiveParser.TOK_LIMIT, "TOK_LIMIT")
        .add(HiveParser.Number, value.toString()).node();
  }

  static ASTNode selectExpr(ASTNode expr, String alias) {
    return ASTBuilder.construct(HiveParser.TOK_SELEXPR, "TOK_SELEXPR").add(expr)
        .add(HiveParser.Identifier, alias).node();
  }

  static ASTNode literal(RexLiteral literal) {
    Object val = literal.getValue3();
    int type = 0;
    SqlTypeName sqlType = literal.getType().getSqlTypeName();

    switch (sqlType) {
    case TINYINT:
      type = HiveParser.TinyintLiteral;
      break;
    case SMALLINT:
      type = HiveParser.SmallintLiteral;
      break;
    case INTEGER:
    case BIGINT:
      type = HiveParser.BigintLiteral;
      break;
    case DECIMAL:
    case FLOAT:
    case DOUBLE:
    case REAL:
      type = HiveParser.Number;
      break;
    case VARCHAR:
    case CHAR:
      type = HiveParser.StringLiteral;
      val = "'" + String.valueOf(val) + "'";
      break;
    case BOOLEAN:
      type = ((Boolean) val).booleanValue() ? HiveParser.KW_TRUE
          : HiveParser.KW_FALSE;
      break;

    default:
      throw new RuntimeException("Unsupported Type: " + sqlType);
    }

    return (ASTNode) ParseDriver.adaptor.create(type, String.valueOf(val));
  }

  ASTNode curr;

  ASTNode node() {
    return curr;
  }

  ASTBuilder add(int tokenType, String text) {
    ParseDriver.adaptor.addChild(curr, createAST(tokenType, text));
    return this;
  }

  ASTBuilder add(ASTBuilder b) {
    ParseDriver.adaptor.addChild(curr, b.curr);
    return this;
  }

  ASTBuilder add(ASTNode n) {
    if (n != null) {
      ParseDriver.adaptor.addChild(curr, n);
    }
    return this;
  }
}