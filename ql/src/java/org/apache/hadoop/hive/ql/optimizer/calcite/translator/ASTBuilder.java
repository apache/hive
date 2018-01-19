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
package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import java.math.BigDecimal;

import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;

public class ASTBuilder {

  public static ASTBuilder construct(int tokenType, String text) {
    ASTBuilder b = new ASTBuilder();
    b.curr = createAST(tokenType, text);
    return b;
  }

  public static ASTNode createAST(int tokenType, String text) {
    return (ASTNode) ParseDriver.adaptor.create(tokenType, text);
  }

  public static ASTNode destNode() {
    return ASTBuilder
        .construct(HiveParser.TOK_DESTINATION, "TOK_DESTINATION")
        .add(
            ASTBuilder.construct(HiveParser.TOK_DIR, "TOK_DIR").add(HiveParser.TOK_TMP_FILE,
                "TOK_TMP_FILE")).node();
  }

  public static ASTNode table(RelNode scan) {
    HiveTableScan hts;
    if (scan instanceof DruidQuery) {
      hts = (HiveTableScan) ((DruidQuery)scan).getTableScan();
    } else {
      hts = (HiveTableScan) scan;
    }

    RelOptHiveTable hTbl = (RelOptHiveTable) hts.getTable();
    ASTBuilder b = ASTBuilder.construct(HiveParser.TOK_TABREF, "TOK_TABREF").add(
        ASTBuilder.construct(HiveParser.TOK_TABNAME, "TOK_TABNAME")
            .add(HiveParser.Identifier, hTbl.getHiveTableMD().getDbName())
            .add(HiveParser.Identifier, hTbl.getHiveTableMD().getTableName()));

    ASTBuilder propList = ASTBuilder.construct(HiveParser.TOK_TABLEPROPLIST, "TOK_TABLEPROPLIST");
    if (scan instanceof DruidQuery) {
      // Pass possible query to Druid
      DruidQuery dq = (DruidQuery) scan;
      propList.add(ASTBuilder.construct(HiveParser.TOK_TABLEPROPERTY, "TOK_TABLEPROPERTY")
              .add(HiveParser.StringLiteral, "\"" + Constants.DRUID_QUERY_JSON + "\"")
              .add(HiveParser.StringLiteral, "\"" + SemanticAnalyzer.escapeSQLString(
                      dq.getQueryString()) + "\""));
      propList.add(ASTBuilder.construct(HiveParser.TOK_TABLEPROPERTY, "TOK_TABLEPROPERTY")
              .add(HiveParser.StringLiteral, "\"" + Constants.DRUID_QUERY_TYPE + "\"")
              .add(HiveParser.StringLiteral, "\"" + dq.getQueryType().getQueryName() + "\""));
    }
    if (hts.isInsideView()) {
      // We need to carry the insideView information from calcite into the ast.
      propList.add(ASTBuilder.construct(HiveParser.TOK_TABLEPROPERTY, "TOK_TABLEPROPERTY")
              .add(HiveParser.StringLiteral, "\"insideView\"")
              .add(HiveParser.StringLiteral, "\"TRUE\""));
    }
    b.add(ASTBuilder.construct(HiveParser.TOK_TABLEPROPERTIES, "TOK_TABLEPROPERTIES").add(propList));

    // NOTE: Calcite considers tbls to be equal if their names are the same. Hence
    // we need to provide Calcite the fully qualified table name (dbname.tblname)
    // and not the user provided aliases.
    // However in HIVE DB name can not appear in select list; in case of join
    // where table names differ only in DB name, Hive would require user
    // introducing explicit aliases for tbl.
    b.add(HiveParser.Identifier, hts.getTableAlias());
    return b.node();
  }

  public static ASTNode join(ASTNode left, ASTNode right, JoinRelType joinType, ASTNode cond,
      boolean semiJoin) {
    ASTBuilder b = null;

    switch (joinType) {
    case INNER:
      if (semiJoin) {
        b = ASTBuilder.construct(HiveParser.TOK_LEFTSEMIJOIN, "TOK_LEFTSEMIJOIN");
      } else {
        b = ASTBuilder.construct(HiveParser.TOK_JOIN, "TOK_JOIN");
      }
      break;
    case LEFT:
      b = ASTBuilder.construct(HiveParser.TOK_LEFTOUTERJOIN, "TOK_LEFTOUTERJOIN");
      break;
    case RIGHT:
      b = ASTBuilder.construct(HiveParser.TOK_RIGHTOUTERJOIN, "TOK_RIGHTOUTERJOIN");
      break;
    case FULL:
      b = ASTBuilder.construct(HiveParser.TOK_FULLOUTERJOIN, "TOK_FULLOUTERJOIN");
      break;
    }

    b.add(left).add(right).add(cond);
    return b.node();
  }

  public static ASTNode subQuery(ASTNode qry, String alias) {
    return ASTBuilder.construct(HiveParser.TOK_SUBQUERY, "TOK_SUBQUERY").add(qry)
        .add(HiveParser.Identifier, alias).node();
  }

  public static ASTNode qualifiedName(String tableName, String colName) {
    ASTBuilder b = ASTBuilder
        .construct(HiveParser.DOT, ".")
        .add(
            ASTBuilder.construct(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL").add(
                HiveParser.Identifier, tableName)).add(HiveParser.Identifier, colName);
    return b.node();
  }

  public static ASTNode unqualifiedName(String colName) {
    ASTBuilder b = ASTBuilder.construct(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL").add(
        HiveParser.Identifier, colName);
    return b.node();
  }

  public static ASTNode where(ASTNode cond) {
    return ASTBuilder.construct(HiveParser.TOK_WHERE, "TOK_WHERE").add(cond).node();
  }

  public static ASTNode having(ASTNode cond) {
    return ASTBuilder.construct(HiveParser.TOK_HAVING, "TOK_HAVING").add(cond).node();
  }

  public static ASTNode limit(Object offset, Object limit) {
    return ASTBuilder.construct(HiveParser.TOK_LIMIT, "TOK_LIMIT")
        .add(HiveParser.Number, offset.toString())
        .add(HiveParser.Number, limit.toString()).node();
  }

  public static ASTNode selectExpr(ASTNode expr, String alias) {
    return ASTBuilder.construct(HiveParser.TOK_SELEXPR, "TOK_SELEXPR").add(expr)
        .add(HiveParser.Identifier, alias).node();
  }

  public static ASTNode literal(RexLiteral literal) {
    return literal(literal, false);
  }

  public static ASTNode literal(RexLiteral literal, boolean useTypeQualInLiteral) {
    Object val = null;
    int type = 0;
    SqlTypeName sqlType = literal.getType().getSqlTypeName();

    switch (sqlType) {
    case BINARY:
    case DATE:
    case TIME:
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_MONTH:
    case INTERVAL_SECOND:
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
      if (literal.getValue() == null) {
        return ASTBuilder.construct(HiveParser.TOK_NULL, "TOK_NULL").node();
      }
      break;
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
    case DOUBLE:
    case DECIMAL:
    case FLOAT:
    case REAL:
    case VARCHAR:
    case CHAR:
    case BOOLEAN:
      if (literal.getValue3() == null) {
        return ASTBuilder.construct(HiveParser.TOK_NULL, "TOK_NULL").node();
      }
    }

    switch (sqlType) {
    case TINYINT:
      if (useTypeQualInLiteral) {
        val = literal.getValue3() + "Y";
      } else {
        val = literal.getValue3();
      }
      type = HiveParser.IntegralLiteral;
      break;
    case SMALLINT:
      if (useTypeQualInLiteral) {
        val = literal.getValue3() + "S";
      } else {
        val = literal.getValue3();
      }
      type = HiveParser.IntegralLiteral;
      break;
    case INTEGER:
      val = literal.getValue3();
      type = HiveParser.IntegralLiteral;
      break;
    case BIGINT:
      if (useTypeQualInLiteral) {
        val = literal.getValue3() + "L";
      } else {
        val = literal.getValue3();
      }
      type = HiveParser.IntegralLiteral;
      break;
    case DOUBLE:
      val = literal.getValue3() + "D";
      type = HiveParser.NumberLiteral;
      break;
    case DECIMAL:
      val = literal.getValue3() + "BD";
      type = HiveParser.NumberLiteral;
      break;
    case FLOAT:
    case REAL:
      val = literal.getValue3();
      type = HiveParser.Number;
      break;
    case VARCHAR:
    case CHAR:
      val = literal.getValue3();
      String escapedVal = BaseSemanticAnalyzer.escapeSQLString(String.valueOf(val));
      type = HiveParser.StringLiteral;
      val = "'" + escapedVal + "'";
      break;
    case BOOLEAN:
      val = literal.getValue3();
      type = ((Boolean) val).booleanValue() ? HiveParser.KW_TRUE : HiveParser.KW_FALSE;
      break;
    case DATE:
      val = "'" + literal.getValueAs(DateString.class).toString() + "'";
      type = HiveParser.TOK_DATELITERAL;
      break;
    case TIME:
      val = "'" + literal.getValueAs(TimeString.class).toString() + "'";
      type = HiveParser.TOK_TIMESTAMPLITERAL;
      break;
    case TIMESTAMP:
      val = "'" + literal.getValueAs(TimestampString.class).toString() + "'";
      type = HiveParser.TOK_TIMESTAMPLITERAL;
      break;
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      // Calcite stores timestamp with local time-zone in UTC internally, thus
      // when we bring it back, we need to add the UTC suffix.
      val = "'" + literal.getValueAs(TimestampString.class).toString() + " UTC'";
      type = HiveParser.TOK_TIMESTAMPLOCALTZLITERAL;
      break;
    case INTERVAL_YEAR:
    case INTERVAL_MONTH:
    case INTERVAL_YEAR_MONTH: {
      type = HiveParser.TOK_INTERVAL_YEAR_MONTH_LITERAL;
      BigDecimal monthsBd = (BigDecimal) literal.getValue();
      HiveIntervalYearMonth intervalYearMonth = new HiveIntervalYearMonth(monthsBd.intValue());
      val = "'" + intervalYearMonth.toString() + "'";
    }
      break;
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND: {
      type = HiveParser.TOK_INTERVAL_DAY_TIME_LITERAL;
      BigDecimal millisBd = (BigDecimal) literal.getValue();

      // Calcite literal is in millis, convert to seconds
      BigDecimal secsBd = millisBd.divide(BigDecimal.valueOf(1000));
      HiveIntervalDayTime intervalDayTime = new HiveIntervalDayTime(secsBd);
      val = "'" + intervalDayTime.toString() + "'";
    }
      break;
    case NULL:
      type = HiveParser.TOK_NULL;
      break;

    //binary type should not be seen.
    case BINARY:
    default:
      throw new RuntimeException("Unsupported Type: " + sqlType);
    }

    return (ASTNode) ParseDriver.adaptor.create(type, String.valueOf(val));
  }

  ASTNode curr;

  public ASTNode node() {
    return curr;
  }

  public ASTBuilder add(int tokenType, String text) {
    ParseDriver.adaptor.addChild(curr, createAST(tokenType, text));
    return this;
  }

  public ASTBuilder add(ASTBuilder b) {
    ParseDriver.adaptor.addChild(curr, b.curr);
    return this;
  }

  public ASTBuilder add(ASTNode n) {
    if (n != null) {
      ParseDriver.adaptor.addChild(curr, n);
    }
    return this;
  }
}
