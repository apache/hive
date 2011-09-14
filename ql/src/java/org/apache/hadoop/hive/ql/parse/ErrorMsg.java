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

package org.apache.hadoop.hive.ql.parse;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;

/**
 * List of error messages thrown by the parser.
 **/

public enum ErrorMsg {
  // SQLStates are taken from Section 12.5 of ISO-9075.
  // See http://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt
  // Most will just rollup to the generic syntax error state of 42000, but
  // specific errors can override the that state.
  // See this page for how MySQL uses SQLState codes:
  // http://dev.mysql.com/doc/refman/5.0/en/connector-j-reference-error-sqlstates.html

  GENERIC_ERROR("Exception while processing"),
  INVALID_TABLE("Table not found", "42S02"),
  INVALID_COLUMN("Invalid column reference"),
  INVALID_INDEX("Invalid index"),
  INVALID_TABLE_OR_COLUMN("Invalid table alias or column reference"),
  AMBIGUOUS_TABLE_OR_COLUMN("Ambiguous table alias or column reference"),
  INVALID_PARTITION("Partition not found"),
  AMBIGUOUS_COLUMN("Ambiguous column reference"),
  AMBIGUOUS_TABLE_ALIAS("Ambiguous table alias"),
  INVALID_TABLE_ALIAS("Invalid table alias"),
  NO_TABLE_ALIAS("No table alias"),
  INVALID_FUNCTION("Invalid function"),
  INVALID_FUNCTION_SIGNATURE("Function argument type mismatch"),
  INVALID_OPERATOR_SIGNATURE("Operator argument type mismatch"),
  INVALID_ARGUMENT("Wrong arguments"),
  INVALID_ARGUMENT_LENGTH("Arguments length mismatch", "21000"),
  INVALID_ARGUMENT_TYPE("Argument type mismatch"),
  INVALID_JOIN_CONDITION_1("Both left and right aliases encountered in JOIN"),
  INVALID_JOIN_CONDITION_2("Neither left nor right aliases encountered in JOIN"),
  INVALID_JOIN_CONDITION_3("OR not supported in JOIN currently"),
  INVALID_TRANSFORM("TRANSFORM with other SELECT columns not supported"),
  DUPLICATE_GROUPBY_KEY("Repeated key in GROUP BY"),
  UNSUPPORTED_MULTIPLE_DISTINCTS("DISTINCT on different columns not supported with skew in data"),
  NO_SUBQUERY_ALIAS("No alias for subquery"),
  NO_INSERT_INSUBQUERY("Cannot insert in a subquery. Inserting to table "),
  NON_KEY_EXPR_IN_GROUPBY("Expression not in GROUP BY key"),
  INVALID_XPATH("General . and [] operators are not supported"),
  INVALID_PATH("Invalid path"), ILLEGAL_PATH("Path is not legal"),
  INVALID_NUMERICAL_CONSTANT("Invalid numerical constant"),
  INVALID_ARRAYINDEX_CONSTANT("Non-constant expressions for array indexes not supported"),
  INVALID_MAPINDEX_CONSTANT("Non-constant expression for map indexes not supported"),
  INVALID_MAPINDEX_TYPE("MAP key type does not match index expression type"),
  NON_COLLECTION_TYPE("[] not valid on non-collection types"),
  SELECT_DISTINCT_WITH_GROUPBY("SELECT DISTINCT and GROUP BY can not be in the same query"),
  COLUMN_REPEATED_IN_PARTITIONING_COLS("Column repeated in partitioning columns"),
  DUPLICATE_COLUMN_NAMES("Duplicate column name:"),
  INVALID_BUCKET_NUMBER("Bucket number should be bigger than zero"),
  COLUMN_REPEATED_IN_CLUSTER_SORT("Same column cannot appear in CLUSTER BY and SORT BY"),
  SAMPLE_RESTRICTION("Cannot SAMPLE on more than two columns"),
  SAMPLE_COLUMN_NOT_FOUND("SAMPLE column not found"),
  NO_PARTITION_PREDICATE("No partition predicate found"),
  INVALID_DOT(". Operator is only supported on struct or list of struct types"),
  INVALID_TBL_DDL_SERDE("Either list of columns or a custom serializer should be specified"),
  TARGET_TABLE_COLUMN_MISMATCH(
      "Cannot insert into target table because column number/types are different"),
  TABLE_ALIAS_NOT_ALLOWED("Table alias not allowed in sampling clause"),
  CLUSTERBY_DISTRIBUTEBY_CONFLICT("Cannot have both CLUSTER BY and DISTRIBUTE BY clauses"),
  ORDERBY_DISTRIBUTEBY_CONFLICT("Cannot have both ORDER BY and DISTRIBUTE BY clauses"),
  CLUSTERBY_SORTBY_CONFLICT("Cannot have both CLUSTER BY and SORT BY clauses"),
  ORDERBY_SORTBY_CONFLICT("Cannot have both ORDER BY and SORT BY clauses"),
  CLUSTERBY_ORDERBY_CONFLICT("Cannot have both CLUSTER BY and ORDER BY clauses"),
  NO_LIMIT_WITH_ORDERBY("In strict mode, if ORDER BY is specified, LIMIT must also be specified"),
  NO_CARTESIAN_PRODUCT("In strict mode, cartesian product is not allowed. "
      + "If you really want to perform the operation, set hive.mapred.mode=nonstrict"),
  UNION_NOTIN_SUBQ("Top level UNION is not supported currently; use a subquery for the UNION"),
  INVALID_INPUT_FORMAT_TYPE("Input format must implement InputFormat"),
  INVALID_OUTPUT_FORMAT_TYPE("Output Format must implement HiveOutputFormat, "
      + "otherwise it should be either IgnoreKeyTextOutputFormat or SequenceFileOutputFormat"),
  NO_VALID_PARTN("The query does not reference any valid partition. "
      + "To run this query, set hive.mapred.mode=nonstrict"),
  NO_OUTER_MAPJOIN("MAPJOIN cannot be performed with OUTER JOIN"),
  INVALID_MAPJOIN_HINT("Neither table specified as map-table"),
  INVALID_MAPJOIN_TABLE("Result of a union cannot be a map table"),
  NON_BUCKETED_TABLE("Sampling expression needed for non-bucketed table"),
  BUCKETED_NUMBERATOR_BIGGER_DENOMINATOR("Numberator should not be bigger than "
      + "denaminator in sample clause for table"),
  NEED_PARTITION_ERROR("Need to specify partition columns because the destination "
      + "table is partitioned"),
  CTAS_CTLT_COEXISTENCE("Create table command does not allow LIKE and AS-SELECT in "
      + "the same command"),
  LINES_TERMINATED_BY_NON_NEWLINE("LINES TERMINATED BY only supports newline '\\n' right now"),
  CTAS_COLLST_COEXISTENCE("CREATE TABLE AS SELECT command cannot specify the list of columns "
      + "for the target table"),
  CTLT_COLLST_COEXISTENCE("CREATE TABLE LIKE command cannot specify the list of columns for "
      + "the target table"),
  INVALID_SELECT_SCHEMA("Cannot derive schema from the select-clause"),
  CTAS_PARCOL_COEXISTENCE("CREATE-TABLE-AS-SELECT does not support partitioning in the target "
      + "table"),
  CTAS_MULTI_LOADFILE("CREATE-TABLE-AS-SELECT results in multiple file load"),
  CTAS_EXTTBL_COEXISTENCE("CREATE-TABLE-AS-SELECT cannot create external table"),
  TABLE_ALREADY_EXISTS("Table already exists:", "42S02"),
  COLUMN_ALIAS_ALREADY_EXISTS("Column alias already exists:", "42S02"),
  UDTF_MULTIPLE_EXPR("Only a single expression in the SELECT clause is supported with UDTF's"),
  UDTF_REQUIRE_AS("UDTF's require an AS clause"),
  UDTF_NO_GROUP_BY("GROUP BY is not supported with a UDTF in the SELECT clause"),
  UDTF_NO_SORT_BY("SORT BY is not supported with a UDTF in the SELECT clause"),
  UDTF_NO_CLUSTER_BY("CLUSTER BY is not supported with a UDTF in the SELECT clause"),
  UDTF_NO_DISTRIBUTE_BY("DISTRUBTE BY is not supported with a UDTF in the SELECT clause"),
  UDTF_INVALID_LOCATION("UDTF's are not supported outside the SELECT clause, nor nested "
      + "in expressions"),
  UDTF_LATERAL_VIEW("UDTF's cannot be in a select expression when there is a lateral view"),
  UDTF_ALIAS_MISMATCH("The number of aliases supplied in the AS clause does not match the "
      + "number of columns output by the UDTF"),
  UDF_STATEFUL_INVALID_LOCATION("Stateful UDF's can only be invoked in the SELECT list"),
  LATERAL_VIEW_WITH_JOIN("JOIN with a LATERAL VIEW is not supported"),
  LATERAL_VIEW_INVALID_CHILD("LATERAL VIEW AST with invalid child"),
  OUTPUT_SPECIFIED_MULTIPLE_TIMES("The same output cannot be present multiple times: "),
  INVALID_AS("AS clause has an invalid number of aliases"),
  VIEW_COL_MISMATCH("The number of columns produced by the SELECT clause does not match the "
      + "number of column names specified by CREATE VIEW"),
  DML_AGAINST_VIEW("A view cannot be used as target table for LOAD or INSERT"),
  ANALYZE_VIEW("ANALYZE is not supported for views"),
  VIEW_PARTITION_TOTAL("At least one non-partitioning column must be present in view"),
  VIEW_PARTITION_MISMATCH("Rightmost columns in view output do not match PARTITIONED ON clause"),
  PARTITION_DYN_STA_ORDER("Dynamic partition cannot be the parent of a static partition"),
  DYNAMIC_PARTITION_DISABLED("Dynamic partition is disabled. Either enable it by setting "
      + "hive.exec.dynamic.partition=true or specify partition column values"),
  DYNAMIC_PARTITION_STRICT_MODE("Dynamic partition strict mode requires at least one "
      + "static partition column. To turn this off set hive.exec.dynamic.partition.mode=nonstrict"),
  DYNAMIC_PARTITION_MERGE("Dynamic partition does not support merging using non-CombineHiveInputFormat"
      + "Please check your hive.input.format setting and make sure your Hadoop version support "
      + "CombineFileInputFormat"),
  NONEXISTPARTCOL("Non-Partition column appears in the partition specification: "),
  UNSUPPORTED_TYPE("DATE and DATETIME types aren't supported yet. Please use "
      + "TIMESTAMP instead"),
  CREATE_NON_NATIVE_AS("CREATE TABLE AS SELECT cannot be used for a non-native table"),
  LOAD_INTO_NON_NATIVE("A non-native table cannot be used as target for LOAD"),
  LOCKMGR_NOT_SPECIFIED("Lock manager not specified correctly, set hive.lock.manager"),
  LOCKMGR_NOT_INITIALIZED("Lock manager could not be initialized, check hive.lock.manager "),
  LOCK_CANNOT_BE_ACQUIRED("Locks on the underlying objects cannot be acquired. retry after some time"),
  ZOOKEEPER_CLIENT_COULD_NOT_BE_INITIALIZED("Check hive.zookeeper.quorum and hive.zookeeper.client.port"),
  OVERWRITE_ARCHIVED_PART("Cannot overwrite an archived partition. " +
      "Unarchive before running this command"),
  ARCHIVE_METHODS_DISABLED("Archiving methods are currently disabled. " +
      "Please see the Hive wiki for more information about enabling archiving"),
  ARCHIVE_ON_MULI_PARTS("ARCHIVE can only be run on a single partition"),
  UNARCHIVE_ON_MULI_PARTS("ARCHIVE can only be run on a single partition"),
  ARCHIVE_ON_TABLE("ARCHIVE can only be run on partitions"),
  RESERVED_PART_VAL("Partition value contains a reserved substring"),
  HOLD_DDLTIME_ON_NONEXIST_PARTITIONS("HOLD_DDLTIME hint cannot be applied to dynamic " +
  		"partitions or non-existent partitions"),
  OFFLINE_TABLE_OR_PARTITION("Query against an offline table or partition"),
  OUTERJOIN_USES_FILTERS("The query results could be wrong. " +
  		"Turn on hive.outerjoin.supports.filters"),
  NEED_PARTITION_SPECIFICATION("Table is partitioned and partition specification is needed"),
  INVALID_METADATA("The metadata file could not be parsed "),
  NEED_TABLE_SPECIFICATION("Table name could be determined; It should be specified "),
  PARTITION_EXISTS("Partition already exists"),
  TABLE_DATA_EXISTS("Table exists and contains data files"),
  INCOMPATIBLE_SCHEMA("The existing table is not compatible with the import spec. "),
  EXIM_FOR_NON_NATIVE("Export/Import cannot be done for a non-native table. "),
  INSERT_INTO_BUCKETIZED_TABLE("Bucketized tables do not support INSERT INTO:"),
  NO_COMPARE_BIGINT_STRING("In strict mode, comparing bigints and strings is not allowed, "
      + "it may result in a loss of precision. "
      + "If you really want to perform the operation, set hive.mapred.mode=nonstrict"),
  NO_COMPARE_BIGINT_DOUBLE("In strict mode, comparing bigints and doubles is not allowed, "
      + "it may result in a loss of precision. "
      + "If you really want to perform the operation, set hive.mapred.mode=nonstrict"),
      FUNCTIONS_ARE_NOT_SUPPORTED_IN_ORDER_BY("functions are not supported in order by"),
      ;

  private String mesg;
  private String sqlState;

  private static final char SPACE = ' ';
  private static final Pattern ERROR_MESSAGE_PATTERN = Pattern.compile(".*line [0-9]+:[0-9]+ (.*)");
  private static Map<String, ErrorMsg> mesgToErrorMsgMap = new HashMap<String, ErrorMsg>();
  private static int minMesgLength = -1;

  static {
    for (ErrorMsg errorMsg : values()) {
      mesgToErrorMsgMap.put(errorMsg.getMsg().trim(), errorMsg);

      int length = errorMsg.getMsg().trim().length();
      if (minMesgLength == -1 || length < minMesgLength) {
        minMesgLength = length;
      }
    }
  }

  /**
   * For a given error message string, searches for a <code>ErrorMsg</code> enum
   * that appears to be a match. If an match is found, returns the
   * <code>SQLState</code> associated with the <code>ErrorMsg</code>. If a match
   * is not found or <code>ErrorMsg</code> has no <code>SQLState</code>, returns
   * the <code>SQLState</code> bound to the <code>GENERIC_ERROR</code>
   * <code>ErrorMsg</code>.
   *
   * @param mesg
   *          An error message string
   * @return SQLState
   */
  public static String findSQLState(String mesg) {

    if (mesg == null) {
      return GENERIC_ERROR.getSQLState();
    }

    // first see if there is a direct match
    ErrorMsg errorMsg = mesgToErrorMsgMap.get(mesg);
    if (errorMsg != null) {
      if (errorMsg.getSQLState() != null) {
        return errorMsg.getSQLState();
      } else {
        return GENERIC_ERROR.getSQLState();
      }
    }

    // if not see if the mesg follows type of format, which is typically the
    // case:
    // line 1:14 Table not found table_name
    String truncatedMesg = mesg.trim();
    Matcher match = ERROR_MESSAGE_PATTERN.matcher(mesg);
    if (match.matches()) {
      truncatedMesg = match.group(1);
    }

    // appends might exist after the root message, so strip tokens off until we
    // match
    while (truncatedMesg.length() > minMesgLength) {
      errorMsg = mesgToErrorMsgMap.get(truncatedMesg.trim());
      if (errorMsg != null) {
        if (errorMsg.getSQLState() != null) {
          return errorMsg.getSQLState();
        } else {
          return GENERIC_ERROR.getSQLState();
        }
      }

      int lastSpace = truncatedMesg.lastIndexOf(SPACE);
      if (lastSpace == -1) {
        break;
      }

      // hack off the last word and try again
      truncatedMesg = truncatedMesg.substring(0, lastSpace).trim();
    }

    return GENERIC_ERROR.getSQLState();
  }

  ErrorMsg(String mesg) {
    // 42000 is the generic SQLState for syntax error.
    this(mesg, "42000");
  }

  ErrorMsg(String mesg, String sqlState) {
    this.mesg = mesg;
    this.sqlState = sqlState;
  }

  private static int getLine(ASTNode tree) {
    if (tree.getChildCount() == 0) {
      return tree.getToken().getLine();
    }

    return getLine((ASTNode) tree.getChild(0));
  }

  private static int getCharPositionInLine(ASTNode tree) {
    if (tree.getChildCount() == 0) {
      return tree.getToken().getCharPositionInLine();
    }

    return getCharPositionInLine((ASTNode) tree.getChild(0));
  }

  // Dirty hack as this will throw away spaces and other things - find a better
  // way!
  public static String getText(ASTNode tree) {
    if (tree.getChildCount() == 0) {
      return tree.getText();
    }
    return getText((ASTNode) tree.getChild(tree.getChildCount() - 1));
  }

  public String getMsg(ASTNode tree) {
    StringBuilder sb = new StringBuilder();
    renderPosition(sb, tree);
    sb.append(" ");
    sb.append(mesg);
    sb.append(" '");
    sb.append(getText(tree));
    sb.append("'");
    renderOrigin(sb, tree.getOrigin());
    return sb.toString();
  }

  public static void renderOrigin(StringBuilder sb, ASTNodeOrigin origin) {
    while (origin != null) {
      sb.append(" in definition of ");
      sb.append(origin.getObjectType());
      sb.append(" ");
      sb.append(origin.getObjectName());
      sb.append(" [");
      sb.append(HiveUtils.LINE_SEP);
      sb.append(origin.getObjectDefinition());
      sb.append(HiveUtils.LINE_SEP);
      sb.append("] used as ");
      sb.append(origin.getUsageAlias());
      sb.append(" at ");
      ASTNode usageNode = origin.getUsageNode();
      renderPosition(sb, usageNode);
      origin = usageNode.getOrigin();
    }
  }

  private static void renderPosition(StringBuilder sb, ASTNode tree) {
    sb.append("Line ");
    sb.append(getLine(tree));
    sb.append(":");
    sb.append(getCharPositionInLine(tree));
  }

  public String getMsg(Tree tree) {
    return getMsg((ASTNode) tree);
  }

  public String getMsg(ASTNode tree, String reason) {
    return getMsg(tree) + ": " + reason;
  }

  public String getMsg(Tree tree, String reason) {
    return getMsg((ASTNode) tree, reason);
  }

  public String getMsg(String reason) {
    return mesg + " " + reason;
  }

  public String getMsg() {
    return mesg;
  }

  public String getSQLState() {
    return sqlState;
  }
}
