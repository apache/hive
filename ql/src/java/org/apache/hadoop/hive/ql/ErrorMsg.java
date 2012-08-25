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

package org.apache.hadoop.hive.ql;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ASTNodeOrigin;

/**
 * List of all error messages.
 * This list contains both compile time and run-time errors.
 **/

public enum ErrorMsg {
  // The error codes are Hive-specific and partitioned into the following ranges:
  // 10000 to 19999: Errors occuring during semantic analysis and compilation of the query.
  // 20000 to 29999: Runtime errors where Hive believes that retries are unlikely to succeed.
  // 30000 to 39999: Runtime errors which Hive thinks may be transient and retrying may succeed.
  // 40000 to 49999: Errors where Hive is unable to advise about retries.
  // In addition to the error code, ErrorMsg also has a SQLState field.
  // SQLStates are taken from Section 12.5 of ISO-9075.
  // See http://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt
  // Most will just rollup to the generic syntax error state of 42000, but
  // specific errors can override the that state.
  // See this page for how MySQL uses SQLState codes:
  // http://dev.mysql.com/doc/refman/5.0/en/connector-j-reference-error-sqlstates.html
  GENERIC_ERROR(40000, "Exception while processing"),

  INVALID_TABLE(10001, "Table not found", "42S02"),
  INVALID_COLUMN(10002, "Invalid column reference"),
  INVALID_INDEX(10003, "Invalid index"),
  INVALID_TABLE_OR_COLUMN(10004, "Invalid table alias or column reference"),
  AMBIGUOUS_TABLE_OR_COLUMN(10005, "Ambiguous table alias or column reference"),
  INVALID_PARTITION(10006, "Partition not found"),
  AMBIGUOUS_COLUMN(10007, "Ambiguous column reference"),
  AMBIGUOUS_TABLE_ALIAS(10008, "Ambiguous table alias"),
  INVALID_TABLE_ALIAS(10009, "Invalid table alias"),
  NO_TABLE_ALIAS(10010, "No table alias"),
  INVALID_FUNCTION(10011, "Invalid function"),
  INVALID_FUNCTION_SIGNATURE(10012, "Function argument type mismatch"),
  INVALID_OPERATOR_SIGNATURE(10013, "Operator argument type mismatch"),
  INVALID_ARGUMENT(10014, "Wrong arguments"),
  INVALID_ARGUMENT_LENGTH(10015, "Arguments length mismatch", "21000"),
  INVALID_ARGUMENT_TYPE(10016, "Argument type mismatch"),
  INVALID_JOIN_CONDITION_1(10017, "Both left and right aliases encountered in JOIN"),
  INVALID_JOIN_CONDITION_2(10018, "Neither left nor right aliases encountered in JOIN"),
  INVALID_JOIN_CONDITION_3(10019, "OR not supported in JOIN currently"),
  INVALID_TRANSFORM(10020, "TRANSFORM with other SELECT columns not supported"),
  DUPLICATE_GROUPBY_KEY(10021, "Repeated key in GROUP BY"),
  UNSUPPORTED_MULTIPLE_DISTINCTS(10022, "DISTINCT on different columns not supported" +
      " with skew in data"),
  NO_SUBQUERY_ALIAS(10023, "No alias for subquery"),
  NO_INSERT_INSUBQUERY(10024, "Cannot insert in a subquery. Inserting to table "),
  NON_KEY_EXPR_IN_GROUPBY(10025, "Expression not in GROUP BY key"),
  INVALID_XPATH(10026, "General . and [] operators are not supported"),
  INVALID_PATH(10027, "Invalid path"),
  ILLEGAL_PATH(10028, "Path is not legal"),
  INVALID_NUMERICAL_CONSTANT(10029, "Invalid numerical constant"),
  INVALID_ARRAYINDEX_CONSTANT(10030, "Non-constant expressions for array indexes not supported"),
  INVALID_MAPINDEX_CONSTANT(10031, "Non-constant expression for map indexes not supported"),
  INVALID_MAPINDEX_TYPE(10032, "MAP key type does not match index expression type"),
  NON_COLLECTION_TYPE(10033, "[] not valid on non-collection types"),
  SELECT_DISTINCT_WITH_GROUPBY(10034, "SELECT DISTINCT and GROUP BY can not be in the same query"),
  COLUMN_REPEATED_IN_PARTITIONING_COLS(10035, "Column repeated in partitioning columns"),
  DUPLICATE_COLUMN_NAMES(10036, "Duplicate column name:"),
  INVALID_BUCKET_NUMBER(10037, "Bucket number should be bigger than zero"),
  COLUMN_REPEATED_IN_CLUSTER_SORT(10038, "Same column cannot appear in CLUSTER BY and SORT BY"),
  SAMPLE_RESTRICTION(10039, "Cannot SAMPLE on more than two columns"),
  SAMPLE_COLUMN_NOT_FOUND(10040, "SAMPLE column not found"),
  NO_PARTITION_PREDICATE(10041, "No partition predicate found"),
  INVALID_DOT(10042, ". Operator is only supported on struct or list of struct types"),
  INVALID_TBL_DDL_SERDE(10043, "Either list of columns or a custom serializer should be specified"),
  TARGET_TABLE_COLUMN_MISMATCH(10044,
      "Cannot insert into target table because column number/types are different"),
  TABLE_ALIAS_NOT_ALLOWED(10045, "Table alias not allowed in sampling clause"),
  CLUSTERBY_DISTRIBUTEBY_CONFLICT(10046, "Cannot have both CLUSTER BY and DISTRIBUTE BY clauses"),
  ORDERBY_DISTRIBUTEBY_CONFLICT(10047, "Cannot have both ORDER BY and DISTRIBUTE BY clauses"),
  CLUSTERBY_SORTBY_CONFLICT(10048, "Cannot have both CLUSTER BY and SORT BY clauses"),
  ORDERBY_SORTBY_CONFLICT(10049, "Cannot have both ORDER BY and SORT BY clauses"),
  CLUSTERBY_ORDERBY_CONFLICT(10050, "Cannot have both CLUSTER BY and ORDER BY clauses"),
  NO_LIMIT_WITH_ORDERBY(10051, "In strict mode, if ORDER BY is specified, "
      + "LIMIT must also be specified"),
  NO_CARTESIAN_PRODUCT(10052, "In strict mode, cartesian product is not allowed. "
      + "If you really want to perform the operation, set hive.mapred.mode=nonstrict"),
  UNION_NOTIN_SUBQ(10053, "Top level UNION is not supported currently; "
      + "use a subquery for the UNION"),
  INVALID_INPUT_FORMAT_TYPE(10054, "Input format must implement InputFormat"),
  INVALID_OUTPUT_FORMAT_TYPE(10055, "Output Format must implement HiveOutputFormat, "
      + "otherwise it should be either IgnoreKeyTextOutputFormat or SequenceFileOutputFormat"),
  NO_VALID_PARTN(10056, "The query does not reference any valid partition. "
      + "To run this query, set hive.mapred.mode=nonstrict"),
  NO_OUTER_MAPJOIN(10057, "MAPJOIN cannot be performed with OUTER JOIN"),
  INVALID_MAPJOIN_HINT(10058, "Neither table specified as map-table"),
  INVALID_MAPJOIN_TABLE(10059, "Result of a union cannot be a map table"),
  NON_BUCKETED_TABLE(10060, "Sampling expression needed for non-bucketed table"),
  BUCKETED_NUMERATOR_BIGGER_DENOMINATOR(10061, "Numerator should not be bigger than "
      + "denominator in sample clause for table"),
  NEED_PARTITION_ERROR(10062, "Need to specify partition columns because the destination "
      + "table is partitioned"),
  CTAS_CTLT_COEXISTENCE(10063, "Create table command does not allow LIKE and AS-SELECT in "
      + "the same command"),
  LINES_TERMINATED_BY_NON_NEWLINE(10064, "LINES TERMINATED BY only supports "
      + "newline '\\n' right now"),
  CTAS_COLLST_COEXISTENCE(10065, "CREATE TABLE AS SELECT command cannot specify "
      + "the list of columns "
      + "for the target table"),
  CTLT_COLLST_COEXISTENCE(10066, "CREATE TABLE LIKE command cannot specify the list of columns for "
      + "the target table"),
  INVALID_SELECT_SCHEMA(10067, "Cannot derive schema from the select-clause"),
  CTAS_PARCOL_COEXISTENCE(10068, "CREATE-TABLE-AS-SELECT does not support "
      + "partitioning in the target table "),
  CTAS_MULTI_LOADFILE(10069, "CREATE-TABLE-AS-SELECT results in multiple file load"),
  CTAS_EXTTBL_COEXISTENCE(10070, "CREATE-TABLE-AS-SELECT cannot create external table"),
  INSERT_EXTERNAL_TABLE(10071, "Inserting into a external table is not allowed"),
  DATABASE_NOT_EXISTS(10072, "Database does not exist:"),
  TABLE_ALREADY_EXISTS(10073, "Table already exists:", "42S02"),
  COLUMN_ALIAS_ALREADY_EXISTS(10074, "Column alias already exists:", "42S02"),
  UDTF_MULTIPLE_EXPR(10075, "Only a single expression in the SELECT clause is "
      + "supported with UDTF's"),
  UDTF_REQUIRE_AS(10076, "UDTF's require an AS clause"),
  UDTF_NO_GROUP_BY(10077, "GROUP BY is not supported with a UDTF in the SELECT clause"),
  UDTF_NO_SORT_BY(10078, "SORT BY is not supported with a UDTF in the SELECT clause"),
  UDTF_NO_CLUSTER_BY(10079, "CLUSTER BY is not supported with a UDTF in the SELECT clause"),
  UDTF_NO_DISTRIBUTE_BY(10080, "DISTRUBTE BY is not supported with a UDTF in the SELECT clause"),
  UDTF_INVALID_LOCATION(10081, "UDTF's are not supported outside the SELECT clause, nor nested "
      + "in expressions"),
  UDTF_LATERAL_VIEW(10082, "UDTF's cannot be in a select expression when there is a lateral view"),
  UDTF_ALIAS_MISMATCH(10083, "The number of aliases supplied in the AS clause does not match the "
      + "number of columns output by the UDTF"),
  UDF_STATEFUL_INVALID_LOCATION(10084, "Stateful UDF's can only be invoked in the SELECT list"),
  LATERAL_VIEW_WITH_JOIN(10085, "JOIN with a LATERAL VIEW is not supported"),
  LATERAL_VIEW_INVALID_CHILD(10086, "LATERAL VIEW AST with invalid child"),
  OUTPUT_SPECIFIED_MULTIPLE_TIMES(10087, "The same output cannot be present multiple times: "),
  INVALID_AS(10088, "AS clause has an invalid number of aliases"),
  VIEW_COL_MISMATCH(10089, "The number of columns produced by the SELECT clause does not match the "
      + "number of column names specified by CREATE VIEW"),
  DML_AGAINST_VIEW(10090, "A view cannot be used as target table for LOAD or INSERT"),
  ANALYZE_VIEW(10091, "ANALYZE is not supported for views"),
  VIEW_PARTITION_TOTAL(10092, "At least one non-partitioning column must be present in view"),
  VIEW_PARTITION_MISMATCH(10093, "Rightmost columns in view output do not match "
      + "PARTITIONED ON clause"),
  PARTITION_DYN_STA_ORDER(10094, "Dynamic partition cannot be the parent of a static partition"),
  DYNAMIC_PARTITION_DISABLED(10095, "Dynamic partition is disabled. Either enable it by setting "
      + "hive.exec.dynamic.partition=true or specify partition column values"),
  DYNAMIC_PARTITION_STRICT_MODE(10096, "Dynamic partition strict mode requires at least one "
      + "static partition column. To turn this off set hive.exec.dynamic.partition.mode=nonstrict"),
  DYNAMIC_PARTITION_MERGE(10097, "Dynamic partition does not support merging using "
      + "non-CombineHiveInputFormat. Please check your hive.input.format setting and "
      + "make sure your Hadoop version support CombineFileInputFormat"),
  NONEXISTPARTCOL(10098, "Non-Partition column appears in the partition specification: "),
  UNSUPPORTED_TYPE(10099, "DATE and DATETIME types aren't supported yet. Please use "
      + "TIMESTAMP instead"),
  CREATE_NON_NATIVE_AS(10100, "CREATE TABLE AS SELECT cannot be used for a non-native table"),
  LOAD_INTO_NON_NATIVE(10101, "A non-native table cannot be used as target for LOAD"),
  LOCKMGR_NOT_SPECIFIED(10102, "Lock manager not specified correctly, set hive.lock.manager"),
  LOCKMGR_NOT_INITIALIZED(10103, "Lock manager could not be initialized, check hive.lock.manager "),
  LOCK_CANNOT_BE_ACQUIRED(10104, "Locks on the underlying objects cannot be acquired. "
      + "retry after some time"),
  ZOOKEEPER_CLIENT_COULD_NOT_BE_INITIALIZED(10105, "Check hive.zookeeper.quorum "
      + "and hive.zookeeper.client.port"),
  OVERWRITE_ARCHIVED_PART(10106, "Cannot overwrite an archived partition. " +
      "Unarchive before running this command"),
  ARCHIVE_METHODS_DISABLED(10107, "Archiving methods are currently disabled. " +
      "Please see the Hive wiki for more information about enabling archiving"),
  ARCHIVE_ON_MULI_PARTS(10108, "ARCHIVE can only be run on a single partition"),
  UNARCHIVE_ON_MULI_PARTS(10109, "ARCHIVE can only be run on a single partition"),
  ARCHIVE_ON_TABLE(10110, "ARCHIVE can only be run on partitions"),
  RESERVED_PART_VAL(10111, "Partition value contains a reserved substring"),
  HOLD_DDLTIME_ON_NONEXIST_PARTITIONS(10112, "HOLD_DDLTIME hint cannot be applied to dynamic " +
                                      "partitions or non-existent partitions"),
  OFFLINE_TABLE_OR_PARTITION(10113, "Query against an offline table or partition"),
  OUTERJOIN_USES_FILTERS(10114, "The query results could be wrong. " +
                         "Turn on hive.outerjoin.supports.filters"),
  NEED_PARTITION_SPECIFICATION(10115, "Table is partitioned and partition specification is needed"),
  INVALID_METADATA(10116, "The metadata file could not be parsed "),
  NEED_TABLE_SPECIFICATION(10117, "Table name could be determined; It should be specified "),
  PARTITION_EXISTS(10118, "Partition already exists"),
  TABLE_DATA_EXISTS(10119, "Table exists and contains data files"),
  INCOMPATIBLE_SCHEMA(10120, "The existing table is not compatible with the import spec. "),
  EXIM_FOR_NON_NATIVE(10121, "Export/Import cannot be done for a non-native table. "),
  INSERT_INTO_BUCKETIZED_TABLE(10122, "Bucketized tables do not support INSERT INTO:"),
  NO_COMPARE_BIGINT_STRING(10123, "In strict mode, comparing bigints and strings is not allowed, "
      + "it may result in a loss of precision. "
      + "If you really want to perform the operation, set hive.mapred.mode=nonstrict"),
  NO_COMPARE_BIGINT_DOUBLE(10124, "In strict mode, comparing bigints and doubles is not allowed, "
      + "it may result in a loss of precision. "
      + "If you really want to perform the operation, set hive.mapred.mode=nonstrict"),
  PARTSPEC_DIFFER_FROM_SCHEMA(10125, "Partition columns in partition specification are "
      + "not the same as that defined in the table schema. "
      + "The names and orders have to be exactly the same."),
  PARTITION_COLUMN_NON_PRIMITIVE(10126, "Partition column must be of primitive type."),
  INSERT_INTO_DYNAMICPARTITION_IFNOTEXISTS(10127,
      "Dynamic partitions do not support IF NOT EXISTS. Specified partitions with value :"),
  UDAF_INVALID_LOCATION(10128, "Not yet supported place for UDAF"),
  DROP_PARTITION_NON_STRING_PARTCOLS_NONEQUALITY(10129,
    "Drop partitions for a non string partition columns is not allowed using non-equality"),
  NUM_BUCKETS_CHANGE_NOT_ALLOWED(10130, "Changing the number of buckets for a " +
    "partitioned table is not allowed. It may lead to wrong results for " +
    "older partitions"),
  ALTER_COMMAND_FOR_VIEWS(10131, "To alter a view you need to use the ALTER VIEW command."),
  ALTER_COMMAND_FOR_TABLES(10132, "To alter a base table you need to use the ALTER TABLE command."),
  ALTER_VIEW_DISALLOWED_OP(10133, "Cannot use this form of ALTER on a view"),
  ALTER_TABLE_NON_NATIVE(10134, "ALTER TABLE cannot be used for a non-native table"),
  SORTMERGE_MAPJOIN_FAILED(10135,
      "Sort merge bucketed join could not be performed. " +
      "If you really want to perform the operation, either set " +
      "hive.optimize.bucketmapjoin.sortedmerge=false, or set " +
      "hive.enforce.sortmergebucketmapjoin=false."),
  BUCKET_MAPJOIN_NOT_POSSIBLE(10136,
    "Bucketed mapjoin cannot be performed. " +
    "This can be due to multiple reasons: " +
    " . Join columns dont match bucketed columns. " +
    " . Number of buckets are not a multiple of each other. " +
    "If you really want to perform the operation, either remove the " +
    "mapjoin hint from your query or set hive.enforce.bucketmapjoin to false."),

  EXPRESSIONS_NOT_ALLOWED_CLUSTERBY(10137,
    "Expressions are not allowed in a cluster by clause. Use a column alias instead"),
  EXPRESSIONS_NOT_ALLOWED_DISTRIBUTEBY(10138,
    "Expressions are not allowed in a distribute by clause. Use a column alias instead"),
  EXPRESSIONS_NOT_ALLOWED_ORDERBY(10139,
    "Expressions are not allowed in an order by clause. Use a column alias instead"),
  EXPRESSIONS_NOT_ALLOWED_SORTBY(10140,
    "Expressions are not allowed in a sort by clause. Use a column alias instead"),

  BUCKETED_TABLE_METADATA_INCORRECT(10141,
   "Bucketed table metadata is not correct. " +
    "Fix the metadata or don't use bucketed mapjoin, by setting " +
    "hive.enforce.bucketmapjoin to false."),

  CREATE_SKEWED_TABLE_NO_COLUMN_NAME(10200, "No skewed column name."),
  CREATE_SKEWED_TABLE_NO_COLUMN_VALUE(10201, "No skewed values."),
  CREATE_SKEWED_TABLE_DUPLICATE_COLUMN_NAMES(10202,
      "Duplicate skewed column name:"),
  CREATE_SKEWED_TABLE_INVALID_COLUMN(10203,
      "Invalid skewed column name:"),
  CREATE_SKEWED_TABLE_SKEWED_COL_NAME_VALUE_MISMATCH_1(10204,
      "Skewed column name is empty but skewed value is not."),
  CREATE_SKEWED_TABLE_SKEWED_COL_NAME_VALUE_MISMATCH_2(10205,
      "Skewed column value is empty but skewed name is not."),
  CREATE_SKEWED_TABLE_SKEWED_COL_NAME_VALUE_MISMATCH_3(10206,
      "The number of skewed column names and the number of " +
      "skewed column values are different: "),
  ALTER_TABLE_NOT_ALLOWED_RENAME_SKEWED_COLUMN(10207,
          " is a skewed column. It's not allowed to rename skewed column."),
  HIVE_INTERNAL_DDL_LIST_BUCKETING_DISABLED(10208,
              "List Bucketing DDL is not allowed to use since feature is not completed yet."),

  SCRIPT_INIT_ERROR(20000, "Unable to initialize custom script."),
  SCRIPT_IO_ERROR(20001, "An error occurred while reading or writing to your custom script. "
      + "It may have crashed with an error."),
  SCRIPT_GENERIC_ERROR(20002, "Hive encountered some unknown error while "
      + "running your custom script."),
  SCRIPT_CLOSING_ERROR(20003, "An error occurred when trying to close the Operator " +
      "running your custom script."),

  STATSPUBLISHER_NOT_OBTAINED(30000, "StatsPublisher cannot be obtained. " +
    "There was a error to retrieve the StatsPublisher, and retrying " +
    "might help. If you dont want the query to fail because accurate statistics " +
    "could not be collected, set hive.stats.reliable=false"),
  STATSPUBLISHER_INITIALIZATION_ERROR(30001, "StatsPublisher cannot be initialized. " +
    "There was a error in the initialization of StatsPublisher, and retrying " +
    "might help. If you dont want the query to fail because accurate statistics " +
    "could not be collected, set hive.stats.reliable=false"),
  STATSPUBLISHER_CONNECTION_ERROR(30002, "StatsPublisher cannot be connected to." +
    "There was a error while connecting to the StatsPublisher, and retrying " +
    "might help. If you dont want the query to fail because accurate statistics " +
    "could not be collected, set hive.stats.reliable=false"),
  STATSPUBLISHER_PUBLISHING_ERROR(30003, "Error in publishing stats. There was an " +
    "error in publishing stats via StatsPublisher, and retrying " +
    "might help. If you dont want the query to fail because accurate statistics " +
    "could not be collected, set hive.stats.reliable=false"),
  STATSPUBLISHER_CLOSING_ERROR(30004, "StatsPublisher cannot be closed." +
    "There was a error while closing the StatsPublisher, and retrying " +
    "might help. If you dont want the query to fail because accurate statistics " +
    "could not be collected, set hive.stats.reliable=false"),

    ;

  private int errorCode;
  private String mesg;
  private String sqlState;

  private static final char SPACE = ' ';
  private static final Pattern ERROR_MESSAGE_PATTERN = Pattern.compile(".*Line [0-9]+:[0-9]+ (.*)");
  private static final Pattern ERROR_CODE_PATTERN =
    Pattern.compile("HiveException:\\s+\\[Error ([0-9]+)\\]: (.*)");
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
   * Given an error message string, returns the ErrorMsg object associated with it.
   * @param mesg An error message string
   * @return ErrorMsg
   */
  public static ErrorMsg getErrorMsg(String mesg) {
    if (mesg == null) {
      return GENERIC_ERROR;
    }

    // first see if there is a direct match
    ErrorMsg errorMsg = mesgToErrorMsgMap.get(mesg);
    if (errorMsg != null) {
      return errorMsg;
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
        return errorMsg;
      }

      int lastSpace = truncatedMesg.lastIndexOf(SPACE);
      if (lastSpace == -1) {
        break;
      }

      // hack off the last word and try again
      truncatedMesg = truncatedMesg.substring(0, lastSpace).trim();
    }

    return GENERIC_ERROR;
  }

  /**
   * Given an error code, returns the ErrorMsg object associated with it.
   * @param errorCode An error code
   * @return ErrorMsg
   */
  public static ErrorMsg getErrorMsg(int errorCode) {
    for (ErrorMsg errorMsg : values()) {
      if (errorMsg.getErrorCode() == errorCode) {
        return errorMsg;
      }
    }
    return null;
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
    ErrorMsg error = getErrorMsg(mesg);
    return error.getSQLState();
  }

  private ErrorMsg(int errorCode, String mesg) {
    // 42000 is the generic SQLState for syntax error.
    this(errorCode, mesg, "42000");
  }

  private ErrorMsg(int errorCode, String mesg, String sqlState) {
    this.errorCode = errorCode;
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

  public String getErrorCodedMsg() {
    return "[Error " + errorCode + "]: " + mesg;
  }

  public static Pattern getErrorCodePattern() {
    return ERROR_CODE_PATTERN;
  }

  public String getMsg() {
    return mesg;
  }

  public String getSQLState() {
    return sqlState;
  }

  public int getErrorCode() {
    return errorCode;
  }
}
