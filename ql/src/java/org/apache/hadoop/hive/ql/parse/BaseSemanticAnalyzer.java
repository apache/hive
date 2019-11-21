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

package org.apache.hadoop.hive.ql.parse;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.cache.results.CacheUsage;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.listbucketingpruner.ListBucketingPrunerUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.util.DirectionUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.security.alias.AbstractJavaKeyStoreProvider;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

/**
 * BaseSemanticAnalyzer.
 *
 */
public abstract class BaseSemanticAnalyzer {
  protected static final Logger STATIC_LOG = LoggerFactory.getLogger(BaseSemanticAnalyzer.class.getName());
  // Assumes one instance of this + single-threaded compilation for each query.
  protected final Hive db;
  protected final HiveConf conf;
  protected final QueryState queryState;
  protected List<Task<?>> rootTasks;
  protected FetchTask fetchTask;
  protected final Logger LOG;
  protected final LogHelper console;

  protected CompilationOpContext cContext;
  protected Context ctx;
  protected Map<String, String> idToTableNameMap;
  protected QueryProperties queryProperties;

  /**
   * A set of FileSinkOperators being written to in an ACID compliant way.  We need to remember
   * them here because when we build them we don't yet know the write id.  We need to go
   * back and set it once we actually start running the query.
   * This also contains insert-only sinks.
   */
  protected Set<FileSinkDesc> acidFileSinks = new HashSet<FileSinkDesc>();

  // whether any ACID table or Insert-only (mm) table is involved in a query
  // They both require DbTxnManager and both need to recordValidTxns when acquiring locks in Driver
  protected boolean transactionalInQuery;

  protected HiveTxnManager txnManager;

  /**
   * ReadEntities that are passed to the hooks.
   */
  protected Set<ReadEntity> inputs;
  /**
   * List of WriteEntities that are passed to the hooks.
   */
  protected Set<WriteEntity> outputs;
  /**
   * Lineage information for the query.
   */
  protected LineageInfo linfo;
  protected TableAccessInfo tableAccessInfo;
  protected ColumnAccessInfo columnAccessInfo;

  protected CacheUsage cacheUsage;

  /**
   * Columns accessed by updates
   */
  protected ColumnAccessInfo updateColumnAccessInfo;
  /**
   * the value of set autocommit true|false
   * It's an object to make sure it's {@code null} if the parsed statement is
   * not 'set autocommit...'
   */
  private Boolean autoCommitValue;

  public Boolean getAutoCommitValue() {
    return autoCommitValue;
  }
  void setAutoCommitValue(Boolean autoCommit) {
    autoCommitValue = autoCommit;
  }

  public boolean skipAuthorization() {
    return false;
  }

  public String getCboInfo() {
    return ctx.getCboInfo();
  }

  class RowFormatParams {
    String fieldDelim = null;
    String fieldEscape = null;
    String collItemDelim = null;
    String mapKeyDelim = null;
    String lineDelim = null;
    String nullFormat = null;

    protected void analyzeRowFormat(ASTNode child) throws SemanticException {
      child = (ASTNode) child.getChild(0);
      int numChildRowFormat = child.getChildCount();
      for (int numC = 0; numC < numChildRowFormat; numC++) {
        ASTNode rowChild = (ASTNode) child.getChild(numC);
        switch (rowChild.getToken().getType()) {
        case HiveParser.TOK_TABLEROWFORMATFIELD:
          fieldDelim = unescapeSQLString(rowChild.getChild(0)
              .getText());
          if (rowChild.getChildCount() >= 2) {
            fieldEscape = unescapeSQLString(rowChild
                .getChild(1).getText());
          }
          break;
        case HiveParser.TOK_TABLEROWFORMATCOLLITEMS:
          collItemDelim = unescapeSQLString(rowChild
              .getChild(0).getText());
          break;
        case HiveParser.TOK_TABLEROWFORMATMAPKEYS:
          mapKeyDelim = unescapeSQLString(rowChild.getChild(0)
              .getText());
          break;
        case HiveParser.TOK_TABLEROWFORMATLINES:
          lineDelim = unescapeSQLString(rowChild.getChild(0)
              .getText());
          if (!lineDelim.equals("\n")
              && !lineDelim.equals("10")) {
            throw new SemanticException(SemanticAnalyzer.generateErrorMessage(rowChild,
                ErrorMsg.LINES_TERMINATED_BY_NON_NEWLINE.getMsg()));
          }
          break;
        case HiveParser.TOK_TABLEROWFORMATNULL:
          nullFormat = unescapeSQLString(rowChild.getChild(0)
                    .getText());
          break;
        default:
          throw new AssertionError("Unkown Token: " + rowChild);
        }
      }
    }
  }

  public BaseSemanticAnalyzer(QueryState queryState) throws SemanticException {
    this(queryState, createHiveDB(queryState.getConf()));
  }

  public BaseSemanticAnalyzer(QueryState queryState, Hive db) throws SemanticException {
    try {
      this.queryState = queryState;
      this.conf = queryState.getConf();
      this.db = db;
      rootTasks = new ArrayList<Task<?>>();
      LOG = LoggerFactory.getLogger(this.getClass().getName());
      console = new LogHelper(LOG);
      idToTableNameMap = new HashMap<String, String>();
      inputs = new LinkedHashSet<ReadEntity>();
      outputs = new LinkedHashSet<WriteEntity>();
      txnManager = queryState.getTxnManager();
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  protected static Hive createHiveDB(HiveConf conf) throws SemanticException {
    try {
      return Hive.get(conf);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  public Map<String, String> getIdToTableNameMap() {
    return idToTableNameMap;
  }

  public abstract void analyzeInternal(ASTNode ast) throws SemanticException;
  public void init(boolean clearPartsCache) {
    //no-op
  }

  public void initCtx(Context ctx) {
    this.ctx = ctx;
  }

  public void analyze(ASTNode ast, Context ctx) throws SemanticException {
    initCtx(ctx);
    init(true);
    analyzeInternal(ast);
  }

  public void validate() throws SemanticException {
    // Implementations may choose to override this
  }

  public List<Task<?>> getRootTasks() {
    return rootTasks;
  }

  /**
   * @return the fetchTask
   */
  public FetchTask getFetchTask() {
    return fetchTask;
  }

  /**
   * @param fetchTask
   *          the fetchTask to set
   */
  public void setFetchTask(FetchTask fetchTask) {
    this.fetchTask = fetchTask;
  }

  protected void reset(boolean clearPartsCache) {
    rootTasks = new ArrayList<Task<?>>();
  }

  public static String stripIdentifierQuotes(String val) {
    if ((val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`')) {
      val = val.substring(1, val.length() - 1);
    }
    return val;
  }

  public static String stripQuotes(String val) {
    return PlanUtils.stripQuotes(val);
  }

  public static String charSetString(String charSetName, String charSetString)
      throws SemanticException {
    try {
      // The character set name starts with a _, so strip that
      charSetName = charSetName.substring(1);
      if (charSetString.charAt(0) == '\'') {
        return new String(unescapeSQLString(charSetString).getBytes(),
            charSetName);
      } else // hex input is also supported
      {
        assert charSetString.charAt(0) == '0';
        assert charSetString.charAt(1) == 'x';
        charSetString = charSetString.substring(2);

        byte[] bArray = new byte[charSetString.length() / 2];
        int j = 0;
        for (int i = 0; i < charSetString.length(); i += 2) {
          int val = Character.digit(charSetString.charAt(i), 16) * 16
              + Character.digit(charSetString.charAt(i + 1), 16);
          if (val > 127) {
            val = val - 256;
          }
          bArray[j++] = (byte)val;
        }

        String res = new String(bArray, charSetName);
        return res;
      }
    } catch (UnsupportedEncodingException e) {
      throw new SemanticException(e);
    }
  }

  /**
   * Get dequoted name from a table/column node.
   * @param tableOrColumnNode the table or column node
   * @return for table node, db.tab or tab. for column node column.
   * @throws SemanticException
   */
  public static String getUnescapedName(ASTNode tableOrColumnNode) throws SemanticException {
    return getUnescapedName(tableOrColumnNode, null);
  }

  public static Map.Entry<String, String> getDbTableNamePair(ASTNode tableNameNode) throws SemanticException {

    if (tableNameNode.getType() != HiveParser.TOK_TABNAME ||
        (tableNameNode.getChildCount() != 1 && tableNameNode.getChildCount() != 2)) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE_NAME.getMsg(tableNameNode));
    }

    if (tableNameNode.getChildCount() == 2) {
      String dbName = unescapeIdentifier(tableNameNode.getChild(0).getText());
      String tableName = unescapeIdentifier(tableNameNode.getChild(1).getText());
      if (dbName.contains(".") || tableName.contains(".")) {
        throw new SemanticException(ErrorMsg.OBJECTNAME_CONTAINS_DOT.getMsg(tableNameNode));
      }
      return Pair.of(dbName, tableName);
    } else {
      String tableName = unescapeIdentifier(tableNameNode.getChild(0).getText());
      if (tableName.contains(".")) {
        throw new SemanticException(ErrorMsg.OBJECTNAME_CONTAINS_DOT.getMsg(tableNameNode));
      }
      return Pair.of(null,tableName);
    }
  }

  public static String getUnescapedName(ASTNode tableOrColumnNode, String currentDatabase) throws SemanticException {
    int tokenType = tableOrColumnNode.getToken().getType();
    if (tokenType == HiveParser.TOK_TABNAME) {
      // table node
      Map.Entry<String,String> dbTablePair = getDbTableNamePair(tableOrColumnNode);
      return TableName.fromString(dbTablePair.getValue(),
          null,
          dbTablePair.getKey() == null ? currentDatabase : dbTablePair.getKey())
          .getNotEmptyDbTable();
    } else if (tokenType == HiveParser.StringLiteral) {
      return unescapeSQLString(tableOrColumnNode.getText());
    }
    // column node
    return unescapeIdentifier(tableOrColumnNode.getText());
  }

  /**
   * Get the name reference of a DB table node.
   * @param tabNameNode
   * @return a {@link TableName}, not null. The catalog will be missing from this.
   * @throws SemanticException
   */
  public static TableName getQualifiedTableName(ASTNode tabNameNode) throws SemanticException {
    // Ideally this would be removed, once the catalog is accessible in all use cases
    return getQualifiedTableName(tabNameNode, null);
  }

  /**
   * Get the name reference of a DB table node.
   * @param tabNameNode
   * @param catalogName the catalog of the DB/object
   * @return a {@link TableName}, not null. The catalog will be missing from this.
   * @throws SemanticException
   */
  public static TableName getQualifiedTableName(ASTNode tabNameNode, String catalogName) throws SemanticException {
    if (tabNameNode.getType() != HiveParser.TOK_TABNAME || (tabNameNode.getChildCount() != 1
        && tabNameNode.getChildCount() != 2)) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE_NAME.getMsg(tabNameNode));
    }
    if (tabNameNode.getChildCount() == 2) {
      final String dbName = unescapeIdentifier(tabNameNode.getChild(0).getText());
      final String tableName = unescapeIdentifier(tabNameNode.getChild(1).getText());
      if (dbName.contains(".") || tableName.contains(".")) {
        throw new SemanticException(ErrorMsg.OBJECTNAME_CONTAINS_DOT.getMsg(tabNameNode));
      }
      return HiveTableName.ofNullable(tableName, dbName);
    }
    final String tableName = unescapeIdentifier(tabNameNode.getChild(0).getText());
    if (tableName.contains(".")) {
      throw new SemanticException(ErrorMsg.OBJECTNAME_CONTAINS_DOT.getMsg(tabNameNode));
    }
    return HiveTableName.ofNullable(tableName);
  }

  /**
   * Get the unqualified name from a table node.
   *
   * This method works for table names qualified with their schema (e.g., "db.table")
   * and table names without schema qualification. In both cases, it returns
   * the table name without the schema.
   *
   * @param node the table node
   * @return the table name without schema qualification
   *         (i.e., if name is "db.table" or "table", returns "table")
   * @throws SemanticException
   */
  public static String getUnescapedUnqualifiedTableName(ASTNode node) throws SemanticException {
    assert node.getChildCount() <= 2;

    if (node.getChildCount() == 2) {
      node = (ASTNode) node.getChild(1);
    }

    return getUnescapedName(node);
  }


  /**
   * Remove the encapsulating "`" pair from the identifier. We allow users to
   * use "`" to escape identifier for table names, column names and aliases, in
   * case that coincide with Hive language keywords.
   */
  public static String unescapeIdentifier(String val) {
    if (val == null) {
      return null;
    }
    if (val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`') {
      val = val.substring(1, val.length() - 1);
    }
    return val;
  }

  public static Map<String, String> getProps(ASTNode prop) {
    // Must be deterministic order map for consistent q-test output across Java versions
    Map<String, String> mapProp = new LinkedHashMap<String, String>();
    readProps(prop, mapProp);
    return mapProp;
  }

  /**
   * Converts parsed key/value properties pairs into a map.
   *
   * @param prop ASTNode parent of the key/value pairs
   *
   * @param mapProp property map which receives the mappings
   */
  public static void readProps(
    ASTNode prop, Map<String, String> mapProp) {

    for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
      String key = unescapeSQLString(prop.getChild(propChild).getChild(0)
          .getText());
      String value = null;
      if (prop.getChild(propChild).getChild(1) != null) {
        value = unescapeSQLString(prop.getChild(propChild).getChild(1).getText());
      }
      mapProp.put(key, value);
    }
  }

  @SuppressWarnings("nls")
  public static String unescapeSQLString(String b) {
    Character enclosure = null;

    // Some of the strings can be passed in as unicode. For example, the
    // delimiter can be passed in as \002 - So, we first check if the
    // string is a unicode number, else go back to the old behavior
    StringBuilder sb = new StringBuilder(b.length());
    for (int i = 0; i < b.length(); i++) {

      char currentChar = b.charAt(i);
      if (enclosure == null) {
        if (currentChar == '\'' || b.charAt(i) == '\"') {
          enclosure = currentChar;
        }
        // ignore all other chars outside the enclosure
        continue;
      }

      if (enclosure.equals(currentChar)) {
        enclosure = null;
        continue;
      }

      if (currentChar == '\\' && (i + 6 < b.length()) && b.charAt(i + 1) == 'u') {
        int code = 0;
        int base = i + 2;
        for (int j = 0; j < 4; j++) {
          int digit = Character.digit(b.charAt(j + base), 16);
          code = (code << 4) + digit;
        }
        sb.append((char)code);
        i += 5;
        continue;
      }

      if (currentChar == '\\' && (i + 4 < b.length())) {
        char i1 = b.charAt(i + 1);
        char i2 = b.charAt(i + 2);
        char i3 = b.charAt(i + 3);
        if ((i1 >= '0' && i1 <= '1') && (i2 >= '0' && i2 <= '7')
            && (i3 >= '0' && i3 <= '7')) {
          byte bVal = (byte) ((i3 - '0') + ((i2 - '0') * 8) + ((i1 - '0') * 8 * 8));
          byte[] bValArr = new byte[1];
          bValArr[0] = bVal;
          String tmp = new String(bValArr);
          sb.append(tmp);
          i += 3;
          continue;
        }
      }

      if (currentChar == '\\' && (i + 2 < b.length())) {
        char n = b.charAt(i + 1);
        switch (n) {
        case '0':
          sb.append("\0");
          break;
        case '\'':
          sb.append("'");
          break;
        case '"':
          sb.append("\"");
          break;
        case 'b':
          sb.append("\b");
          break;
        case 'n':
          sb.append("\n");
          break;
        case 'r':
          sb.append("\r");
          break;
        case 't':
          sb.append("\t");
          break;
        case 'Z':
          sb.append("\u001A");
          break;
        case '\\':
          sb.append("\\");
          break;
        // The following 2 lines are exactly what MySQL does TODO: why do we do this?
        case '%':
          sb.append("\\%");
          break;
        case '_':
          sb.append("\\_");
          break;
        default:
          sb.append(n);
        }
        i++;
      } else {
        sb.append(currentChar);
      }
    }
    return sb.toString();
  }

  /**
   * Escapes the string for AST; doesn't enclose it in quotes, however.
   */
  public static String escapeSQLString(String b) {
    // There's usually nothing to escape so we will be optimistic.
    String result = b;
    for (int i = 0; i < result.length(); ++i) {
      char currentChar = result.charAt(i);
      if (currentChar == '\\' && ((i + 1) < result.length())) {
        // TODO: do we need to handle the "this is what MySQL does" here?
        char nextChar = result.charAt(i + 1);
        if (nextChar == '%' || nextChar == '_') {
          ++i;
          continue;
        }
      }
      switch (currentChar) {
      case '\0': result = spliceString(result, i, "\\0"); ++i; break;
      case '\'': result = spliceString(result, i, "\\'"); ++i; break;
      case '\"': result = spliceString(result, i, "\\\""); ++i; break;
      case '\b': result = spliceString(result, i, "\\b"); ++i; break;
      case '\n': result = spliceString(result, i, "\\n"); ++i; break;
      case '\r': result = spliceString(result, i, "\\r"); ++i; break;
      case '\t': result = spliceString(result, i, "\\t"); ++i; break;
      case '\\': result = spliceString(result, i, "\\\\"); ++i; break;
      case '\u001A': result = spliceString(result, i, "\\Z"); ++i; break;
      default: {
        if (currentChar < ' ') {
          String hex = Integer.toHexString(currentChar);
          String unicode = "\\u";
          for (int j = 4; j > hex.length(); --j) {
            unicode += '0';
          }
          unicode += hex;
          result = spliceString(result, i, unicode);
          i += (unicode.length() - 1);
        }
        break; // if not a control character, do nothing
      }
      }
    }
    return result;
  }

  private static String spliceString(String str, int i, String replacement) {
    return spliceString(str, i, 1, replacement);
  }

  private static String spliceString(String str, int i, int length, String replacement) {
    return str.substring(0, i) + replacement + str.substring(i + length);
  }

  public Set<ReadEntity> getInputs() {
    return inputs;
  }

  public Set<WriteEntity> getOutputs() {
    return outputs;
  }

  /**
   * @return the schema for the fields which will be produced
   * when the statement is executed, or null if not known
   */
  public List<FieldSchema> getResultSchema() {
    return null;
  }

  protected List<FieldSchema> getColumns(ASTNode ast) throws SemanticException {
    return getColumns(ast, true, conf);
  }

  /**
   * Get the list of FieldSchema out of the ASTNode.
   */
  public static List<FieldSchema> getColumns(ASTNode ast, boolean lowerCase, Configuration conf)
      throws SemanticException {
    return getColumns(ast, lowerCase, null, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(),
            new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), conf);
  }

  private static class ConstraintInfo {
    final String colName;
    final String constraintName;
    final boolean enable;
    final boolean validate;
    final boolean rely;
    final String defaultValue;

    ConstraintInfo(String colName, String constraintName,
                   boolean enable, boolean validate, boolean rely, String defaultValue) {
      this.colName = colName;
      this.constraintName = constraintName;
      this.enable = enable;
      this.validate = validate;
      this.rely = rely;
      this.defaultValue = defaultValue;
    }
  }

  /**
   * Process the primary keys from the ast node and populate the SQLPrimaryKey list.
   */
  protected static void processPrimaryKeys(TableName tName, ASTNode child, List<SQLPrimaryKey> primaryKeys)
      throws SemanticException {
    List<ConstraintInfo> primaryKeyInfos = new ArrayList<ConstraintInfo>();
    generateConstraintInfos(child, primaryKeyInfos);
    constraintInfosToPrimaryKeys(tName, primaryKeyInfos, primaryKeys);
  }

  protected static void processPrimaryKeys(TableName tName, ASTNode child, List<String> columnNames,
      List<SQLPrimaryKey> primaryKeys) throws SemanticException {
    List<ConstraintInfo> primaryKeyInfos = new ArrayList<ConstraintInfo>();
    generateConstraintInfos(child, columnNames, primaryKeyInfos, null, null);
    constraintInfosToPrimaryKeys(tName, primaryKeyInfos, primaryKeys);
  }

  private static void constraintInfosToPrimaryKeys(TableName tName, List<ConstraintInfo> primaryKeyInfos,
      List<SQLPrimaryKey> primaryKeys) {
    int i = 1;
    for (ConstraintInfo primaryKeyInfo : primaryKeyInfos) {
      primaryKeys.add(
          new SQLPrimaryKey(tName.getDb(), tName.getTable(), primaryKeyInfo.colName, i++, primaryKeyInfo.constraintName,
              primaryKeyInfo.enable, primaryKeyInfo.validate, primaryKeyInfo.rely));
    }
  }

  /**
   * Process the unique constraints from the ast node and populate the SQLUniqueConstraint list.
   */
  protected static void processUniqueConstraints(TableName tName, ASTNode child,
      List<SQLUniqueConstraint> uniqueConstraints) throws SemanticException {
    List<ConstraintInfo> uniqueInfos = new ArrayList<ConstraintInfo>();
    generateConstraintInfos(child, uniqueInfos);
    constraintInfosToUniqueConstraints(tName, uniqueInfos, uniqueConstraints);
  }

  protected static void processUniqueConstraints(TableName tName, ASTNode child, List<String> columnNames,
      List<SQLUniqueConstraint> uniqueConstraints) throws SemanticException {
    List<ConstraintInfo> uniqueInfos = new ArrayList<ConstraintInfo>();
    generateConstraintInfos(child, columnNames, uniqueInfos, null, null);
    constraintInfosToUniqueConstraints(tName, uniqueInfos, uniqueConstraints);
  }

  private static void constraintInfosToUniqueConstraints(TableName tName, List<ConstraintInfo> uniqueInfos,
      List<SQLUniqueConstraint> uniqueConstraints) {
    int i = 1;
    for (ConstraintInfo uniqueInfo : uniqueInfos) {
      uniqueConstraints.add(
          new SQLUniqueConstraint(tName.getCat(), tName.getDb(), tName.getTable(), uniqueInfo.colName, i++,
              uniqueInfo.constraintName, uniqueInfo.enable, uniqueInfo.validate, uniqueInfo.rely));
    }
  }

  protected static void processCheckConstraints(TableName tName, ASTNode child, List<String> columnNames,
      List<SQLCheckConstraint> checkConstraints, final ASTNode typeChild, final TokenRewriteStream tokenRewriteStream)
      throws SemanticException {
    List<ConstraintInfo> checkInfos = new ArrayList<ConstraintInfo>();
    generateConstraintInfos(child, columnNames, checkInfos, typeChild, tokenRewriteStream);
    constraintInfosToCheckConstraints(tName, checkInfos, checkConstraints);
  }

  private static void constraintInfosToCheckConstraints(TableName tName, List<ConstraintInfo> checkInfos,
      List<SQLCheckConstraint> checkConstraints) {
    for (ConstraintInfo checkInfo : checkInfos) {
      checkConstraints.add(new SQLCheckConstraint(tName.getCat(), tName.getDb(), tName.getTable(), checkInfo.colName,
          checkInfo.defaultValue, checkInfo.constraintName, checkInfo.enable, checkInfo.validate, checkInfo.rely));
    }
  }

  protected static void processDefaultConstraints(TableName tName, ASTNode child, List<String> columnNames,
      List<SQLDefaultConstraint> defaultConstraints, final ASTNode typeChild,
      final TokenRewriteStream tokenRewriteStream) throws SemanticException {
    List<ConstraintInfo> defaultInfos = new ArrayList<ConstraintInfo>();
    generateConstraintInfos(child, columnNames, defaultInfos, typeChild, tokenRewriteStream);
    constraintInfosToDefaultConstraints(tName, defaultInfos, defaultConstraints);
  }

  private static void constraintInfosToDefaultConstraints(TableName tName, List<ConstraintInfo> defaultInfos,
      List<SQLDefaultConstraint> defaultConstraints) {
    for (ConstraintInfo defaultInfo : defaultInfos) {
      defaultConstraints.add(
          new SQLDefaultConstraint(tName.getCat(), tName.getDb(), tName.getTable(), defaultInfo.colName,
              defaultInfo.defaultValue, defaultInfo.constraintName, defaultInfo.enable, defaultInfo.validate,
              defaultInfo.rely));
    }
  }

  protected static void processNotNullConstraints(TableName tName, ASTNode child, List<String> columnNames,
      List<SQLNotNullConstraint> notNullConstraints) throws SemanticException {
    List<ConstraintInfo> notNullInfos = new ArrayList<ConstraintInfo>();
    generateConstraintInfos(child, columnNames, notNullInfos, null, null);
    constraintInfosToNotNullConstraints(tName, notNullInfos, notNullConstraints);
  }

  private static void constraintInfosToNotNullConstraints(TableName tName, List<ConstraintInfo> notNullInfos,
      List<SQLNotNullConstraint> notNullConstraints) {
    for (ConstraintInfo notNullInfo : notNullInfos) {
      notNullConstraints.add(
          new SQLNotNullConstraint(tName.getCat(), tName.getDb(), tName.getTable(), notNullInfo.colName,
              notNullInfo.constraintName, notNullInfo.enable, notNullInfo.validate, notNullInfo.rely));
    }
  }

  /**
   * Get the constraint from the AST and populate the cstrInfos with the required
   * information.
   * @param child  The node with the constraint token
   * @param cstrInfos Constraint information
   * @throws SemanticException
   */
  private static void generateConstraintInfos(ASTNode child,
      List<ConstraintInfo> cstrInfos) throws SemanticException {
    ImmutableList.Builder<String> columnNames = ImmutableList.builder();
    for (int j = 0; j < child.getChild(0).getChildCount(); j++) {
      Tree columnName = child.getChild(0).getChild(j);
      checkColumnName(columnName.getText());
      columnNames.add(unescapeIdentifier(columnName.getText().toLowerCase()));
    }
    generateConstraintInfos(child, columnNames.build(), cstrInfos, null, null);
  }

  private static boolean isDefaultValueAllowed(ExprNodeDesc defaultValExpr) {
    while (FunctionRegistry.isOpCast(defaultValExpr)) {
      defaultValExpr = defaultValExpr.getChildren().get(0);
    }

    if(defaultValExpr instanceof ExprNodeConstantDesc) {
      return true;
    }

    if(defaultValExpr instanceof ExprNodeGenericFuncDesc){
      for (ExprNodeDesc argument : defaultValExpr.getChildren()) {
        if (!isDefaultValueAllowed(argument)) {
          return false;
        }
      }
      return true;
    }

    return false;
  }

  // given an ast node this method recursively goes over checkExpr ast. If it finds a node of type TOK_SUBQUERY_EXPR
  // it throws an error.
  // This method is used to validate check expression since check expression isn't allowed to have subquery
  private static void validateCheckExprAST(ASTNode checkExpr) throws SemanticException {
    if(checkExpr == null) {
      return;
    }
    if(checkExpr.getType() == HiveParser.TOK_SUBQUERY_EXPR) {
      throw new SemanticException(ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Subqueries are not allowed "
                                                                          + "in Check Constraints"));
    }
    for(int i=0; i<checkExpr.getChildCount(); i++) {
      validateCheckExprAST((ASTNode)checkExpr.getChild(i));
    }
  }
  // recursively go through expression and make sure the following:
  // * If expression is UDF it is not permanent UDF
  private static void validateCheckExpr(ExprNodeDesc checkExpr) throws SemanticException {
    if(checkExpr instanceof ExprNodeGenericFuncDesc){
      ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc)checkExpr;
      boolean isBuiltIn = FunctionRegistry.isBuiltInFuncExpr(funcDesc);
      boolean isPermanent = FunctionRegistry.isPermanentFunction(funcDesc);
      if(!isBuiltIn && !isPermanent) {
        throw new SemanticException(ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Temporary UDFs are not allowed "
                                                                            + "in Check Constraints"));
      }

      if(FunctionRegistry.impliesOrder(funcDesc.getFuncText())) {
        throw new SemanticException(ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Window functions are not allowed "
                                                                            + "in Check Constraints"));
      }
    }
    if(checkExpr.getChildren() == null) {
      return;
    }
    for(ExprNodeDesc childExpr:checkExpr.getChildren()){
      validateCheckExpr(childExpr);
    }
  }

  public static void validateCheckConstraint(List<FieldSchema> cols, List<SQLCheckConstraint> checkConstraints,
                                             Configuration conf)
  throws SemanticException{

    // create colinfo and then row resolver
    RowResolver rr = new RowResolver();
    for(FieldSchema col: cols) {
      ColumnInfo ci = new ColumnInfo(col.getName(),TypeInfoUtils.getTypeInfoFromTypeString(col.getType()),
                                     null, false);
      rr.put(null, col.getName(), ci);
    }

    TypeCheckCtx typeCheckCtx = new TypeCheckCtx(rr);
    // TypeCheckProcFactor expects typecheckctx to have unparse translator
    UnparseTranslator unparseTranslator = new UnparseTranslator(conf);
    typeCheckCtx.setUnparseTranslator(unparseTranslator);
    for(SQLCheckConstraint cc:checkConstraints) {
      try {
        ParseDriver parseDriver = new ParseDriver();
        ASTNode checkExprAST = parseDriver.parseExpression(cc.getCheck_expression());
        validateCheckExprAST(checkExprAST);
        Map<ASTNode, ExprNodeDesc> genExprs = TypeCheckProcFactory
            .genExprNode(checkExprAST, typeCheckCtx);
        ExprNodeDesc checkExpr = genExprs.get(checkExprAST);
        if(checkExpr == null) {
          throw new SemanticException(
              ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Invalid type for CHECK constraint: ")
                  + cc.getCheck_expression());
        }
        if(checkExpr.getTypeInfo().getTypeName() != serdeConstants.BOOLEAN_TYPE_NAME) {
          throw new SemanticException(
              ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Only boolean type is supported for CHECK constraint: ")
                  + cc.getCheck_expression() + ". Found: " + checkExpr.getTypeInfo().getTypeName());
        }
        validateCheckExpr(checkExpr);
      } catch(Exception e) {
        throw new SemanticException(
            ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Invalid CHECK constraint expression: ")
                + cc.getCheck_expression() + ". " + e.getMessage(), e);
      }
    }
  }

  private static String getCheckExpression(ASTNode checkExprAST, final TokenRewriteStream tokenRewriteStream)
      throws SemanticException{
    return tokenRewriteStream.toOriginalString(checkExprAST.getTokenStartIndex(), checkExprAST.getTokenStopIndex());
  }

  /**
   * Validate and get the default value from the AST
   * @param defaultValueAST AST node corresponding to default value
   * @return retrieve the default value and return it as string
   * @throws SemanticException
   */
  private static String getDefaultValue(ASTNode defaultValueAST, ASTNode typeChild,
                                        final TokenRewriteStream tokenStream) throws SemanticException{
    // first create expression from defaultValueAST
    TypeCheckCtx typeCheckCtx = new TypeCheckCtx(null);
    ExprNodeDesc defaultValExpr = TypeCheckProcFactory
        .genExprNode(defaultValueAST, typeCheckCtx).get(defaultValueAST);

    if(defaultValExpr == null) {
      throw new SemanticException(
          ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Invalid Default value!"));
    }

    //get default value to be be stored in metastore
    String defaultValueText  = tokenStream.toOriginalString(defaultValueAST.getTokenStartIndex(),
                                                            defaultValueAST.getTokenStopIndex());
    final int DEFAULT_MAX_LEN = 255;
    if(defaultValueText.length() > DEFAULT_MAX_LEN) {
      throw new SemanticException(
          ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Invalid Default value:  " + defaultValueText +
                                                  " .Maximum character length allowed is " + DEFAULT_MAX_LEN +" ."));
    }

    // Make sure the default value expression type is exactly same as column's type.
    TypeInfo defaultValTypeInfo = defaultValExpr.getTypeInfo();
    TypeInfo colTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(getTypeStringFromAST(typeChild));
    if(!defaultValTypeInfo.equals(colTypeInfo)) {
      throw new SemanticException(
          ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Invalid type: " + defaultValTypeInfo.getTypeName()
                                                  + " for default value: "
                                                  + defaultValueText
                                                  + ". Please make sure that the type is compatible with column type: "
                                                  + colTypeInfo.getTypeName()));
    }

    // throw an error if default value isn't what hive allows
    if(!isDefaultValueAllowed(defaultValExpr)) {
      throw new SemanticException(
          ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Invalid Default value: " + defaultValueText
                                                  + ". DEFAULT only allows constant or function expressions"));
    }
    return defaultValueText;
  }


  /**
   * Get the constraint from the AST and populate the cstrInfos with the required
   * information.
   * @param child  The node with the constraint token
   * @param columnNames The name of the columns for the primary key
   * @param cstrInfos Constraint information
   * @param typeChildForDefault type of column used for default value type check
   * @throws SemanticException
   */
  private static void generateConstraintInfos(ASTNode child, List<String> columnNames,
      List<ConstraintInfo> cstrInfos, ASTNode typeChildForDefault,
                                              final TokenRewriteStream tokenRewriteStream) throws SemanticException {
    // The ANTLR grammar looks like :
    // 1. KW_CONSTRAINT idfr=identifier KW_PRIMARY KW_KEY pkCols=columnParenthesesList
    //  constraintOptsCreate?
    // -> ^(TOK_PRIMARY_KEY $pkCols $idfr constraintOptsCreate?)
    // when the user specifies the constraint name.
    // 2.  KW_PRIMARY KW_KEY columnParenthesesList
    // constraintOptsCreate?
    // -> ^(TOK_PRIMARY_KEY columnParenthesesList constraintOptsCreate?)
    // when the user does not specify the constraint name.
    // Default values
    String constraintName = null;
    //by default if user hasn't provided any optional constraint properties
    // it will be considered ENABLE and NOVALIDATE and RELY=true
    boolean enable = true;
    boolean validate = false;
    boolean rely = true;
    String checkOrDefaultValue = null;
    for (int i = 0; i < child.getChildCount(); i++) {
      ASTNode grandChild = (ASTNode) child.getChild(i);
      int type = grandChild.getToken().getType();
      if (type == HiveParser.TOK_CONSTRAINT_NAME) {
        constraintName = unescapeIdentifier(grandChild.getChild(0).getText().toLowerCase());
      } else if (type == HiveParser.TOK_ENABLE) {
        enable = true;
        // validate is false by default if we enable the constraint
        // TODO: A constraint like NOT NULL could be enabled using ALTER but VALIDATE remains
        //  false in such cases. Ideally VALIDATE should be set to true to validate existing data
        validate = false;
      } else if (type == HiveParser.TOK_DISABLE) {
        enable = false;
        // validate is false by default if we disable the constraint
        validate = false;
        rely = false;
      } else if (type == HiveParser.TOK_VALIDATE) {
        validate = true;
      } else if (type == HiveParser.TOK_NOVALIDATE) {
        validate = false;
      } else if (type == HiveParser.TOK_RELY) {
        rely = true;
      } else if( type == HiveParser.TOK_NORELY) {
        rely = false;
      } else if( child.getToken().getType() == HiveParser.TOK_DEFAULT_VALUE){
        // try to get default value only if this is DEFAULT constraint
        checkOrDefaultValue = getDefaultValue(grandChild, typeChildForDefault, tokenRewriteStream);
      }
      else if(child.getToken().getType() == HiveParser.TOK_CHECK_CONSTRAINT) {
        checkOrDefaultValue = getCheckExpression(grandChild, tokenRewriteStream);
      }
    }

    // metastore schema only allows maximum 255 for constraint name column
    final int CONSTRAINT_MAX_LENGTH = 255;
    if(constraintName != null && constraintName.length() > CONSTRAINT_MAX_LENGTH) {
      throw new SemanticException(
          ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Constraint name: " + constraintName + " exceeded maximum allowed "
                                              + "length: " + CONSTRAINT_MAX_LENGTH ));
    }

    // metastore schema only allows maximum 255 for constraint value column
    if(checkOrDefaultValue!= null && checkOrDefaultValue.length() > CONSTRAINT_MAX_LENGTH) {
      throw new SemanticException(
          ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Constraint value: " + checkOrDefaultValue+ " exceeded maximum allowed "
                                                  + "length: " + CONSTRAINT_MAX_LENGTH ));
    }

    // NOT NULL constraint could be enforced/enabled
    if (enable && child.getToken().getType() != HiveParser.TOK_NOT_NULL
        && child.getToken().getType() != HiveParser.TOK_DEFAULT_VALUE
        && child.getToken().getType() != HiveParser.TOK_CHECK_CONSTRAINT) {
      throw new SemanticException(
          ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("ENABLE/ENFORCED feature not supported yet. "
              + "Please use DISABLE/NOT ENFORCED instead."));
    }
    if (validate) {
      throw new SemanticException(
        ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("VALIDATE feature not supported yet. "
              + "Please use NOVALIDATE instead."));
    }

    if(columnNames == null) {
      cstrInfos.add(new ConstraintInfo(null, constraintName,
                                       enable, validate, rely, checkOrDefaultValue));
    } else {
      for (String columnName : columnNames) {
        cstrInfos.add(new ConstraintInfo(columnName, constraintName,
                                         enable, validate, rely, checkOrDefaultValue));
      }
    }
  }

  /**
   * Process the foreign keys from the AST and populate the foreign keys in the SQLForeignKey list
   * @param tName catalog/db/table name reference
   * @param child Foreign Key token node
   * @param foreignKeys SQLForeignKey list
   * @throws SemanticException
   */
  protected static void processForeignKeys(TableName tName, ASTNode child, List<SQLForeignKey> foreignKeys)
      throws SemanticException {
    // The ANTLR grammar looks like :
    // 1.  KW_CONSTRAINT idfr=identifier KW_FOREIGN KW_KEY fkCols=columnParenthesesList
    // KW_REFERENCES tabName=tableName parCols=columnParenthesesList
    // enableSpec=enableSpecification validateSpec=validateSpecification relySpec=relySpecification
    // -> ^(TOK_FOREIGN_KEY $idfr $fkCols $tabName $parCols $relySpec $enableSpec $validateSpec)
    // when the user specifies the constraint name (i.e. child.getChildCount() == 7)
    // 2.  KW_FOREIGN KW_KEY fkCols=columnParenthesesList
    // KW_REFERENCES tabName=tableName parCols=columnParenthesesList
    // enableSpec=enableSpecification validateSpec=validateSpecification relySpec=relySpecification
    // -> ^(TOK_FOREIGN_KEY $fkCols  $tabName $parCols $relySpec $enableSpec $validateSpec)
    // when the user does not specify the constraint name (i.e. child.getChildCount() == 6)
    String constraintName = null;
    boolean enable = true;
    boolean validate = true;
    boolean rely = false;
    int fkIndex = -1;
    for (int i = 0; i < child.getChildCount(); i++) {
      ASTNode grandChild = (ASTNode) child.getChild(i);
      int type = grandChild.getToken().getType();
      if (type == HiveParser.TOK_CONSTRAINT_NAME) {
        constraintName = unescapeIdentifier(grandChild.getChild(0).getText().toLowerCase());
      } else if (type == HiveParser.TOK_ENABLE) {
        enable = true;
        // validate is true by default if we enable the constraint
        validate = true;
      } else if (type == HiveParser.TOK_DISABLE) {
        enable = false;
        // validate is false by default if we disable the constraint
        validate = false;
      } else if (type == HiveParser.TOK_VALIDATE) {
        validate = true;
      } else if (type == HiveParser.TOK_NOVALIDATE) {
        validate = false;
      } else if (type == HiveParser.TOK_RELY) {
        rely = true;
      } else if (type == HiveParser.TOK_TABCOLNAME && fkIndex == -1) {
        fkIndex = i;
      }
    }
    if (enable) {
      throw new SemanticException(
          ErrorMsg.INVALID_FK_SYNTAX.getMsg("ENABLE feature not supported yet. "
              + "Please use DISABLE instead."));
    }
    if (validate) {
      throw new SemanticException(
        ErrorMsg.INVALID_FK_SYNTAX.getMsg("VALIDATE feature not supported yet. "
              + "Please use NOVALIDATE instead."));
    }

    int ptIndex = fkIndex + 1;
    int pkIndex = ptIndex + 1;
    if (child.getChild(fkIndex).getChildCount() != child.getChild(pkIndex).getChildCount()) {
      throw new SemanticException(ErrorMsg.INVALID_FK_SYNTAX.getMsg(
        " The number of foreign key columns should be same as number of parent key columns "));
    }

    final TableName parentTblName = getQualifiedTableName((ASTNode) child.getChild(ptIndex));
    for (int j = 0; j < child.getChild(fkIndex).getChildCount(); j++) {
      SQLForeignKey sqlForeignKey = new SQLForeignKey();
      sqlForeignKey.setFktable_db(tName.getDb());
      sqlForeignKey.setFktable_name(tName.getTable());
      Tree fkgrandChild = child.getChild(fkIndex).getChild(j);
      checkColumnName(fkgrandChild.getText());
      sqlForeignKey.setFkcolumn_name(unescapeIdentifier(fkgrandChild.getText().toLowerCase()));
      sqlForeignKey.setPktable_db(parentTblName.getDb());
      sqlForeignKey.setPktable_name(parentTblName.getTable());
      Tree pkgrandChild = child.getChild(pkIndex).getChild(j);
      sqlForeignKey.setPkcolumn_name(unescapeIdentifier(pkgrandChild.getText().toLowerCase()));
      sqlForeignKey.setKey_seq(j+1);
      sqlForeignKey.setFk_name(constraintName);
      sqlForeignKey.setEnable_cstr(enable);
      sqlForeignKey.setValidate_cstr(validate);
      sqlForeignKey.setRely_cstr(rely);
      foreignKeys.add(sqlForeignKey);
    }
  }

  protected boolean hasEnabledOrValidatedConstraints(List<SQLNotNullConstraint> notNullConstraints,
                                                     List<SQLDefaultConstraint> defaultConstraints,
                                                     List<SQLCheckConstraint> checkConstraints){
    if(notNullConstraints != null) {
      for (SQLNotNullConstraint nnC : notNullConstraints) {
        if (nnC.isEnable_cstr() || nnC.isValidate_cstr()) {
          return true;
        }
      }
    }
    if(defaultConstraints!= null && !defaultConstraints.isEmpty()) {
          return true;
    }
    if(checkConstraints!= null && !checkConstraints.isEmpty()) {
          return true;
    }
    return false;
  }

  private static void checkColumnName(String columnName) throws SemanticException {
    if (VirtualColumn.VIRTUAL_COLUMN_NAMES.contains(columnName.toUpperCase())) {
      throw new SemanticException(ErrorMsg.INVALID_COLUMN_NAME.getMsg(columnName));
    }
  }

  /**
   * Get the list of FieldSchema out of the ASTNode.
   * Additionally, populate the primaryKeys and foreignKeys if any.
   */
  public static List<FieldSchema> getColumns(
      ASTNode ast, boolean lowerCase, TokenRewriteStream tokenRewriteStream,
      List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys,
      List<SQLUniqueConstraint> uniqueConstraints, List<SQLNotNullConstraint> notNullConstraints,
      List<SQLDefaultConstraint> defaultConstraints, List<SQLCheckConstraint> checkConstraints,
      Configuration conf) throws SemanticException {
    List<FieldSchema> colList = new ArrayList<FieldSchema>();
    Tree parent = ast.getParent();

    for (int i = 0; i < ast.getChildCount(); i++) {
      FieldSchema col = new FieldSchema();
      ASTNode child = (ASTNode) ast.getChild(i);
      switch (child.getToken().getType()) {
      case HiveParser.TOK_UNIQUE: {
        final TableName tName =
            getQualifiedTableName((ASTNode) parent.getChild(0), MetaStoreUtils.getDefaultCatalog(conf));
        // TODO CAT - for now always use the default catalog.  Eventually will want to see if
        // the user specified a catalog
        processUniqueConstraints(tName, child, uniqueConstraints);
      }
      break;
      case HiveParser.TOK_PRIMARY_KEY: {
        if (!primaryKeys.isEmpty()) {
          throw new SemanticException(ErrorMsg.INVALID_CONSTRAINT
              .getMsg("Cannot exist more than one primary key definition for the same table"));
        }
        final TableName tName = getQualifiedTableName((ASTNode) parent.getChild(0));
        processPrimaryKeys(tName, child, primaryKeys);
      }
      break;
      case HiveParser.TOK_FOREIGN_KEY: {
        final TableName tName = getQualifiedTableName((ASTNode) parent.getChild(0));
        processForeignKeys(tName, child, foreignKeys);
      }
      break;
      case HiveParser.TOK_CHECK_CONSTRAINT: {
        final TableName tName =
            getQualifiedTableName((ASTNode) parent.getChild(0), MetaStoreUtils.getDefaultCatalog(conf));
        // TODO CAT - for now always use the default catalog.  Eventually will want to see if
        // the user specified a catalog
        processCheckConstraints(tName, child, null,
            checkConstraints, null, tokenRewriteStream);
      }
      break;
      default:
        Tree grandChild = child.getChild(0);
        if (grandChild != null) {
          String name = grandChild.getText();
          if (lowerCase) {
            name = name.toLowerCase();
          }
          checkColumnName(name);
          // child 0 is the name of the column
          col.setName(unescapeIdentifier(name));
          // child 1 is the type of the column
          ASTNode typeChild = (ASTNode) (child.getChild(1));
          col.setType(getTypeStringFromAST(typeChild));

          // child 2 is the optional comment of the column
          // child 3 is the optional constraint
          ASTNode constraintChild = null;
          if (child.getChildCount() == 4) {
            col.setComment(unescapeSQLString(child.getChild(2).getText()));
            constraintChild = (ASTNode) child.getChild(3);
          } else if (child.getChildCount() == 3
              && ((ASTNode) child.getChild(2)).getToken().getType() == HiveParser.StringLiteral) {
            col.setComment(unescapeSQLString(child.getChild(2).getText()));
          } else if (child.getChildCount() == 3) {
            constraintChild = (ASTNode) child.getChild(2);
          }
          if (constraintChild != null) {
            final TableName tName =
                getQualifiedTableName((ASTNode) parent.getChild(0), MetaStoreUtils.getDefaultCatalog(conf));
            // TODO CAT - for now always use the default catalog.  Eventually will want to see if
            // the user specified a catalog
            // Process column constraint
            switch (constraintChild.getToken().getType()) {
            case HiveParser.TOK_CHECK_CONSTRAINT:
              processCheckConstraints(tName, constraintChild, ImmutableList.of(col.getName()), checkConstraints,
                  typeChild, tokenRewriteStream);
              break;
            case HiveParser.TOK_DEFAULT_VALUE:
              processDefaultConstraints(tName, constraintChild, ImmutableList.of(col.getName()), defaultConstraints,
                  typeChild, tokenRewriteStream);
              break;
            case HiveParser.TOK_NOT_NULL:
              processNotNullConstraints(tName, constraintChild, ImmutableList.of(col.getName()), notNullConstraints);
              break;
            case HiveParser.TOK_UNIQUE:
              processUniqueConstraints(tName, constraintChild, ImmutableList.of(col.getName()), uniqueConstraints);
              break;
            case HiveParser.TOK_PRIMARY_KEY:
              if (!primaryKeys.isEmpty()) {
                throw new SemanticException(ErrorMsg.INVALID_CONSTRAINT
                    .getMsg("Cannot exist more than one primary key definition for the same table"));
              }
              processPrimaryKeys(tName, constraintChild, ImmutableList.of(col.getName()), primaryKeys);
              break;
            case HiveParser.TOK_FOREIGN_KEY:
              processForeignKeys(tName, constraintChild,
                  foreignKeys);
              break;
            default:
              throw new SemanticException(ErrorMsg.NOT_RECOGNIZED_CONSTRAINT.getMsg(
                  constraintChild.getToken().getText()));
            }
          }
        }
        colList.add(col);
        break;
      }
    }
    return colList;
  }

  public static List<String> getColumnNames(ASTNode ast) {
    List<String> colList = new ArrayList<String>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      colList.add(unescapeIdentifier(child.getText()).toLowerCase());
    }
    return colList;
  }

  protected List<Order> getColumnNamesOrder(ASTNode ast) throws SemanticException {
    List<Order> colList = new ArrayList<Order>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      int directionCode = DirectionUtils.tokenToCode(child.getToken().getType());
      child = (ASTNode) child.getChild(0);
      if (child.getToken().getType() != HiveParser.TOK_NULLS_FIRST && directionCode == DirectionUtils.ASCENDING_CODE) {
        throw new SemanticException(
                "create/alter bucketed table: not supported NULLS LAST for SORTED BY in ASC order");
      }
      if (child.getToken().getType() != HiveParser.TOK_NULLS_LAST && directionCode == DirectionUtils.DESCENDING_CODE) {
        throw new SemanticException(
                "create/alter bucketed table: not supported NULLS FIRST for SORTED BY in DESC order");
      }
      colList.add(new Order(unescapeIdentifier(child.getChild(0).getText()).toLowerCase(), directionCode));
    }
    return colList;
  }

  protected static String getTypeStringFromAST(ASTNode typeNode)
      throws SemanticException {
    switch (typeNode.getType()) {
    case HiveParser.TOK_LIST:
      return serdeConstants.LIST_TYPE_NAME + "<"
          + getTypeStringFromAST((ASTNode) typeNode.getChild(0)) + ">";
    case HiveParser.TOK_MAP:
      return serdeConstants.MAP_TYPE_NAME + "<"
          + getTypeStringFromAST((ASTNode) typeNode.getChild(0)) + ","
          + getTypeStringFromAST((ASTNode) typeNode.getChild(1)) + ">";
    case HiveParser.TOK_STRUCT:
      return getStructTypeStringFromAST(typeNode);
    case HiveParser.TOK_UNIONTYPE:
      return getUnionTypeStringFromAST(typeNode);
    default:
      return DDLSemanticAnalyzer.getTypeName(typeNode);
    }
  }

  private static String getStructTypeStringFromAST(ASTNode typeNode)
      throws SemanticException {
    String typeStr = serdeConstants.STRUCT_TYPE_NAME + "<";
    typeNode = (ASTNode) typeNode.getChild(0);
    int children = typeNode.getChildCount();
    if (children <= 0) {
      throw new SemanticException("empty struct not allowed.");
    }
    StringBuilder buffer = new StringBuilder(typeStr);
    for (int i = 0; i < children; i++) {
      ASTNode child = (ASTNode) typeNode.getChild(i);
      buffer.append(unescapeIdentifier(child.getChild(0).getText())).append(":");
      buffer.append(getTypeStringFromAST((ASTNode) child.getChild(1)));
      if (i < children - 1) {
        buffer.append(",");
      }
    }

    buffer.append(">");
    return buffer.toString();
  }

  private static String getUnionTypeStringFromAST(ASTNode typeNode)
      throws SemanticException {
    String typeStr = serdeConstants.UNION_TYPE_NAME + "<";
    typeNode = (ASTNode) typeNode.getChild(0);
    int children = typeNode.getChildCount();
    if (children <= 0) {
      throw new SemanticException("empty union not allowed.");
    }
    StringBuilder buffer = new StringBuilder(typeStr);
    for (int i = 0; i < children; i++) {
      buffer.append(getTypeStringFromAST((ASTNode) typeNode.getChild(i)));
      if (i < children - 1) {
        buffer.append(",");
      }
    }
    buffer.append(">");
    typeStr = buffer.toString();
    return typeStr;
  }

  /**
   * TableSpec.
   *
   */
  public static class TableSpec {
    private TableName tableName;
    public Table tableHandle;
    public Map<String, String> partSpec; // has to use LinkedHashMap to enforce order
    public Partition partHandle;
    public int numDynParts; // number of dynamic partition columns
    public List<Partition> partitions; // involved partitions in TableScanOperator/FileSinkOperator
    public static enum SpecType {TABLE_ONLY, STATIC_PARTITION, DYNAMIC_PARTITION}
    public SpecType specType;

    public TableSpec(Hive db, HiveConf conf, ASTNode ast)
        throws SemanticException {
      this(db, conf, ast, true, false);
    }

    public TableSpec(Table table) {
      tableHandle = table;
      tableName = TableName.fromString(table.getTableName(), SessionState.get().getCurrentCatalog(), table.getDbName());
      specType = SpecType.TABLE_ONLY;
    }

    public TableSpec(Hive db, TableName tableName, Map<String, String> partSpec)
        throws HiveException {
      this(db, tableName.getNotEmptyDbTable(), partSpec, false);
    }

    public TableSpec(Hive db, String tableName, Map<String, String> partSpec, boolean allowPartialPartitionsSpec)
        throws HiveException {
      Table table = db.getTable(tableName);
      tableHandle = table;
      this.tableName = TableName.fromString(table.getTableName(), SessionState.get().getCurrentCatalog(),
          table.getDbName());
      if (partSpec == null) {
        specType = SpecType.TABLE_ONLY;
      } else if(allowPartialPartitionsSpec) {
        partitions = db.getPartitions(table, partSpec);
        specType = SpecType.STATIC_PARTITION;
      } else {
        Partition partition = db.getPartition(table, partSpec, false);
        if (partition == null) {
          throw new SemanticException("partition is unknown: " + table + "/" + partSpec);
        }
        partHandle = partition;
        partitions = Collections.singletonList(partHandle);
        specType = SpecType.STATIC_PARTITION;
      }
    }

    public TableSpec(Table tableHandle, List<Partition> partitions)
        throws HiveException {
      this.tableHandle = tableHandle;
      this.tableName =
          TableName.fromString(tableHandle.getTableName(), tableHandle.getCatalogName(), tableHandle.getDbName());
      if (partitions != null && !partitions.isEmpty()) {
        this.specType = SpecType.STATIC_PARTITION;
        this.partitions = partitions;
        List<FieldSchema> partCols = this.tableHandle.getPartCols();
        this.partSpec = new LinkedHashMap<>();
        for (FieldSchema partCol : partCols) {
          partSpec.put(partCol.getName(), null);
        }
      } else {
        this.specType = SpecType.TABLE_ONLY;
      }
    }

    private boolean createDynPartSpec(ASTNode ast) {
      if(ast.getToken().getType() != HiveParser.TOK_CREATETABLE &&
          ast.getToken().getType() != HiveParser.TOK_CREATE_MATERIALIZED_VIEW &&
          ast.getToken().getType() != HiveParser.TOK_ALTER_MATERIALIZED_VIEW &&
          tableHandle.getPartitionKeys().size() > 0
          && (ast.getParent() != null && (ast.getParent().getType() == HiveParser.TOK_INSERT_INTO
          || ast.getParent().getType() == HiveParser.TOK_INSERT)
          || ast.getParent().getType() == HiveParser.TOK_DESTINATION
          || ast.getParent().getType() == HiveParser.TOK_ANALYZE)) {
        return true;
      }
      return false;
    }
    public TableSpec(Hive db, HiveConf conf, ASTNode ast, boolean allowDynamicPartitionsSpec,
        boolean allowPartialPartitionsSpec) throws SemanticException {
      assert (ast.getToken().getType() == HiveParser.TOK_TAB
          || ast.getToken().getType() == HiveParser.TOK_TABLE_PARTITION
          || ast.getToken().getType() == HiveParser.TOK_TABTYPE
          || ast.getToken().getType() == HiveParser.TOK_CREATETABLE
          || ast.getToken().getType() == HiveParser.TOK_CREATE_MATERIALIZED_VIEW);
      int childIndex = 0;
      numDynParts = 0;

      try {
        // get table metadata
        tableName = HiveTableName.withNoDefault(getUnescapedName((ASTNode)ast.getChild(0)));
        boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODE);
        if (testMode) {
          tableName = TableName.fromString(String.join("", conf.getVar(HiveConf.ConfVars.HIVETESTMODEPREFIX),
              tableName.getTable()), tableName.getCat(), tableName.getDb()); // not that elegant, but hard to refactor
        }
        if (ast.getToken().getType() != HiveParser.TOK_CREATETABLE &&
            ast.getToken().getType() != HiveParser.TOK_CREATE_MATERIALIZED_VIEW &&
            ast.getToken().getType() != HiveParser.TOK_ALTER_MATERIALIZED_VIEW) {
          tableHandle = db.getTable(tableName);
        }
      } catch (InvalidTableException ite) {
        throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(ast
            .getChild(0)), ite);
      } catch (HiveException e) {
        throw new SemanticException(ErrorMsg.CANNOT_RETRIEVE_TABLE_METADATA.getMsg(ast
            .getChild(childIndex), e.getMessage()), e);
      }

      // get partition metadata if partition specified
      if (ast.getChildCount() == 2 && ast.getToken().getType() != HiveParser.TOK_CREATETABLE &&
          ast.getToken().getType() != HiveParser.TOK_CREATE_MATERIALIZED_VIEW &&
          ast.getToken().getType() != HiveParser.TOK_ALTER_MATERIALIZED_VIEW) {
        childIndex = 1;
        ASTNode partspec = (ASTNode) ast.getChild(1);
        partitions = new ArrayList<Partition>();
        // partSpec is a mapping from partition column name to its value.
        Map<String, String> tmpPartSpec = new HashMap<String, String>(partspec.getChildCount());
        for (int i = 0; i < partspec.getChildCount(); ++i) {
          ASTNode partspec_val = (ASTNode) partspec.getChild(i);
          String val = null;
          String colName = unescapeIdentifier(partspec_val.getChild(0).getText().toLowerCase());
          if (partspec_val.getChildCount() < 2) { // DP in the form of T partition (ds, hr)
            if (allowDynamicPartitionsSpec) {
              ++numDynParts;
            } else {
              throw new SemanticException(ErrorMsg.INVALID_PARTITION
                                                       .getMsg(" - Dynamic partitions not allowed"));
            }
          } else { // in the form of T partition (ds="2010-03-03")
            val = stripQuotes(partspec_val.getChild(1).getText());
          }
          tmpPartSpec.put(colName, val);
        }

        // check if the columns, as well as value types in the partition() clause are valid
        validatePartSpec(tableHandle, tmpPartSpec, ast, conf, false);

        List<FieldSchema> parts = tableHandle.getPartitionKeys();
        partSpec = new LinkedHashMap<String, String>(partspec.getChildCount());
        for (FieldSchema fs : parts) {
          String partKey = fs.getName();
          partSpec.put(partKey, tmpPartSpec.get(partKey));
        }

        // check if the partition spec is valid
        if (numDynParts > 0) {
          int numStaPart = parts.size() - numDynParts;
          if (numStaPart == 0 &&
              conf.getVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE).equalsIgnoreCase("strict")) {
            throw new SemanticException(ErrorMsg.DYNAMIC_PARTITION_STRICT_MODE.getMsg());
          }

          // check the partitions in partSpec be the same as defined in table schema
          if (partSpec.keySet().size() != parts.size()) {
            ErrorPartSpec(partSpec, parts);
          }
          Iterator<String> itrPsKeys = partSpec.keySet().iterator();
          for (FieldSchema fs: parts) {
            if (!itrPsKeys.next().toLowerCase().equals(fs.getName().toLowerCase())) {
              ErrorPartSpec(partSpec, parts);
            }
          }

          // check if static partition appear after dynamic partitions
          for (FieldSchema fs: parts) {
            if (partSpec.get(fs.getName().toLowerCase()) == null) {
              if (numStaPart > 0) { // found a DP, but there exists ST as subpartition
                throw new SemanticException(
                    ErrorMsg.PARTITION_DYN_STA_ORDER.getMsg(ast.getChild(childIndex)));
              }
              break;
            } else {
              --numStaPart;
            }
          }
          partHandle = null;
          specType = SpecType.DYNAMIC_PARTITION;
        } else {
          try {
            if (allowPartialPartitionsSpec) {
              partitions = db.getPartitions(tableHandle, partSpec);
            } else {
              // this doesn't create partition.
              partHandle = db.getPartition(tableHandle, partSpec, false);
              if (partHandle == null) {
                // if partSpec doesn't exists in DB, return a delegate one
                // and the actual partition is created in MoveTask
                partHandle = new Partition(tableHandle, partSpec, null);
              } else {
                partitions.add(partHandle);
              }
            }
          } catch (HiveException e) {
            throw new SemanticException(
                ErrorMsg.INVALID_PARTITION.getMsg(ast.getChild(childIndex)), e);
          }
          specType = SpecType.STATIC_PARTITION;
        }
      } else if(createDynPartSpec(ast) && allowDynamicPartitionsSpec) {
        // if user hasn't specify partition spec generate it from table's partition spec
        // do this only if it is INSERT/INSERT INTO/INSERT OVERWRITE/ANALYZE
        List<FieldSchema> parts = tableHandle.getPartitionKeys();
        partSpec = new LinkedHashMap<String, String>(parts.size());
        for (FieldSchema fs : parts) {
          String partKey = fs.getName();
          partSpec.put(partKey, null);
        }
        partHandle = null;
        specType = SpecType.DYNAMIC_PARTITION;
      } else {
        specType = SpecType.TABLE_ONLY;
      }
    }

    public TableName getTableName() {
      return tableName;
    }

    public void setTableName(TableName tableName) {
      this.tableName = tableName;
    }

    public Map<String, String> getPartSpec() {
      return this.partSpec;
    }

    public void setPartSpec(Map<String, String> partSpec) {
      this.partSpec = partSpec;
    }

    @Override
    public String toString() {
      if (partHandle != null) {
        return partHandle.toString();
      } else {
        return tableHandle.toString();
      }
    }
  }

  public static class AnalyzeRewriteContext {

    private String tableName;
    private List<String> colName;
    private List<String> colType;
    private boolean tblLvl;

    public String getTableName() {
      return tableName;
    }

    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    public List<String> getColName() {
      return colName;
    }

    public void setColName(List<String> colName) {
      this.colName = colName;
    }

    public boolean isTblLvl() {
      return tblLvl;
    }

    public void setTblLvl(boolean isTblLvl) {
      this.tblLvl = isTblLvl;
    }

    public List<String> getColType() {
      return colType;
    }

    public void setColType(List<String> colType) {
      this.colType = colType;
    }

  }

  /**
   * Gets the lineage information.
   *
   * @return LineageInfo associated with the query.
   */
  public LineageInfo getLineageInfo() {
    return linfo;
  }

  /**
   * Sets the lineage information.
   *
   * @param linfo The LineageInfo structure that is set in the optimization phase.
   */
  public void setLineageInfo(LineageInfo linfo) {
    this.linfo = linfo;
  }

  /**
   * Gets the table access information.
   *
   * @return TableAccessInfo associated with the query.
   */
  public TableAccessInfo getTableAccessInfo() {
    return tableAccessInfo;
  }

  /**
   * Sets the table access information.
   *
   * @param tableAccessInfo The TableAccessInfo structure that is set in the optimization phase.
   */
  public void setTableAccessInfo(TableAccessInfo tableAccessInfo) {
    this.tableAccessInfo = tableAccessInfo;
  }

  /**
   * Gets the column access information.
   *
   * @return ColumnAccessInfo associated with the query.
   */
  public ColumnAccessInfo getColumnAccessInfo() {
    return columnAccessInfo;
  }

  /**
   * Sets the column access information.
   *
   * @param columnAccessInfo The ColumnAccessInfo structure that is set immediately after
   * the optimization phase.
   */
  public void setColumnAccessInfo(ColumnAccessInfo columnAccessInfo) {
    this.columnAccessInfo = columnAccessInfo;
  }

  public ColumnAccessInfo getUpdateColumnAccessInfo() {
    return updateColumnAccessInfo;
  }

  public void setUpdateColumnAccessInfo(ColumnAccessInfo updateColumnAccessInfo) {
    this.updateColumnAccessInfo = updateColumnAccessInfo;
  }

  /**
   * Checks if given specification is proper specification for prefix of
   * partition cols, for table partitioned by ds, hr, min valid ones are
   * (ds='2008-04-08'), (ds='2008-04-08', hr='12'), (ds='2008-04-08', hr='12', min='30')
   * invalid one is for example (ds='2008-04-08', min='30')
   * @param spec specification key-value map
   * @return true if the specification is prefix; never returns false, but throws
   * @throws HiveException
   */
  public final boolean isValidPrefixSpec(Table tTable, Map<String, String> spec)
      throws HiveException {

    // TODO - types need to be checked.
    List<FieldSchema> partCols = tTable.getPartitionKeys();
    if (partCols == null || (partCols.size() == 0)) {
      if (spec != null) {
        throw new HiveException(
            "table is not partitioned but partition spec exists: "
                + spec);
      } else {
        return true;
      }
    }

    if (spec == null) {
      throw new HiveException("partition spec is not specified");
    }

    Iterator<String> itrPsKeys = spec.keySet().iterator();
    for (FieldSchema fs: partCols) {
      if(!itrPsKeys.hasNext()) {
        break;
      }
      if (!itrPsKeys.next().toLowerCase().equals(
              fs.getName().toLowerCase())) {
        ErrorPartSpec(spec, partCols);
      }
    }

    if(itrPsKeys.hasNext()) {
      ErrorPartSpec(spec, partCols);
    }

    return true;
  }

  private static void ErrorPartSpec(Map<String, String> partSpec,
      List<FieldSchema> parts) throws SemanticException {
    StringBuilder sb =
        new StringBuilder(
            "Partition columns in the table schema are: (");
    for (FieldSchema fs : parts) {
      sb.append(fs.getName()).append(", ");
    }
    sb.setLength(sb.length() - 2); // remove the last ", "
    sb.append("), while the partitions specified in the query are: (");

    Iterator<String> itrPsKeys = partSpec.keySet().iterator();
    while (itrPsKeys.hasNext()) {
      sb.append(itrPsKeys.next()).append(", ");
    }
    sb.setLength(sb.length() - 2); // remove the last ", "
    sb.append(").");
    throw new SemanticException(ErrorMsg.PARTSPEC_DIFFER_FROM_SCHEMA
        .getMsg(sb.toString()));
  }

  public Hive getDb() {
    return db;
  }

  public QueryProperties getQueryProperties() {
    return queryProperties;
  }

  public Set<FileSinkDesc> getAcidFileSinks() {
    return acidFileSinks;
  }

  public boolean hasTransactionalInQuery() {
    return transactionalInQuery;
  }

  protected ListBucketingCtx constructListBucketingCtx(List<String> skewedColNames,
      List<List<String>> skewedValues, Map<List<String>, String> skewedColValueLocationMaps,
      boolean isStoredAsSubDirectories) {
    ListBucketingCtx lbCtx = new ListBucketingCtx();
    lbCtx.setSkewedColNames(skewedColNames);
    lbCtx.setSkewedColValues(skewedValues);
    lbCtx.setLbLocationMap(skewedColValueLocationMaps);
    lbCtx.setStoredAsSubDirectories(isStoredAsSubDirectories);
    lbCtx.setDefaultKey(ListBucketingPrunerUtils.HIVE_LIST_BUCKETING_DEFAULT_KEY);
    lbCtx.setDefaultDirName(ListBucketingPrunerUtils.HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME);
    return lbCtx;
  }

  /**
   * Given a ASTNode, return list of values.
   *
   * use case:
   *   create table xyz list bucketed (col1) with skew (1,2,5)
   *   AST Node is for (1,2,5)
   * @param ast
   * @return
   */
  protected List<String> getSkewedValueFromASTNode(ASTNode ast) {
    List<String> colList = new ArrayList<String>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      colList.add(stripQuotes(child.getText()).toLowerCase());
    }
    return colList;
  }

  /**
   * Retrieve skewed values from ASTNode.
   *
   * @param node
   * @return
   * @throws SemanticException
   */
  protected List<String> getSkewedValuesFromASTNode(Node node) throws SemanticException {
    List<String> result = null;
    Tree leafVNode = ((ASTNode) node).getChild(0);
    if (leafVNode == null) {
      throw new SemanticException(
          ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
    } else {
      ASTNode lVAstNode = (ASTNode) leafVNode;
      if (lVAstNode.getToken().getType() != HiveParser.TOK_TABCOLVALUE) {
        throw new SemanticException(
            ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
      } else {
        result = new ArrayList<String>(getSkewedValueFromASTNode(lVAstNode));
      }
    }
    return result;
  }

  /**
   * Analyze list bucket column names
   *
   * @param skewedColNames
   * @param child
   * @return
   * @throws SemanticException
   */
  protected List<String> analyzeSkewedTablDDLColNames(List<String> skewedColNames, ASTNode child)
      throws SemanticException {
  Tree nNode = child.getChild(0);
    if (nNode == null) {
      throw new SemanticException(ErrorMsg.SKEWED_TABLE_NO_COLUMN_NAME.getMsg());
    } else {
      ASTNode nAstNode = (ASTNode) nNode;
      if (nAstNode.getToken().getType() != HiveParser.TOK_TABCOLNAME) {
        throw new SemanticException(ErrorMsg.SKEWED_TABLE_NO_COLUMN_NAME.getMsg());
      } else {
        skewedColNames = getColumnNames(nAstNode);
      }
    }
    return skewedColNames;
  }

  /**
   * Handle skewed values in DDL.
   *
   * It can be used by both skewed by ... on () and set skewed location ().
   *
   * @param skewedValues
   * @param child
   * @throws SemanticException
   */
  protected void analyzeDDLSkewedValues(List<List<String>> skewedValues, ASTNode child)
      throws SemanticException {
  Tree vNode = child.getChild(1);
    if (vNode == null) {
      throw new SemanticException(ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
    }
    ASTNode vAstNode = (ASTNode) vNode;
    switch (vAstNode.getToken().getType()) {
      case HiveParser.TOK_TABCOLVALUE:
        for (String str : getSkewedValueFromASTNode(vAstNode)) {
          List<String> sList = new ArrayList<String>(Arrays.asList(str));
          skewedValues.add(sList);
        }
        break;
      case HiveParser.TOK_TABCOLVALUE_PAIR:
        List<Node> vLNodes = vAstNode.getChildren();
        for (Node node : vLNodes) {
          if ( ((ASTNode) node).getToken().getType() != HiveParser.TOK_TABCOLVALUES) {
            throw new SemanticException(
                ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
          } else {
            skewedValues.add(getSkewedValuesFromASTNode(node));
          }
        }
        break;
      default:
        break;
    }
  }

  /**
   * process stored as directories
   *
   * @param child
   * @return
   */
  protected boolean analyzeStoredAdDirs(ASTNode child) {
    boolean storedAsDirs = false;
    if ((child.getChildCount() == 3)
        && (((ASTNode) child.getChild(2)).getToken().getType()
            == HiveParser.TOK_STOREDASDIRS)) {
      storedAsDirs = true;
    }
    return storedAsDirs;
  }

  private static boolean getPartExprNodeDesc(ASTNode astNode, HiveConf conf,
      Map<ASTNode, ExprNodeDesc> astExprNodeMap) throws SemanticException {

    if (astNode == null) {
      return true;
    } else if ((astNode.getChildren() == null) || (astNode.getChildren().size() == 0)) {
      return astNode.getType() != HiveParser.TOK_PARTVAL;
    }

    TypeCheckCtx typeCheckCtx = new TypeCheckCtx(null);
    String defaultPartitionName = HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULTPARTITIONNAME);
    boolean result = true;
    for (Node childNode : astNode.getChildren()) {
      ASTNode childASTNode = (ASTNode)childNode;

      if (childASTNode.getType() != HiveParser.TOK_PARTVAL) {
        result = getPartExprNodeDesc(childASTNode, conf, astExprNodeMap) && result;
      } else {
        boolean isDynamicPart = childASTNode.getChildren().size() <= 1;
        result = !isDynamicPart && result;
        if (!isDynamicPart) {
          ASTNode partVal = (ASTNode)childASTNode.getChildren().get(1);
          if (!defaultPartitionName.equalsIgnoreCase(unescapeSQLString(partVal.getText()))) {
            astExprNodeMap.put((ASTNode)childASTNode.getChildren().get(0),
                TypeCheckProcFactory.genExprNode(partVal, typeCheckCtx).get(partVal));
          }
        }
      }
    }
    return result;
  }

  /**
   * Get the partition specs from the tree.
   *
   * @param ast
   *          Tree to extract partitions from.
   * @return A list of partition name to value mappings.
   * @throws SemanticException
   */
  public List<Map<String, String>> getPartitionSpecs(Table tbl, CommonTree ast)
      throws SemanticException {
    List<Map<String, String>> partSpecs = new ArrayList<Map<String, String>>();
    int childIndex = 0;
    // get partition metadata if partition specified
    for (childIndex = 0; childIndex < ast.getChildCount(); childIndex++) {
      ASTNode partSpecNode = (ASTNode)ast.getChild(childIndex);
      // sanity check
      if (partSpecNode.getType() == HiveParser.TOK_PARTSPEC) {
        Map<String, String> partSpec = getValidatedPartSpec(tbl, partSpecNode, conf, false);
        partSpecs.add(partSpec);
      }
    }
    return partSpecs;
  }

  public static Map<String, String> getValidatedPartSpec(Table table, ASTNode astNode,
      HiveConf conf, boolean shouldBeFull) throws SemanticException {
    Map<String, String> partSpec = getPartSpec(astNode);
    if (partSpec != null && !partSpec.isEmpty()) {
      validatePartSpec(table, partSpec, astNode, conf, shouldBeFull);
    }
    return partSpec;
  }

  public static Map<String, String> getPartSpec(ASTNode node)
      throws SemanticException {
    if (node == null) {
      return null;
    }

    Map<String, String> partSpec = new LinkedHashMap<String, String>();
    for (int i = 0; i < node.getChildCount(); ++i) {
      ASTNode child = (ASTNode) node.getChild(i);
      String key = child.getChild(0).getText();
      String val = null;
      if (child.getChildCount() > 1) {
        val = stripQuotes(child.getChild(1).getText());
      }
      partSpec.put(key.toLowerCase(), val);
    }
    return partSpec;
  }

  public static void validatePartSpec(Table tbl, Map<String, String> partSpec,
      ASTNode astNode, HiveConf conf, boolean shouldBeFull) throws SemanticException {
    tbl.validatePartColumnNames(partSpec, shouldBeFull);
    validatePartColumnType(tbl, partSpec, astNode, conf);
  }

  public static void validatePartColumnType(Table tbl, Map<String, String> partSpec,
      ASTNode astNode, HiveConf conf) throws SemanticException {
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_TYPE_CHECK_ON_INSERT)) {
      return;
    }

    Map<ASTNode, ExprNodeDesc> astExprNodeMap = new HashMap<ASTNode, ExprNodeDesc>();
    if (!getPartExprNodeDesc(astNode, conf, astExprNodeMap)) {
      STATIC_LOG.warn("Dynamic partitioning is used; only validating "
          + astExprNodeMap.size() + " columns");
    }

    if (astExprNodeMap.isEmpty()) {
      return; // All columns are dynamic, nothing to do.
    }

    List<FieldSchema> parts = tbl.getPartitionKeys();
    Map<String, String> partCols = new HashMap<String, String>(parts.size());
    for (FieldSchema col : parts) {
      partCols.put(col.getName(), col.getType().toLowerCase());
    }
    for (Entry<ASTNode, ExprNodeDesc> astExprNodePair : astExprNodeMap.entrySet()) {
      String astKeyName = astExprNodePair.getKey().toString().toLowerCase();
      if (astExprNodePair.getKey().getType() == HiveParser.Identifier) {
        astKeyName = stripIdentifierQuotes(astKeyName);
      }
      String colType = partCols.get(astKeyName);
      ObjectInspector inputOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo
          (astExprNodePair.getValue().getTypeInfo());

      TypeInfo expectedType =
          TypeInfoUtils.getTypeInfoFromTypeString(colType);
      ObjectInspector outputOI =
          TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(expectedType);
      //  Since partVal is a constant, it is safe to cast ExprNodeDesc to ExprNodeConstantDesc.
      //  Its value should be in normalized format (e.g. no leading zero in integer, date is in
      //  format of YYYY-MM-DD etc)
      Object value = ((ExprNodeConstantDesc)astExprNodePair.getValue()).getValue();
      Object convertedValue = value;
      if (!inputOI.getTypeName().equals(outputOI.getTypeName())) {
        convertedValue = ObjectInspectorConverters.getConverter(inputOI, outputOI).convert(value);
        if (convertedValue == null) {
          throw new SemanticException(ErrorMsg.PARTITION_SPEC_TYPE_MISMATCH, astKeyName,
              inputOI.getTypeName(), outputOI.getTypeName());
        }

        if (!convertedValue.toString().equals(value.toString())) {
          //  value might have been changed because of the normalization in conversion
          STATIC_LOG.warn("Partition " + astKeyName + " expects type " + outputOI.getTypeName()
          + " but input value is in type " + inputOI.getTypeName() + ". Convert "
          + value.toString() + " to " + convertedValue.toString());
        }
      }

      if (!convertedValue.toString().equals(partSpec.get(astKeyName))) {
        STATIC_LOG.warn("Partition Spec " + astKeyName + "=" + partSpec.get(astKeyName)
            + " has been changed to " + astKeyName + "=" + convertedValue.toString());
      }
      partSpec.put(astKeyName, convertedValue.toString());
    }
  }

  @VisibleForTesting
  static void normalizeColSpec(Map<String, String> partSpec, String colName,
      String colType, String originalColSpec, Object colValue) throws SemanticException {
    if (colValue == null) {
      return; // nothing to do with nulls
    }
    String normalizedColSpec = originalColSpec;
    if (colType.equals(serdeConstants.DATE_TYPE_NAME)) {
      normalizedColSpec = normalizeDateCol(colValue);
    }
    if (!normalizedColSpec.equals(originalColSpec)) {
      STATIC_LOG.warn("Normalizing partition spec - " + colName + " from "
          + originalColSpec + " to " + normalizedColSpec);
      partSpec.put(colName, normalizedColSpec);
    }
  }

  private static String normalizeDateCol(Object colValue) throws SemanticException {
    Date value;
    if (colValue instanceof DateWritableV2) {
      value = ((DateWritableV2) colValue).get(); // Time doesn't matter.
    } else if (colValue instanceof Date) {
      value = (Date) colValue;
    } else {
      throw new SemanticException("Unexpected date type " + colValue.getClass());
    }
    try {
      return MetaStoreUtils.PARTITION_DATE_FORMAT.get().format(
          MetaStoreUtils.PARTITION_DATE_FORMAT.get().parse(value.toString()));
    } catch (ParseException e) {
      throw new SemanticException(e);
    }
  }

  protected WriteEntity toWriteEntity(String location) throws SemanticException {
    return toWriteEntity(new Path(location));
  }

  protected WriteEntity toWriteEntity(Path location) throws SemanticException {
    return toWriteEntity(location,conf);
  }

  public static WriteEntity toWriteEntity(Path location, HiveConf conf) throws SemanticException {
    try {
      Path path = tryQualifyPath(location,conf);
      return new WriteEntity(path, FileUtils.isLocalFile(conf, path.toUri()));
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  protected ReadEntity toReadEntity(String location) throws SemanticException {
    return toReadEntity(new Path(location));
  }

  protected ReadEntity toReadEntity(Path location) throws SemanticException {
    return toReadEntity(location, conf);
  }

  public static ReadEntity toReadEntity(Path location, HiveConf conf) throws SemanticException {
    try {
      Path path = tryQualifyPath(location, conf);
      return new ReadEntity(path, FileUtils.isLocalFile(conf, path.toUri()));
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  public static Path tryQualifyPath(Path path, HiveConf conf) throws IOException {
    try {
      return path.getFileSystem(conf).makeQualified(path);
    } catch (IOException e) {
      return path;  // some tests expected to pass invalid schema
    }
  }

  protected Database getDatabase(String dbName) throws SemanticException {
    return getDatabase(dbName, true);
  }

  protected Database getDatabase(String dbName, boolean throwException) throws SemanticException {
    Database database;
    try {
      database = db.getDatabase(dbName);
    } catch (Exception e) {
      throw new SemanticException(e.getMessage(), e);
    }
    if (database == null && throwException) {
      throw new SemanticException(ErrorMsg.DATABASE_NOT_EXISTS.getMsg(dbName));
    }
    return database;
  }

  protected Table getTable(TableName tn) throws SemanticException {
    return getTable(tn, true);
  }

  protected Table getTable(TableName tn, boolean throwException) throws SemanticException {
    return getTable(tn.getDb(), tn.getTable(), throwException);
  }

  protected Table getTable(String tblName) throws SemanticException {
    return getTable(null, tblName, true);
  }

  protected Table getTable(String tblName, boolean throwException) throws SemanticException {
    return getTable(null, tblName, throwException);
  }

  protected Table getTable(String database, String tblName, boolean throwException)
      throws SemanticException {
    Table tab;
    try {
      tab = database == null ? db.getTable(tblName, false)
          : db.getTable(database, tblName, false);
    }
    catch (InvalidTableException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(TableName.fromString(tblName, null, database).getNotEmptyDbTable()), e);
    }
    catch (Exception e) {
      throw new SemanticException(e.getMessage(), e);
    }
    if (tab == null && throwException) {
      // getTable needs a refactor with all ~50 occurences
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(TableName.fromString(tblName, null, database).getNotEmptyDbTable()));
    }
    return tab;
  }

  protected Partition getPartition(Table table, Map<String, String> partSpec,
      boolean throwException) throws SemanticException {
    Partition partition;
    try {
      partition = db.getPartition(table, partSpec, false);
    } catch (Exception e) {
      throw new SemanticException(toMessage(ErrorMsg.INVALID_PARTITION, partSpec), e);
    }
    if (partition == null && throwException) {
      throw new SemanticException(toMessage(ErrorMsg.INVALID_PARTITION, partSpec));
    }
    return partition;
  }

  protected List<Partition> getPartitions(Table table, Map<String, String> partSpec,
      boolean throwException) throws SemanticException {
    List<Partition> partitions;
    try {
      partitions = partSpec == null ? db.getPartitions(table) :
          db.getPartitions(table, partSpec);
    } catch (Exception e) {
      throw new SemanticException(toMessage(ErrorMsg.INVALID_PARTITION, partSpec), e);
    }
    if (partitions.isEmpty() && throwException) {
      throw new SemanticException(toMessage(ErrorMsg.INVALID_PARTITION, partSpec));
    }
    return partitions;
  }

  protected String toMessage(ErrorMsg message, Object detail) {
    return detail == null ? message.getMsg() : message.getMsg(detail.toString());
  }

  public List<Task<?>> getAllRootTasks() {
    return rootTasks;
  }

  public Set<ReadEntity> getAllInputs() {
    return inputs;
  }

  public Set<WriteEntity> getAllOutputs() {
    return outputs;
  }

  public QueryState getQueryState() {
    return queryState;
  }

  /**
   * Create a FetchTask for a given schema.
   */
  protected FetchTask createFetchTask(String tableSchema) {
    String schema =
        "json".equals(conf.get(HiveConf.ConfVars.HIVE_DDL_OUTPUT_FORMAT.varname, "text")) ? "json#string" : tableSchema;

    Properties prop = new Properties();
    // Sets delimiter to tab (ascii 9)
    prop.setProperty(serdeConstants.SERIALIZATION_FORMAT, Integer.toString(Utilities.tabCode));
    prop.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, " ");
    String[] colTypes = schema.split("#");
    prop.setProperty("columns", colTypes[0]);
    prop.setProperty("columns.types", colTypes[1]);
    prop.setProperty(serdeConstants.SERIALIZATION_LIB, LazySimpleSerDe.class.getName());
    FetchWork fetch =
        new FetchWork(ctx.getResFile(), new TableDesc(TextInputFormat.class,
            IgnoreKeyTextOutputFormat.class, prop), -1);
    fetch.setSerializationNullFormat(" ");
    return (FetchTask) TaskFactory.get(fetch);
  }

  protected HiveTxnManager getTxnMgr() {
    if (txnManager != null) {
      return txnManager;
    }
    return SessionState.get().getTxnMgr();
  }

  public CacheUsage getCacheUsage() {
    return cacheUsage;
  }

  public void setCacheUsage(CacheUsage cacheUsage) {
    this.cacheUsage = cacheUsage;
  }

  public DDLDescWithWriteId getAcidDdlDesc() {
    return null;
  }

  public WriteEntity getAcidAnalyzeTable() {
    return null;
  }

  public void addPropertyReadEntry(Map<String, String> tblProps, Set<ReadEntity> inputs) throws SemanticException {
    if (tblProps.containsKey(Constants.JDBC_KEYSTORE)) {
      try {
        String keystore = tblProps.get(Constants.JDBC_KEYSTORE);
        Configuration conf = new Configuration();
        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, keystore);
        boolean found = false;
        for (CredentialProvider provider : CredentialProviderFactory.getProviders(conf)) {
          if (provider instanceof AbstractJavaKeyStoreProvider) {
            Path path = ((AbstractJavaKeyStoreProvider) provider).getPath();
            inputs.add(toReadEntity(path));
            found = true;
          }
        }
        if (!found) {
          throw new SemanticException("Cannot recognize keystore " + keystore + ", only JavaKeyStoreProvider is " +
                  "supported");
        }
      } catch (IOException e) {
        throw new SemanticException(e);
      }
    }
  }

  /**
   * Unparses the analyzed statement
   */
  protected void executeUnparseTranlations() {
    UnparseTranslator unparseTranslator = new UnparseTranslator(conf);
    unparseTranslator.applyTranslations(ctx.getTokenRewriteStream());
  }

}
