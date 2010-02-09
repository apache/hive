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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DescFunctionDesc;
import org.apache.hadoop.hive.ql.plan.DescTableDesc;
import org.apache.hadoop.hive.ql.plan.DropTableDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.MsckDesc;
import org.apache.hadoop.hive.ql.plan.ShowFunctionsDesc;
import org.apache.hadoop.hive.ql.plan.ShowPartitionsDesc;
import org.apache.hadoop.hive.ql.plan.ShowTableStatusDesc;
import org.apache.hadoop.hive.ql.plan.ShowTablesDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * DDLSemanticAnalyzer.
 *
 */
public class DDLSemanticAnalyzer extends BaseSemanticAnalyzer {
  private static final Log LOG = LogFactory.getLog("hive.ql.parse.DDLSemanticAnalyzer");
  public static final Map<Integer, String> TokenToTypeName = new HashMap<Integer, String>();
  static {
    TokenToTypeName.put(HiveParser.TOK_BOOLEAN, Constants.BOOLEAN_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_TINYINT, Constants.TINYINT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_SMALLINT, Constants.SMALLINT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_INT, Constants.INT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_BIGINT, Constants.BIGINT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_FLOAT, Constants.FLOAT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_DOUBLE, Constants.DOUBLE_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_STRING, Constants.STRING_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_DATE, Constants.DATE_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_DATETIME, Constants.DATETIME_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_TIMESTAMP, Constants.TIMESTAMP_TYPE_NAME);
  }

  public static String getTypeName(int token) throws SemanticException {
    // date, datetime, and timestamp types aren't currently supported
    if (token == HiveParser.TOK_DATE || token == HiveParser.TOK_DATETIME ||
        token == HiveParser.TOK_TIMESTAMP) {
      throw new SemanticException(ErrorMsg.UNSUPPORTED_TYPE.getMsg());
    }
    return TokenToTypeName.get(token);
  }

  public DDLSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    if (ast.getToken().getType() == HiveParser.TOK_DROPTABLE) {
      analyzeDropTable(ast, false);
    } else if (ast.getToken().getType() == HiveParser.TOK_DESCTABLE) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeDescribeTable(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOWTABLES) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowTables(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOW_TABLESTATUS) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowTableStatus(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOWFUNCTIONS) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowFunctions(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_DESCFUNCTION) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeDescFunction(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_MSCK) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeMetastoreCheck(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_DROPVIEW) {
      analyzeDropTable(ast, true);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_RENAME) {
      analyzeAlterTableRename(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ADDCOLS) {
      analyzeAlterTableModifyCols(ast, AlterTableTypes.ADDCOLS);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_REPLACECOLS) {
      analyzeAlterTableModifyCols(ast, AlterTableTypes.REPLACECOLS);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_RENAMECOL) {
      analyzeAlterTableRenameCol(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ADDPARTS) {
      analyzeAlterTableAddParts(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_DROPPARTS) {
      analyzeAlterTableDropParts(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_PROPERTIES) {
      analyzeAlterTableProps(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES) {
      analyzeAlterTableSerdeProps(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_SERIALIZER) {
      analyzeAlterTableSerde(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_FILEFORMAT) {
      analyzeAlterTableFileFormat(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_CLUSTER_SORT) {
      analyzeAlterTableClusterSort(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOWPARTITIONS) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowPartitions(ast);
    } else {
      throw new SemanticException("Unsupported command.");
    }
  }

  private void analyzeDropTable(ASTNode ast, boolean expectView)
      throws SemanticException {
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    DropTableDesc dropTblDesc = new DropTableDesc(tableName, expectView);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        dropTblDesc), conf));
  }

  private void analyzeAlterTableProps(ASTNode ast) throws SemanticException {
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    HashMap<String, String> mapProp = getProps((ASTNode) (ast.getChild(1))
        .getChild(0));
    AlterTableDesc alterTblDesc = new AlterTableDesc(AlterTableTypes.ADDPROPS);
    alterTblDesc.setProps(mapProp);
    alterTblDesc.setOldName(tableName);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTableSerdeProps(ASTNode ast)
      throws SemanticException {
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    HashMap<String, String> mapProp = getProps((ASTNode) (ast.getChild(1))
        .getChild(0));
    AlterTableDesc alterTblDesc = new AlterTableDesc(
        AlterTableTypes.ADDSERDEPROPS);
    alterTblDesc.setProps(mapProp);
    alterTblDesc.setOldName(tableName);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTableSerde(ASTNode ast) throws SemanticException {
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    String serdeName = unescapeSQLString(ast.getChild(1).getText());
    AlterTableDesc alterTblDesc = new AlterTableDesc(AlterTableTypes.ADDSERDE);
    if (ast.getChildCount() > 2) {
      HashMap<String, String> mapProp = getProps((ASTNode) (ast.getChild(2))
          .getChild(0));
      alterTblDesc.setProps(mapProp);
    }
    alterTblDesc.setOldName(tableName);
    alterTblDesc.setSerdeName(serdeName);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTableFileFormat(ASTNode ast)
      throws SemanticException {
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    String inputFormat = null;
    String outputFormat = null;
    String serde = null;
    ASTNode child = (ASTNode) ast.getChild(1);

    switch (child.getToken().getType()) {
    case HiveParser.TOK_TABLEFILEFORMAT:
      inputFormat = unescapeSQLString(((ASTNode) child.getChild(0)).getToken()
          .getText());
      outputFormat = unescapeSQLString(((ASTNode) child.getChild(1)).getToken()
          .getText());
      try {
        Class.forName(inputFormat);
        Class.forName(outputFormat);
      } catch (ClassNotFoundException e) {
        throw new SemanticException(e);
      }
      break;
    case HiveParser.TOK_TBLSEQUENCEFILE:
      inputFormat = SEQUENCEFILE_INPUT;
      outputFormat = SEQUENCEFILE_OUTPUT;
      break;
    case HiveParser.TOK_TBLTEXTFILE:
      inputFormat = TEXTFILE_INPUT;
      outputFormat = TEXTFILE_OUTPUT;
      break;
    case HiveParser.TOK_TBLRCFILE:
      inputFormat = RCFILE_INPUT;
      outputFormat = RCFILE_OUTPUT;
      serde = COLUMNAR_SERDE;
      break;
    }
    AlterTableDesc alterTblDesc = new AlterTableDesc(tableName, inputFormat,
        outputFormat, serde);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTableClusterSort(ASTNode ast)
      throws SemanticException {
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    ASTNode buckets = (ASTNode) ast.getChild(1);
    List<String> bucketCols = getColumnNames((ASTNode) buckets.getChild(0));
    List<Order> sortCols = new ArrayList<Order>();
    int numBuckets = -1;
    if (buckets.getChildCount() == 2) {
      numBuckets = (Integer.valueOf(buckets.getChild(1).getText())).intValue();
    } else {
      sortCols = getColumnNamesOrder((ASTNode) buckets.getChild(1));
      numBuckets = (Integer.valueOf(buckets.getChild(2).getText())).intValue();
    }
    if (numBuckets <= 0) {
      throw new SemanticException(ErrorMsg.INVALID_BUCKET_NUMBER.getMsg());
    }
    AlterTableDesc alterTblDesc = new AlterTableDesc(tableName, numBuckets,
        bucketCols, sortCols);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private HashMap<String, String> getProps(ASTNode prop) {
    HashMap<String, String> mapProp = new HashMap<String, String>();
    for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
      String key = unescapeSQLString(prop.getChild(propChild).getChild(0)
          .getText());
      String value = unescapeSQLString(prop.getChild(propChild).getChild(1)
          .getText());
      mapProp.put(key, value);
    }
    return mapProp;
  }

  /**
   * Get the fully qualified name in the ast. e.g. the ast of the form ^(DOT
   * ^(DOT a b) c) will generate a name of the form a.b.c
   * 
   * @param ast
   *          The AST from which the qualified name has to be extracted
   * @return String
   */
  private String getFullyQualifiedName(ASTNode ast) {
    if (ast.getChildCount() == 0) {
      return ast.getText();
    }

    return getFullyQualifiedName((ASTNode) ast.getChild(0)) + "."
        + getFullyQualifiedName((ASTNode) ast.getChild(1));
  }

  /**
   * Create a FetchTask for a given table and thrift ddl schema.
   * 
   * @param tablename
   *          tablename
   * @param schema
   *          thrift ddl
   */
  private FetchTask createFetchTask(String schema) {
    Properties prop = new Properties();

    prop.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    prop.setProperty(Constants.SERIALIZATION_NULL_FORMAT, " ");
    String[] colTypes = schema.split("#");
    prop.setProperty("columns", colTypes[0]);
    prop.setProperty("columns.types", colTypes[1]);

    FetchWork fetch = new FetchWork(ctx.getResFile().toString(), new TableDesc(
        LazySimpleSerDe.class, TextInputFormat.class,
        IgnoreKeyTextOutputFormat.class, prop), -1);
    fetch.setSerializationNullFormat(" ");
    return (FetchTask) TaskFactory.get(fetch, conf);
  }

  private void analyzeDescribeTable(ASTNode ast) throws SemanticException {
    ASTNode tableTypeExpr = (ASTNode) ast.getChild(0);
    String tableName = getFullyQualifiedName((ASTNode) tableTypeExpr
        .getChild(0));

    HashMap<String, String> partSpec = null;
    // get partition metadata if partition specified
    if (tableTypeExpr.getChildCount() == 2) {
      ASTNode partspec = (ASTNode) tableTypeExpr.getChild(1);
      partSpec = getPartSpec(partspec);
    }

    boolean isExt = ast.getChildCount() > 1;
    DescTableDesc descTblDesc = new DescTableDesc(ctx.getResFile(), tableName,
        partSpec, isExt);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        descTblDesc), conf));
    setFetchTask(createFetchTask(descTblDesc.getSchema()));
    LOG.info("analyzeDescribeTable done");
  }

  private HashMap<String, String> getPartSpec(ASTNode partspec)
      throws SemanticException {
    HashMap<String, String> partSpec = new LinkedHashMap<String, String>();
    for (int i = 0; i < partspec.getChildCount(); ++i) {
      ASTNode partspec_val = (ASTNode) partspec.getChild(i);
      String val = stripQuotes(partspec_val.getChild(1).getText());
      partSpec.put(partspec_val.getChild(0).getText().toLowerCase(), val);
    }
    return partSpec;
  }

  private void analyzeShowPartitions(ASTNode ast) throws SemanticException {
    ShowPartitionsDesc showPartsDesc;
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    showPartsDesc = new ShowPartitionsDesc(tableName, ctx.getResFile());
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showPartsDesc), conf));
    setFetchTask(createFetchTask(showPartsDesc.getSchema()));
  }

  private void analyzeShowTables(ASTNode ast) throws SemanticException {
    ShowTablesDesc showTblsDesc;
    if (ast.getChildCount() == 1) {
      String tableNames = unescapeSQLString(ast.getChild(0).getText());
      showTblsDesc = new ShowTablesDesc(ctx.getResFile(), tableNames);
    } else {
      showTblsDesc = new ShowTablesDesc(ctx.getResFile());
    }
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showTblsDesc), conf));
    setFetchTask(createFetchTask(showTblsDesc.getSchema()));
  }

  private void analyzeShowTableStatus(ASTNode ast) throws SemanticException {
    ShowTableStatusDesc showTblStatusDesc;
    String tableNames = unescapeIdentifier(ast.getChild(0).getText());
    String dbName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
    int children = ast.getChildCount();
    HashMap<String, String> partSpec = null;
    if (children >= 2) {
      if (children > 3) {
        throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());
      }
      for (int i = 1; i < children; i++) {
        ASTNode child = (ASTNode) ast.getChild(i);
        if (child.getToken().getType() == HiveParser.Identifier) {
          dbName = unescapeIdentifier(child.getText());
        } else if (child.getToken().getType() == HiveParser.TOK_PARTSPEC) {
          partSpec = getPartSpec(child);
        } else {
          throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());
        }
      }
    }
    showTblStatusDesc = new ShowTableStatusDesc(ctx.getResFile(), dbName,
        tableNames, partSpec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showTblStatusDesc), conf));
    setFetchTask(createFetchTask(showTblStatusDesc.getSchema()));
  }

  /**
   * Add the task according to the parsed command tree. This is used for the CLI
   * command "SHOW FUNCTIONS;".
   * 
   * @param ast
   *          The parsed command tree.
   * @throws SemanticException
   *           Parsin failed
   */
  private void analyzeShowFunctions(ASTNode ast) throws SemanticException {
    ShowFunctionsDesc showFuncsDesc;
    if (ast.getChildCount() == 1) {
      String funcNames = stripQuotes(ast.getChild(0).getText());
      showFuncsDesc = new ShowFunctionsDesc(ctx.getResFile(), funcNames);
    } else {
      showFuncsDesc = new ShowFunctionsDesc(ctx.getResFile());
    }
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        showFuncsDesc), conf));
    setFetchTask(createFetchTask(showFuncsDesc.getSchema()));
  }

  /**
   * Add the task according to the parsed command tree. This is used for the CLI
   * command "DESCRIBE FUNCTION;".
   * 
   * @param ast
   *          The parsed command tree.
   * @throws SemanticException
   *           Parsing failed
   */
  private void analyzeDescFunction(ASTNode ast) throws SemanticException {
    String funcName;
    boolean isExtended;

    if (ast.getChildCount() == 1) {
      funcName = stripQuotes(ast.getChild(0).getText());
      isExtended = false;
    } else if (ast.getChildCount() == 2) {
      funcName = stripQuotes(ast.getChild(0).getText());
      isExtended = true;
    } else {
      throw new SemanticException("Unexpected Tokens at DESCRIBE FUNCTION");
    }

    DescFunctionDesc descFuncDesc = new DescFunctionDesc(ctx.getResFile(),
        funcName, isExtended);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        descFuncDesc), conf));
    setFetchTask(createFetchTask(descFuncDesc.getSchema()));
  }

  private void analyzeAlterTableRename(ASTNode ast) throws SemanticException {
    AlterTableDesc alterTblDesc = new AlterTableDesc(unescapeIdentifier(ast
        .getChild(0).getText()), unescapeIdentifier(ast.getChild(1).getText()));
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTableRenameCol(ASTNode ast) throws SemanticException {
    String tblName = unescapeIdentifier(ast.getChild(0).getText());
    String newComment = null;
    String newType = null;
    newType = getTypeStringFromAST((ASTNode) ast.getChild(3));
    boolean first = false;
    String flagCol = null;
    ASTNode positionNode = null;
    if (ast.getChildCount() == 6) {
      newComment = unescapeSQLString(ast.getChild(4).getText());
      positionNode = (ASTNode) ast.getChild(5);
    } else if (ast.getChildCount() == 5) {
      if (ast.getChild(4).getType() == HiveParser.StringLiteral) {
        newComment = unescapeSQLString(ast.getChild(4).getText());
      } else {
        positionNode = (ASTNode) ast.getChild(4);
      }
    }

    if (positionNode != null) {
      if (positionNode.getChildCount() == 0) {
        first = true;
      } else {
        flagCol = unescapeIdentifier(positionNode.getChild(0).getText());
      }
    }

    AlterTableDesc alterTblDesc = new AlterTableDesc(tblName,
        unescapeIdentifier(ast.getChild(1).getText()), unescapeIdentifier(ast
        .getChild(2).getText()), newType, newComment, first, flagCol);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTableModifyCols(ASTNode ast,
      AlterTableTypes alterType) throws SemanticException {
    String tblName = unescapeIdentifier(ast.getChild(0).getText());
    List<FieldSchema> newCols = getColumns((ASTNode) ast.getChild(1));
    AlterTableDesc alterTblDesc = new AlterTableDesc(tblName, newCols,
        alterType);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTableDropParts(ASTNode ast) throws SemanticException {
    String tblName = unescapeIdentifier(ast.getChild(0).getText());
    // get table metadata
    List<Map<String, String>> partSpecs = getPartitionSpecs(ast);
    DropTableDesc dropTblDesc = new DropTableDesc(tblName, partSpecs);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        dropTblDesc), conf));
  }

  /**
   * Add one or more partitions to a table. Useful when the data has been copied
   * to the right location by some other process.
   * 
   * @param ast
   *          The parsed command tree.
   * @throws SemanticException
   *           Parsin failed
   */
  private void analyzeAlterTableAddParts(CommonTree ast)
      throws SemanticException {

    String tblName = unescapeIdentifier(ast.getChild(0).getText());
    // partition name to value
    List<Map<String, String>> partSpecs = getPartitionSpecs(ast);

    Iterator<Map<String, String>> partIter = partSpecs.iterator();

    String currentLocation = null;
    Map<String, String> currentPart = null;
    boolean ifNotExists = false;

    int numCh = ast.getChildCount();
    for (int num = 1; num < numCh; num++) {
      CommonTree child = (CommonTree) ast.getChild(num);
      switch (child.getToken().getType()) {
      case HiveParser.TOK_IFNOTEXISTS:
        ifNotExists = true;
        break;
      case HiveParser.TOK_PARTSPEC:
        if (currentPart != null) {
          AddPartitionDesc addPartitionDesc = new AddPartitionDesc(
              MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName, currentPart,
              currentLocation, ifNotExists);
          rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
              addPartitionDesc), conf));
        }
        // create new partition, set values
        currentLocation = null;
        currentPart = partIter.next();
        break;
      case HiveParser.TOK_PARTITIONLOCATION:
        // if location specified, set in partition
        currentLocation = unescapeSQLString(child.getChild(0).getText());
        break;
      default:
        throw new SemanticException("Unknown child: " + child);
      }
    }

    // add the last one
    if (currentPart != null) {
      AddPartitionDesc addPartitionDesc = new AddPartitionDesc(
          MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName, currentPart,
          currentLocation, ifNotExists);
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
          addPartitionDesc), conf));
    }
  }

  /**
   * Verify that the information in the metastore matches up with the data on
   * the fs.
   * 
   * @param ast
   *          Query tree.
   * @throws SemanticException
   */
  private void analyzeMetastoreCheck(CommonTree ast) throws SemanticException {
    String tableName = null;
    boolean repair = false;
    if (ast.getChildCount() > 0) {
      repair = ast.getChild(0).getType() == HiveParser.KW_REPAIR;
      if (!repair) {
        tableName = unescapeIdentifier(ast.getChild(0).getText());
      } else if (ast.getChildCount() > 1) {
        tableName = unescapeIdentifier(ast.getChild(1).getText());
      }
    }
    List<Map<String, String>> specs = getPartitionSpecs(ast);
    MsckDesc checkDesc = new MsckDesc(tableName, specs, ctx.getResFile(),
        repair);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        checkDesc), conf));
  }

  /**
   * Get the partition specs from the tree.
   * 
   * @param ast
   *          Tree to extract partitions from.
   * @return A list of partition name to value mappings.
   * @throws SemanticException
   */
  private List<Map<String, String>> getPartitionSpecs(CommonTree ast)
      throws SemanticException {
    List<Map<String, String>> partSpecs = new ArrayList<Map<String, String>>();
    int childIndex = 0;
    // get partition metadata if partition specified
    for (childIndex = 1; childIndex < ast.getChildCount(); childIndex++) {
      Tree partspec = ast.getChild(childIndex);
      // sanity check
      if (partspec.getType() == HiveParser.TOK_PARTSPEC) {
        Map<String, String> partSpec = new LinkedHashMap<String, String>();
        for (int i = 0; i < partspec.getChildCount(); ++i) {
          CommonTree partspec_val = (CommonTree) partspec.getChild(i);
          String val = stripQuotes(partspec_val.getChild(1).getText());
          partSpec.put(partspec_val.getChild(0).getText().toLowerCase(), val);
        }
        partSpecs.add(partSpec);
      }
    }
    return partSpecs;
  }
}
