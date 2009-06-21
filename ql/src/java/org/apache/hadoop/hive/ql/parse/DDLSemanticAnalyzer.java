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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.MsckDesc;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.alterTableDesc;
import org.apache.hadoop.hive.ql.plan.createTableDesc;
import org.apache.hadoop.hive.ql.plan.createTableLikeDesc;
import org.apache.hadoop.hive.ql.plan.descTableDesc;
import org.apache.hadoop.hive.ql.plan.dropTableDesc;
import org.apache.hadoop.hive.ql.plan.showPartitionsDesc;
import org.apache.hadoop.hive.ql.plan.showTablesDesc;
import org.apache.hadoop.hive.ql.plan.alterTableDesc.alterTableTypes;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.hive.ql.exec.Task;
import java.io.Serializable;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

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
  private static final String TEXTFILE_INPUT = TextInputFormat.class.getName();
  private static final String TEXTFILE_OUTPUT = IgnoreKeyTextOutputFormat.class.getName();
  private static final String SEQUENCEFILE_INPUT = SequenceFileInputFormat.class.getName();
  private static final String SEQUENCEFILE_OUTPUT = SequenceFileOutputFormat.class.getName();
  private static final String RCFILE_INPUT = RCFileInputFormat.class.getName();
  private static final String RCFILE_OUTPUT = RCFileOutputFormat.class.getName();

  private static final String COLUMNAR_SERDE = ColumnarSerDe.class.getName();
  
  public static String getTypeName(int token) {
    return TokenToTypeName.get(token);
  }

  public DDLSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    if (ast.getToken().getType() == HiveParser.TOK_CREATETABLE)
       analyzeCreateTable(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_DROPTABLE)
       analyzeDropTable(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_DESCTABLE)
    {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeDescribeTable(ast);
    }
    else if (ast.getToken().getType() == HiveParser.TOK_SHOWTABLES)
    {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowTables(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_MSCK) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeMetastoreCheck(ast);    
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_RENAME)
      analyzeAlterTableRename(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ADDCOLS)
      analyzeAlterTableModifyCols(ast, alterTableTypes.ADDCOLS);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_REPLACECOLS)
      analyzeAlterTableModifyCols(ast, alterTableTypes.REPLACECOLS);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ADDPARTS) {
      analyzeAlterTableAddParts(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_DROPPARTS)
      analyzeAlterTableDropParts(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_PROPERTIES)
      analyzeAlterTableProps(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES)
      analyzeAlterTableSerdeProps(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_SERIALIZER)
      analyzeAlterTableSerde(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_SHOWPARTITIONS)
    {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowPartitions(ast);
    }
    else {
      throw new SemanticException("Unsupported command.");
    }
  }

  private void analyzeCreateTable(ASTNode ast) 
    throws SemanticException {
    String            tableName     = unescapeIdentifier(ast.getChild(0).getText());
    String            likeTableName = null;
    List<FieldSchema> cols          = null;
    List<FieldSchema> partCols      = null;
    List<String>      bucketCols    = null;
    List<Order>       sortCols      = null;
    int               numBuckets    = -1;
    String            fieldDelim    = null;
    String            collItemDelim = null;
    String            mapKeyDelim   = null;
    String            lineDelim     = null;
    String            comment       = null;
    String            inputFormat   = TEXTFILE_INPUT;
    String            outputFormat  = TEXTFILE_OUTPUT;
    String            location      = null;
    String            serde         = null;
    Map<String, String> mapProp     = null;
    boolean           ifNotExists   = false;
    boolean           isExt         = false;

    if ("SequenceFile".equalsIgnoreCase(conf.getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT))) {
      inputFormat = SEQUENCEFILE_INPUT;
      outputFormat = SEQUENCEFILE_OUTPUT;
    }

    LOG.info("Creating table" + tableName);    
    int numCh = ast.getChildCount();
    for (int num = 1; num < numCh; num++)
    {
      ASTNode child = (ASTNode)ast.getChild(num);
      switch (child.getToken().getType()) {
        case HiveParser.TOK_IFNOTEXISTS:
          ifNotExists = true;
          break;
        case HiveParser.KW_EXTERNAL:
          isExt = true;
          break;
        case HiveParser.TOK_LIKETABLE:
          if (child.getChildCount() > 0) {
            likeTableName = unescapeIdentifier(child.getChild(0).getText());
          }
          break;
        case HiveParser.TOK_TABCOLLIST:
          cols = getColumns(child);
          break;
        case HiveParser.TOK_TABLECOMMENT:
          comment = unescapeSQLString(child.getChild(0).getText());
          break;
        case HiveParser.TOK_TABLEPARTCOLS:
          partCols = getColumns((ASTNode)child.getChild(0));
          break;
        case HiveParser.TOK_TABLEBUCKETS:
          bucketCols = getColumnNames((ASTNode)child.getChild(0));
          if (child.getChildCount() == 2)
            numBuckets = (Integer.valueOf(child.getChild(1).getText())).intValue();
          else
          {
            sortCols = getColumnNamesOrder((ASTNode)child.getChild(1));
            numBuckets = (Integer.valueOf(child.getChild(2).getText())).intValue();
          }
          break;
        case HiveParser.TOK_TABLEROWFORMAT:
          int numChildRowFormat = child.getChildCount();
          for (int numC = 0; numC < numChildRowFormat; numC++)
          {
            ASTNode rowChild = (ASTNode)child.getChild(numC);
            switch (rowChild.getToken().getType()) {
              case HiveParser.TOK_TABLEROWFORMATFIELD:
                fieldDelim = unescapeSQLString(rowChild.getChild(0).getText());
                break;
              case HiveParser.TOK_TABLEROWFORMATCOLLITEMS:
                collItemDelim = unescapeSQLString(rowChild.getChild(0).getText());
                break;
              case HiveParser.TOK_TABLEROWFORMATMAPKEYS:
                mapKeyDelim = unescapeSQLString(rowChild.getChild(0).getText());
                break;
              case HiveParser.TOK_TABLEROWFORMATLINES:
                lineDelim = unescapeSQLString(rowChild.getChild(0).getText());
                break;
              default: assert false;
            }
          }
          break;
        case HiveParser.TOK_TABLESERIALIZER:
          serde = unescapeSQLString(child.getChild(0).getText());
          if (child.getChildCount() == 2) {
            mapProp = new HashMap<String, String>();
            ASTNode prop = (ASTNode)((ASTNode)child.getChild(1)).getChild(0);
            for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
              String key = unescapeSQLString(prop.getChild(propChild).getChild(0).getText());
              String value = unescapeSQLString(prop.getChild(propChild).getChild(1).getText());
              mapProp.put(key,value);
            }
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
        case HiveParser.TOK_TABLEFILEFORMAT:
          inputFormat = unescapeSQLString(child.getChild(0).getText());
          outputFormat = unescapeSQLString(child.getChild(1).getText());
          break;
        case HiveParser.TOK_TABLELOCATION:
          location = unescapeSQLString(child.getChild(0).getText());
          break;
        default: assert false;
      }
    }
    if (likeTableName == null) {
      createTableDesc crtTblDesc = 
        new createTableDesc(tableName, isExt, cols, partCols, bucketCols, 
                            sortCols, numBuckets,
                            fieldDelim, collItemDelim, mapKeyDelim, lineDelim,
                            comment, inputFormat, outputFormat, location, serde, 
                            mapProp, ifNotExists);
  
      validateCreateTable(crtTblDesc);
      rootTasks.add(TaskFactory.get(new DDLWork(crtTblDesc), conf));
    } else {
      createTableLikeDesc crtTblLikeDesc = 
        new createTableLikeDesc(tableName, isExt, location, ifNotExists, likeTableName);
      rootTasks.add(TaskFactory.get(new DDLWork(crtTblLikeDesc), conf));
    }
    
  }

  private void validateCreateTable(createTableDesc crtTblDesc) throws SemanticException {
    // no duplicate column names
    // currently, it is a simple n*n algorithm - this can be optimized later if need be
    // but it should not be a major bottleneck as the number of columns are anyway not so big
    
    if((crtTblDesc.getCols() == null) || (crtTblDesc.getCols().size() == 0)) {
      // for now make sure that serde exists
      if(StringUtils.isEmpty(crtTblDesc.getSerName()) || SerDeUtils.isNativeSerDe(crtTblDesc.getSerName())) {
        throw new SemanticException(ErrorMsg.INVALID_TBL_DDL_SERDE.getMsg());
      }
      return;
    }
    
    try {
      Class<?> origin = Class.forName(crtTblDesc.getOutputFormat(), true, JavaUtils.getClassLoader());
      Class<? extends HiveOutputFormat> replaced = HiveFileFormatUtils.getOutputFormatSubstitute(origin);
      if(replaced == null)
        throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE.getMsg());
    } catch (ClassNotFoundException e) {
      throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE.getMsg());
    }
    
    Iterator<FieldSchema> iterCols = crtTblDesc.getCols().iterator();
    List<String> colNames = new ArrayList<String>();
    while (iterCols.hasNext()) {
      String colName = iterCols.next().getName();
      Iterator<String> iter = colNames.iterator();
      while (iter.hasNext()) {
        String oldColName = iter.next();
        if (colName.equalsIgnoreCase(oldColName)) 
          throw new SemanticException(ErrorMsg.DUPLICATE_COLUMN_NAMES.getMsg());
      }
      colNames.add(colName);
    }

    if (crtTblDesc.getBucketCols() != null)
    {    
      // all columns in cluster and sort are valid columns
      Iterator<String> bucketCols = crtTblDesc.getBucketCols().iterator();
      while (bucketCols.hasNext()) {
        String bucketCol = bucketCols.next();
        boolean found = false;
        Iterator<String> colNamesIter = colNames.iterator();
        while (colNamesIter.hasNext()) {
          String colName = colNamesIter.next();
          if (bucketCol.equalsIgnoreCase(colName)) {
            found = true;
            break;
          }
        }
        if (!found)
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg());
      }
    }

    if (crtTblDesc.getSortCols() != null)
    {
      // all columns in cluster and sort are valid columns
      Iterator<Order> sortCols = crtTblDesc.getSortCols().iterator();
      while (sortCols.hasNext()) {
        String sortCol = sortCols.next().getCol();
        boolean found = false;
        Iterator<String> colNamesIter = colNames.iterator();
        while (colNamesIter.hasNext()) {
          String colName = colNamesIter.next();
          if (sortCol.equalsIgnoreCase(colName)) {
            found = true;
            break;
          }
        }
        if (!found)
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg());
      }
    }
    
    if (crtTblDesc.getPartCols() != null)
    {
      // there is no overlap between columns and partitioning columns
      Iterator<FieldSchema> partColsIter = crtTblDesc.getPartCols().iterator();
      while (partColsIter.hasNext()) {
        String partCol = partColsIter.next().getName();
        Iterator<String> colNamesIter = colNames.iterator();
        while (colNamesIter.hasNext()) {
          String colName = unescapeIdentifier(colNamesIter.next());
          if (partCol.equalsIgnoreCase(colName)) 
            throw new SemanticException(ErrorMsg.COLUMN_REPEATED_IN_PARTITIONING_COLS.getMsg());
        }
      }
    }
  }
  
  private void analyzeDropTable(ASTNode ast) 
    throws SemanticException {
    String tableName = unescapeIdentifier(ast.getChild(0).getText());    
    dropTableDesc dropTblDesc = new dropTableDesc(tableName);
    rootTasks.add(TaskFactory.get(new DDLWork(dropTblDesc), conf));
  }

  private void analyzeAlterTableProps(ASTNode ast) throws SemanticException { 
    String tableName = unescapeIdentifier(ast.getChild(0).getText());    
    HashMap<String, String> mapProp = getProps((ASTNode)(ast.getChild(1)).getChild(0));
    alterTableDesc alterTblDesc = new alterTableDesc(alterTableTypes.ADDPROPS);
    alterTblDesc.setProps(mapProp);
    alterTblDesc.setOldName(tableName);
    rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
  }

  private void analyzeAlterTableSerdeProps(ASTNode ast) throws SemanticException { 
    String tableName = unescapeIdentifier(ast.getChild(0).getText());    
    HashMap<String, String> mapProp = getProps((ASTNode)(ast.getChild(1)).getChild(0));
    alterTableDesc alterTblDesc = new alterTableDesc(alterTableTypes.ADDSERDEPROPS);
    alterTblDesc.setProps(mapProp);
    alterTblDesc.setOldName(tableName);
    rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
  }

  private void analyzeAlterTableSerde(ASTNode ast) throws SemanticException { 
    String tableName = unescapeIdentifier(ast.getChild(0).getText());    
    String serdeName = unescapeSQLString(ast.getChild(1).getText());
    alterTableDesc alterTblDesc = new alterTableDesc(alterTableTypes.ADDSERDE);
    if(ast.getChildCount() > 2) {
      HashMap<String, String> mapProp = getProps((ASTNode)(ast.getChild(2)).getChild(0));
      alterTblDesc.setProps(mapProp);
    }
    alterTblDesc.setOldName(tableName);
    alterTblDesc.setSerdeName(serdeName);
    rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
  }

  private HashMap<String, String> getProps(ASTNode prop) {
    HashMap<String, String> mapProp = new HashMap<String, String>();
    for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
      String key = unescapeSQLString(prop.getChild(propChild).getChild(0).getText());
      String value = unescapeSQLString(prop.getChild(propChild).getChild(1).getText());
      mapProp.put(key,value);
    }
    return mapProp;
  }

  private List<FieldSchema> getColumns(ASTNode ast)
  {
    List<FieldSchema> colList = new ArrayList<FieldSchema>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      FieldSchema col = new FieldSchema();
      ASTNode child = (ASTNode)ast.getChild(i);
      col.setName(unescapeIdentifier(child.getChild(0).getText()));
      ASTNode typeChild = (ASTNode)(child.getChild(1));
      if (typeChild.getToken().getType() == HiveParser.TOK_LIST)
      {
        ASTNode typName = (ASTNode)typeChild.getChild(0);
        col.setType(MetaStoreUtils.getListType(getTypeName(typName.getToken().getType())));
      }
      else if (typeChild.getToken().getType() == HiveParser.TOK_MAP)
      {
        ASTNode ltypName = (ASTNode)typeChild.getChild(0);
        ASTNode rtypName = (ASTNode)typeChild.getChild(1);
        col.setType(MetaStoreUtils.getMapType(getTypeName(ltypName.getToken().getType()), getTypeName(rtypName.getToken().getType())));
      }
      else                                // primitive type
        col.setType(getTypeName(typeChild.getToken().getType()));
        
      if (child.getChildCount() == 3)
        col.setComment(unescapeSQLString(child.getChild(2).getText()));
      colList.add(col);
    }
    return colList;
  }
  
  private List<String> getColumnNames(ASTNode ast)
  {
    List<String> colList = new ArrayList<String>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      ASTNode child = (ASTNode)ast.getChild(i);
      colList.add(unescapeIdentifier(child.getText()));
    }
    return colList;
  }

  private List<Order> getColumnNamesOrder(ASTNode ast)
  {
    List<Order> colList = new ArrayList<Order>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      ASTNode child = (ASTNode)ast.getChild(i);
      if (child.getToken().getType() == HiveParser.TOK_TABSORTCOLNAMEASC)
        colList.add(new Order(unescapeIdentifier(child.getChild(0).getText()), 1));
      else
        colList.add(new Order(unescapeIdentifier(child.getChild(0).getText()), 0));
    }
    return colList;
  }

  /**
   * Get the fully qualified name in the ast. e.g. the ast of the form ^(DOT ^(DOT a b) c) 
   * will generate a name of the form a.b.c
   *
   * @param ast The AST from which the qualified name has to be extracted
   * @return String
   */
  private String getFullyQualifiedName(ASTNode ast) {
    if (ast.getChildCount() == 0) {
      return ast.getText();
    }

    return getFullyQualifiedName((ASTNode)ast.getChild(0)) + "." +
           getFullyQualifiedName((ASTNode)ast.getChild(1));
  }

  /**
   * Create a FetchTask for a given table and thrift ddl schema
   * @param tablename tablename
   * @param schema thrift ddl
   */
  private Task<? extends Serializable> createFetchTask(String schema) {
    Properties prop = new Properties();
    
    prop.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    prop.setProperty(Constants.SERIALIZATION_NULL_FORMAT, " ");
    String[] colTypes = schema.split("#");
    prop.setProperty("columns", colTypes[0]);
    prop.setProperty("columns.types", colTypes[1]);

    fetchWork fetch = new fetchWork(
      ctx.getResFile().toString(),
      new tableDesc(LazySimpleSerDe.class, TextInputFormat.class, IgnoreKeyTextOutputFormat.class, prop),
      -1
    );    
    fetch.setSerializationNullFormat(" ");
    return TaskFactory.get(fetch, this.conf);
  }

  private void analyzeDescribeTable(ASTNode ast)
  throws SemanticException {
    ASTNode tableTypeExpr = (ASTNode)ast.getChild(0);
    String tableName = getFullyQualifiedName((ASTNode)tableTypeExpr.getChild(0));

    HashMap<String, String> partSpec = null;
    // get partition metadata if partition specified
    if (tableTypeExpr.getChildCount() == 2) {
      ASTNode partspec = (ASTNode) tableTypeExpr.getChild(1);
      partSpec = new LinkedHashMap<String, String>();
      for (int i = 0; i < partspec.getChildCount(); ++i) {
        ASTNode partspec_val = (ASTNode) partspec.getChild(i);
        String val = stripQuotes(partspec_val.getChild(1).getText());
        partSpec.put(partspec_val.getChild(0).getText(), val);
      }
    }
    
    boolean isExt = ast.getChildCount() > 1;
    descTableDesc descTblDesc = new descTableDesc(ctx.getResFile(), tableName, partSpec, isExt);
    rootTasks.add(TaskFactory.get(new DDLWork(descTblDesc), conf));
    setFetchTask(createFetchTask(descTblDesc.getSchema()));
    LOG.info("analyzeDescribeTable done");
  }
  
  private void analyzeShowPartitions(ASTNode ast) 
  throws SemanticException {
    showPartitionsDesc showPartsDesc;
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    showPartsDesc = new showPartitionsDesc(tableName, ctx.getResFile());
    rootTasks.add(TaskFactory.get(new DDLWork(showPartsDesc), conf));
    setFetchTask(createFetchTask(showPartsDesc.getSchema()));
  }
  
  private void analyzeShowTables(ASTNode ast) 
  throws SemanticException {
    showTablesDesc showTblsDesc;
    if (ast.getChildCount() == 1)
    {
      String tableNames = unescapeSQLString(ast.getChild(0).getText());    
      showTblsDesc = new showTablesDesc(ctx.getResFile(), tableNames);
    }
    else {
      showTblsDesc = new showTablesDesc(ctx.getResFile());
    }
    rootTasks.add(TaskFactory.get(new DDLWork(showTblsDesc), conf));
    setFetchTask(createFetchTask(showTblsDesc.getSchema()));
  }

  private void analyzeAlterTableRename(ASTNode ast) 
  throws SemanticException {
    alterTableDesc alterTblDesc = new alterTableDesc(
        unescapeIdentifier(ast.getChild(0).getText()),
        unescapeIdentifier(ast.getChild(1).getText()));
    rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
  }

  private void analyzeAlterTableModifyCols(ASTNode ast, alterTableTypes alterType) 
  throws SemanticException {
    String tblName = unescapeIdentifier(ast.getChild(0).getText());
    List<FieldSchema> newCols = getColumns((ASTNode)ast.getChild(1));
    alterTableDesc alterTblDesc = new alterTableDesc(tblName, newCols, alterType);
    rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
  }
  
  private void analyzeAlterTableDropParts(ASTNode ast) throws SemanticException {
    String tblName = unescapeIdentifier(ast.getChild(0).getText());
    // get table metadata
    List<Map<String, String>> partSpecs = getPartitionSpecs(ast);
    dropTableDesc dropTblDesc = new dropTableDesc(tblName, partSpecs);
    rootTasks.add(TaskFactory.get(new DDLWork(dropTblDesc), conf));
  }
  
  /**
   * Add one or more partitions to a table. Useful
   * when the data has been copied to the right location
   * by some other process.
   * @param ast The parsed command tree.
   * @throws SemanticException Parsin failed
   */
  private void analyzeAlterTableAddParts(CommonTree ast) 
    throws SemanticException {
    
    String tblName = unescapeIdentifier(ast.getChild(0).getText());;
    //partition name to value
    List<Map<String, String>> partSpecs = getPartitionSpecs(ast);
        
    Iterator<Map<String, String>> partIter = partSpecs.iterator();
    
    String currentLocation = null;
    Map<String, String> currentPart = null;
    
    int numCh = ast.getChildCount();
    for (int num = 1; num < numCh; num++) {
      CommonTree child = (CommonTree)ast.getChild(num);
      switch (child.getToken().getType()) {
      case HiveParser.TOK_PARTSPEC:
        if(currentPart != null) {
          AddPartitionDesc addPartitionDesc = 
            new AddPartitionDesc(MetaStoreUtils.DEFAULT_DATABASE_NAME, 
                tblName, currentPart, currentLocation);
          rootTasks.add(TaskFactory.get(new DDLWork(addPartitionDesc), conf));
        }
        //create new partition, set values
        currentLocation = null;
        currentPart = partIter.next();
        break;
      case HiveParser.TOK_PARTITIONLOCATION:
        //if location specified, set in partition
        currentLocation = unescapeSQLString(child.getChild(0).getText());
        break;
      default:
        throw new SemanticException("Unknown child: " + child);
      }
    }
    
    //add the last one
    if(currentPart != null) {
      AddPartitionDesc addPartitionDesc = 
        new AddPartitionDesc(MetaStoreUtils.DEFAULT_DATABASE_NAME, 
            tblName, currentPart, currentLocation);
      rootTasks.add(TaskFactory.get(new DDLWork(addPartitionDesc), conf));
    }
  }  
 
  /**
   * Verify that the information in the metastore matches up
   * with the data on the fs.
   * @param ast Query tree.
   * @throws SemanticException
   */
  private void analyzeMetastoreCheck(CommonTree ast) throws SemanticException {
    String tableName = null;
    if(ast.getChildCount() > 0) {
      tableName = unescapeIdentifier(ast.getChild(0).getText());
    }
    List<Map<String, String>> specs = getPartitionSpecs(ast);
    MsckDesc checkDesc = new MsckDesc(tableName, specs, ctx.getResFile());
    rootTasks.add(TaskFactory.get(new DDLWork(checkDesc), conf));   
  }

  /**
   * Get the partition specs from the tree.
   * @param ast Tree to extract partitions from.
   * @return A list of partition name to value mappings.
   * @throws SemanticException
   */
  private List<Map<String, String>> getPartitionSpecs(CommonTree ast) throws SemanticException {
    List<Map<String, String>> partSpecs = new ArrayList<Map<String, String>>();
    int childIndex = 0;
    // get partition metadata if partition specified
    for (childIndex = 1; childIndex < ast.getChildCount(); childIndex++) {
      Tree partspec = ast.getChild(childIndex);
      //sanity check
      if(partspec.getType() == HiveParser.TOK_PARTSPEC) {
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
