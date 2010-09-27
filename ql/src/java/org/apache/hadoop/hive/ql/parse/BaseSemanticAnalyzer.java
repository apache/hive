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

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * BaseSemanticAnalyzer.
 *
 */
public abstract class BaseSemanticAnalyzer {
  protected final Hive db;
  protected final HiveConf conf;
  protected List<Task<? extends Serializable>> rootTasks;
  protected FetchTask fetchTask;
  protected final Log LOG;
  protected final LogHelper console;

  protected Context ctx;
  protected HashMap<String, String> idToTableNameMap;

  public static int HIVE_COLUMN_ORDER_ASC = 1;
  public static int HIVE_COLUMN_ORDER_DESC = 0;

  /**
   * ReadEntitites that are passed to the hooks.
   */
  protected HashSet<ReadEntity> inputs;
  /**
   * List of WriteEntities that are passed to the hooks.
   */
  protected HashSet<WriteEntity> outputs;
  /**
   * Lineage information for the query.
   */
  protected LineageInfo linfo;

  protected static final String TEXTFILE_INPUT = TextInputFormat.class
      .getName();
  protected static final String TEXTFILE_OUTPUT = IgnoreKeyTextOutputFormat.class
      .getName();
  protected static final String SEQUENCEFILE_INPUT = SequenceFileInputFormat.class
      .getName();
  protected static final String SEQUENCEFILE_OUTPUT = SequenceFileOutputFormat.class
      .getName();
  protected static final String RCFILE_INPUT = RCFileInputFormat.class
      .getName();
  protected static final String RCFILE_OUTPUT = RCFileOutputFormat.class
      .getName();
  protected static final String COLUMNAR_SERDE = ColumnarSerDe.class.getName();

  class RowFormatParams {
    String fieldDelim = null;
    String fieldEscape = null;
    String collItemDelim = null;
    String mapKeyDelim = null;
    String lineDelim = null;

    protected void analyzeRowFormat(AnalyzeCreateCommonVars shared, ASTNode child) throws SemanticException {
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
            throw new SemanticException(
                ErrorMsg.LINES_TERMINATED_BY_NON_NEWLINE.getMsg());
          }
          break;
        default:
          assert false;
        }
      }
    }
  }

  class AnalyzeCreateCommonVars {
    String serde = null;
    Map<String, String> serdeProps = new HashMap<String, String>();
  }

  class StorageFormat {
    String inputFormat = null;
    String outputFormat = null;
    String storageHandler = null;

    protected boolean fillStorageFormat(ASTNode child, AnalyzeCreateCommonVars shared) {
      boolean storageFormat = false;
      switch(child.getToken().getType()) {
      case HiveParser.TOK_TBLSEQUENCEFILE:
        inputFormat = SEQUENCEFILE_INPUT;
        outputFormat = SEQUENCEFILE_OUTPUT;
        storageFormat = true;
        break;
      case HiveParser.TOK_TBLTEXTFILE:
        inputFormat = TEXTFILE_INPUT;
        outputFormat = TEXTFILE_OUTPUT;
        storageFormat = true;
        break;
      case HiveParser.TOK_TBLRCFILE:
        inputFormat = RCFILE_INPUT;
        outputFormat = RCFILE_OUTPUT;
        shared.serde = COLUMNAR_SERDE;
        storageFormat = true;
        break;
      case HiveParser.TOK_TABLEFILEFORMAT:
        inputFormat = unescapeSQLString(child.getChild(0).getText());
        outputFormat = unescapeSQLString(child.getChild(1).getText());
        storageFormat = true;
        break;
      case HiveParser.TOK_STORAGEHANDLER:
        storageHandler = unescapeSQLString(child.getChild(0).getText());
        if (child.getChildCount() == 2) {
          readProps(
            (ASTNode) (child.getChild(1).getChild(0)),
            shared.serdeProps);
        }
        storageFormat = true;
        break;
      }
      return storageFormat;
    }

    protected void fillDefaultStorageFormat(AnalyzeCreateCommonVars shared) {
      if ((inputFormat == null) && (storageHandler == null)) {
        if ("SequenceFile".equalsIgnoreCase(conf.getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT))) {
          inputFormat = SEQUENCEFILE_INPUT;
          outputFormat = SEQUENCEFILE_OUTPUT;
        } else if ("RCFile".equalsIgnoreCase(conf.getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT))) {
          inputFormat = RCFILE_INPUT;
          outputFormat = RCFILE_OUTPUT;
          shared.serde = COLUMNAR_SERDE;
        } else {
          inputFormat = TEXTFILE_INPUT;
          outputFormat = TEXTFILE_OUTPUT;
        }
      }
    }

  }

  public BaseSemanticAnalyzer(HiveConf conf) throws SemanticException {
    try {
      this.conf = conf;
      db = Hive.get(conf);
      rootTasks = new ArrayList<Task<? extends Serializable>>();
      LOG = LogFactory.getLog(this.getClass().getName());
      console = new LogHelper(LOG);
      idToTableNameMap = new HashMap<String, String>();
      inputs = new LinkedHashSet<ReadEntity>();
      outputs = new LinkedHashSet<WriteEntity>();
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  public HashMap<String, String> getIdToTableNameMap() {
    return idToTableNameMap;
  }

  public abstract void analyzeInternal(ASTNode ast) throws SemanticException;

  public void analyze(ASTNode ast, Context ctx) throws SemanticException {
    this.ctx = ctx;
    analyzeInternal(ast);
  }

  public void validate() throws SemanticException {
    // Implementations may choose to override this
  }

  public List<Task<? extends Serializable>> getRootTasks() {
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

  protected void reset() {
    rootTasks = new ArrayList<Task<? extends Serializable>>();
  }

  public static String stripQuotes(String val) throws SemanticException {
    if ((val.charAt(0) == '\'' && val.charAt(val.length() - 1) == '\'')
        || (val.charAt(0) == '\"' && val.charAt(val.length() - 1) == '\"')) {
      val = val.substring(1, val.length() - 1);
    }
    return val;
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
          bArray[j++] = new Integer(val).byteValue();
        }

        String res = new String(bArray, charSetName);
        return res;
      }
    } catch (UnsupportedEncodingException e) {
      throw new SemanticException(e);
    }
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
      String value = unescapeSQLString(prop.getChild(propChild).getChild(1)
          .getText());
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
        // The following 2 lines are exactly what MySQL does
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

  public HashSet<ReadEntity> getInputs() {
    return inputs;
  }

  public HashSet<WriteEntity> getOutputs() {
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
    return getColumns(ast, true);
  }

  /**
   * Get the list of FieldSchema out of the ASTNode.
   */
  protected List<FieldSchema> getColumns(ASTNode ast, boolean lowerCase) throws SemanticException {
    List<FieldSchema> colList = new ArrayList<FieldSchema>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      FieldSchema col = new FieldSchema();
      ASTNode child = (ASTNode) ast.getChild(i);

      String name = child.getChild(0).getText();
      if(lowerCase) {
        name = name.toLowerCase();
      }
      // child 0 is the name of the column
      col.setName(unescapeIdentifier(name));
      // child 1 is the type of the column
      ASTNode typeChild = (ASTNode) (child.getChild(1));
      col.setType(getTypeStringFromAST(typeChild));

      // child 2 is the optional comment of the column
      if (child.getChildCount() == 3) {
        col.setComment(unescapeSQLString(child.getChild(2).getText()));
      }
      colList.add(col);
    }
    return colList;
  }

  protected List<String> getColumnNames(ASTNode ast) {
    List<String> colList = new ArrayList<String>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      colList.add(unescapeIdentifier(child.getText()).toLowerCase());
    }
    return colList;
  }

  protected List<Order> getColumnNamesOrder(ASTNode ast) {
    List<Order> colList = new ArrayList<Order>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      if (child.getToken().getType() == HiveParser.TOK_TABSORTCOLNAMEASC) {
        colList.add(new Order(unescapeIdentifier(child.getChild(0).getText()).toLowerCase(),
            HIVE_COLUMN_ORDER_ASC));
      } else {
        colList.add(new Order(unescapeIdentifier(child.getChild(0).getText()).toLowerCase(),
            HIVE_COLUMN_ORDER_DESC));
      }
    }
    return colList;
  }

  protected static String getTypeStringFromAST(ASTNode typeNode)
      throws SemanticException {
    switch (typeNode.getType()) {
    case HiveParser.TOK_LIST:
      return Constants.LIST_TYPE_NAME + "<"
          + getTypeStringFromAST((ASTNode) typeNode.getChild(0)) + ">";
    case HiveParser.TOK_MAP:
      return Constants.MAP_TYPE_NAME + "<"
          + getTypeStringFromAST((ASTNode) typeNode.getChild(0)) + ","
          + getTypeStringFromAST((ASTNode) typeNode.getChild(1)) + ">";
    case HiveParser.TOK_STRUCT:
      return getStructTypeStringFromAST(typeNode);
    default:
      return DDLSemanticAnalyzer.getTypeName(typeNode.getType());
    }
  }

  private static String getStructTypeStringFromAST(ASTNode typeNode)
      throws SemanticException {
    String typeStr = Constants.STRUCT_TYPE_NAME + "<";
    typeNode = (ASTNode) typeNode.getChild(0);
    int children = typeNode.getChildCount();
    if (children <= 0) {
      throw new SemanticException("empty struct not allowed.");
    }
    for (int i = 0; i < children; i++) {
      ASTNode child = (ASTNode) typeNode.getChild(i);
      typeStr += unescapeIdentifier(child.getChild(0).getText()) + ":";
      typeStr += getTypeStringFromAST((ASTNode) child.getChild(1));
      if (i < children - 1) {
        typeStr += ",";
      }
    }

    typeStr += ">";
    return typeStr;
  }

  /**
   * tableSpec.
   *
   */
  public static class tableSpec {
    public String tableName;
    public Table tableHandle;
    public Map<String, String> partSpec; // has to use LinkedHashMap to enforce order
    public Partition partHandle;
    public int numDynParts; // number of dynamic partition columns
    private List<Partition> partitions; // involved partitions in TableScanOperator/FileSinkOperator

    public tableSpec(Hive db, HiveConf conf, ASTNode ast)
        throws SemanticException {

      assert (ast.getToken().getType() == HiveParser.TOK_TAB || ast.getToken().getType() == HiveParser.TOK_TABTYPE);
      int childIndex = 0;
      numDynParts = 0;

      try {
        // get table metadata
        tableName = unescapeIdentifier(ast.getChild(0).getText());
        boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODE);
        if (testMode) {
          tableName = conf.getVar(HiveConf.ConfVars.HIVETESTMODEPREFIX)
              + tableName;
        }

        tableHandle = db.getTable(tableName);
      } catch (InvalidTableException ite) {
        throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(ast
            .getChild(0)), ite);
      } catch (HiveException e) {
        throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg(ast
            .getChild(childIndex), e.getMessage()), e);
      }
      // get partition metadata if partition specified
      if (ast.getChildCount() == 2) {
        childIndex = 1;
        ASTNode partspec = (ASTNode) ast.getChild(1);
        // partSpec is a mapping from partition column name to its value.
        partSpec = new LinkedHashMap<String, String>(partspec.getChildCount());
        for (int i = 0; i < partspec.getChildCount(); ++i) {
          ASTNode partspec_val = (ASTNode) partspec.getChild(i);
          String val = null;
          if (partspec_val.getChildCount() < 2) { // DP in the form of T partition (ds, hr)
            ++numDynParts;
          } else { // in the form of T partition (ds="2010-03-03")
            val = stripQuotes(partspec_val.getChild(1).getText());
          }
          partSpec.put(unescapeIdentifier(partspec_val.getChild(0).getText().toLowerCase()), val);
        }
        // check if the partition spec is valid
        if (numDynParts > 0) {
          List<FieldSchema> parts = tableHandle.getPartitionKeys();
          int numStaPart = parts.size() - numDynParts;
          if (numStaPart == 0 &&
              conf.getVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE).equalsIgnoreCase("strict")) {
            throw new SemanticException(ErrorMsg.DYNAMIC_PARTITION_STRICT_MODE.getMsg());
          }
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
        } else {
          try {
            // this doesn't create partition. partition is created in MoveTask
            partHandle = new Partition(tableHandle, partSpec, null);
        	} catch (HiveException e) {
         		throw new SemanticException(
         		    ErrorMsg.INVALID_PARTITION.getMsg(ast.getChild(childIndex)));
        	}
        }
      }
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

  protected HashMap<String, String> extractPartitionSpecs(Tree partspec)
      throws SemanticException {
    HashMap<String, String> partSpec = new LinkedHashMap<String, String>();
    for (int i = 0; i < partspec.getChildCount(); ++i) {
      CommonTree partspec_val = (CommonTree) partspec.getChild(i);
      String val = stripQuotes(partspec_val.getChild(1).getText());
      partSpec.put(partspec_val.getChild(0).getText().toLowerCase(), val);
    }
    return partSpec;
  }

}
