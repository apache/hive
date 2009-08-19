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

import java.util.*;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeNullDesc;
import org.apache.hadoop.hive.ql.udf.UDFOPAnd;
import org.apache.hadoop.hive.ql.udf.UDFOPNot;
import org.apache.hadoop.hive.ql.udf.UDFOPOr;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ASTPartitionPruner {
    
  // The log
  @SuppressWarnings("nls")
  private static final Log LOG = LogFactory.getLog("hive.ql.parse.PartitionPruner");
 
  private String tableAlias;

  private QBMetaData metaData;
  
  private Table tab;

  private exprNodeDesc prunerExpr;
  
  private HiveConf conf;
  
  // is set to true if the expression only contains partitioning columns and not any other column reference.
  // This is used to optimize select * from table where ... scenario, when the where condition only references
  // partitioning columns - the partitions are identified and streamed directly to the client without requiring 
  // a map-reduce job
  private boolean onlyContainsPartCols;

  public ASTPartitionPruner() {  
  }
  
  /** Creates a new instance of PartitionPruner */
  public ASTPartitionPruner(String tableAlias, QBMetaData metaData, HiveConf conf) {
    this.tableAlias = tableAlias;
    this.metaData = metaData;
    this.tab = metaData.getTableForAlias(tableAlias);
    this.prunerExpr = null;
    this.conf = conf;
    onlyContainsPartCols = true;
  }

  public boolean onlyContainsPartitionCols() {
    return onlyContainsPartCols;
  }
  
  /** Class to store the return result of genExprNodeDesc.
   * 
   *  TODO: In the future when we refactor the PartitionPruner code, we should
   *  use the same code (GraphWalker) as it is now in TypeCheckProcFactory. 
   *  We should use NULL to represent a table name node, and the DOT operator
   *  should descend into the sub tree for 2 levels in order to find out the
   *  table name.  The benefit is that we get rid of another concept class -
   *  here it is ExprNodeTempDesc - the return value of a branch in the 
   *  Expression Syntax Tree, which is different from the value of a branch in
   *  the Expression Evaluation Tree.  
   *     
   */
  static class ExprNodeTempDesc {

    public ExprNodeTempDesc(exprNodeDesc desc) {
      isTableName = false;
      this.desc = desc;
    }
    
    public ExprNodeTempDesc(String tableName) {
      isTableName = true;
      this.tableName = tableName;
    }
    
    public boolean getIsTableName() {
      return isTableName;
    }
    
    public exprNodeDesc getDesc() {
      return desc;
    }
    
    public String getTableName() {
      return tableName;
    }
    
    boolean isTableName;
    exprNodeDesc desc;
    String tableName;
    
    public String toString() {
      if (isTableName) {
        return "Table:" + tableName;
      } else {
        return "Desc: " + desc;
      }
    }
  };
  
  static ExprNodeTempDesc genSimpleExprNodeDesc(ASTNode expr) throws SemanticException {
    exprNodeDesc desc = null;
    switch(expr.getType()) {
      case HiveParser.TOK_NULL:
        desc = new exprNodeNullDesc();
        break;
      case HiveParser.Identifier:
        // This is the case for an XPATH element (like "c" in "a.b.c.d")
        desc = new exprNodeConstantDesc(
            TypeInfoFactory.stringTypeInfo, 
                SemanticAnalyzer.unescapeIdentifier(expr.getText()));
        break;
      case HiveParser.Number:
        Number v = null;
        try {
          v = Double.valueOf(expr.getText());
          v = Long.valueOf(expr.getText());
          v = Integer.valueOf(expr.getText());
        } catch (NumberFormatException e) {
          // do nothing here, we will throw an exception in the following block
        }
        if (v == null) {
          throw new SemanticException(ErrorMsg.INVALID_NUMERICAL_CONSTANT.getMsg(expr));
        }
        desc = new exprNodeConstantDesc(v);
        break;
      case HiveParser.StringLiteral:
        desc = new exprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, BaseSemanticAnalyzer.unescapeSQLString(expr.getText()));
        break;
      case HiveParser.TOK_CHARSETLITERAL:
        desc = new exprNodeConstantDesc(BaseSemanticAnalyzer.charSetString(expr.getChild(0).getText(), expr.getChild(1).getText()));
        break;
      case HiveParser.KW_TRUE:
        desc = new exprNodeConstantDesc(Boolean.TRUE);
        break;
      case HiveParser.KW_FALSE:
        desc = new exprNodeConstantDesc(Boolean.FALSE);
        break;
    }
    return desc == null ? null : new ExprNodeTempDesc(desc);
  }
  
  /**
   * We use exprNodeConstantDesc(class,null) to represent unknown values.
   * Except UDFOPAnd, UDFOPOr, and UDFOPNot, all UDFs are assumed to return unknown values 
   * if any of the arguments are unknown.  
   *  
   * @param expr
   * @return The expression desc, will NEVER be null.
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private ExprNodeTempDesc genExprNodeDesc(ASTNode expr)
  throws SemanticException {
    //  We recursively create the exprNodeDesc.  Base cases:  when we encounter 
    //  a column ref, we convert that into an exprNodeColumnDesc;  when we encounter 
    //  a constant, we convert that into an exprNodeConstantDesc.  For others we just 
    //  build the exprNodeFuncDesc with recursively built children.

    //  Is this a simple expr node (not a TOK_COLREF or a TOK_FUNCTION or an operator)?
    ExprNodeTempDesc tempDesc = genSimpleExprNodeDesc(expr);
    if (tempDesc != null) {
      return tempDesc;
    }

    int tokType = expr.getType();
    switch (tokType) {
      case HiveParser.TOK_TABLE_OR_COL: {
        String tableOrCol = BaseSemanticAnalyzer.unescapeIdentifier(expr.getChild(0).getText());
        
        if (metaData.getAliasToTable().get(tableOrCol.toLowerCase()) != null) {
          // It's a table name
          tempDesc = new ExprNodeTempDesc(tableOrCol);
        } else {
          // It's a column
          String colName = tableOrCol;
          String tabAlias = SemanticAnalyzer.getTabAliasForCol(this.metaData, colName, (ASTNode)expr.getChild(0));
          LOG.debug("getTableColumnDesc(" + tabAlias + ", " + colName);
          tempDesc = getTableColumnDesc(tabAlias, colName);
        }
        break;
      }
      

      default: {
        
        boolean isFunction = (expr.getType() == HiveParser.TOK_FUNCTION);
        
        // Create all children
        int childrenBegin = (isFunction ? 1 : 0);
        ArrayList<ExprNodeTempDesc> tempChildren = new ArrayList<ExprNodeTempDesc>(expr.getChildCount() - childrenBegin);
        for (int ci=childrenBegin; ci<expr.getChildCount(); ci++) {
          ExprNodeTempDesc child = genExprNodeDesc((ASTNode)expr.getChild(ci));
          tempChildren.add(child);
        }

        // Is it a special case: table DOT column?
        if (expr.getType() == HiveParser.DOT && tempChildren.get(0).getIsTableName()) {
          String tabAlias = tempChildren.get(0).getTableName();
          String colName = ((exprNodeConstantDesc) tempChildren.get(1).getDesc()).getValue().toString();
          tempDesc = getTableColumnDesc(tabAlias, colName);
          
        } else {
        
          ArrayList<exprNodeDesc> children = new ArrayList<exprNodeDesc>(expr.getChildCount() - childrenBegin);
          for (int ci=0; ci<tempChildren.size(); ci++) {
            children.add(tempChildren.get(ci).getDesc());
          }
          
          // Create function desc
          exprNodeDesc desc = null;
          try {
            desc = TypeCheckProcFactory.DefaultExprProcessor.getXpathOrFuncExprNodeDesc(expr, isFunction, children);
          } catch (UDFArgumentTypeException e) {
            throw new SemanticException(ErrorMsg.INVALID_ARGUMENT_TYPE
                .getMsg(expr.getChild(childrenBegin + e.getArgumentId()), e.getMessage()));
          } catch (UDFArgumentLengthException e) {
            throw new SemanticException(ErrorMsg.INVALID_ARGUMENT_LENGTH
                .getMsg(expr, e.getMessage()));
          } catch (UDFArgumentException e) {
            throw new SemanticException(ErrorMsg.INVALID_ARGUMENT
                .getMsg(expr, e.getMessage()));
          }
          
          if (FunctionRegistry.isOpAndOrNot(desc)) {
            // do nothing because "And" and "Or" and "Not" supports null value evaluation
            // NOTE: In the future all UDFs that treats null value as UNKNOWN (both in parameters and return 
            // values) should derive from a common base class UDFNullAsUnknown, so instead of listing the classes
            // here we would test whether a class is derived from that base class. 
          } else if (mightBeUnknown(desc) ||
                     ((desc instanceof exprNodeGenericFuncDesc) && 
                         !FunctionRegistry.isDeterministic(((exprNodeGenericFuncDesc)desc).getGenericUDF())))
          {
            // If its a non-deterministic UDF or if any child is null, set this node to null
            LOG.trace("Pruner function might be unknown: " + expr.toStringTree());
            desc = new exprNodeConstantDesc(desc.getTypeInfo(), null);
          }

          tempDesc = new ExprNodeTempDesc(desc);
        }
        break;
      }
    }
    return tempDesc;
  }

  private ExprNodeTempDesc getTableColumnDesc(String tabAlias, String colName) {
    ExprNodeTempDesc desc;
    try {
      Table t = this.metaData.getTableForAlias(tabAlias);
      if (t.isPartitionKey(colName)) {
        // Set value to null if it's not partition column
        if (tabAlias.equalsIgnoreCase(tableAlias)) {
          desc = new ExprNodeTempDesc(new exprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, 
                                                             colName, tabAlias, true)); 
        } else {                
          desc = new ExprNodeTempDesc(new exprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, null));
        }
      } else {
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(
            this.metaData.getTableForAlias(tabAlias).getDeserializer().getObjectInspector());
        desc = new ExprNodeTempDesc(
            new exprNodeConstantDesc(((StructTypeInfo)typeInfo).getStructFieldTypeInfo(colName), null));
        onlyContainsPartCols = false;
      }
    } catch (SerDeException e){
      throw new RuntimeException(e);
    }
    return desc;
  }  
  
  public static boolean mightBeUnknown(exprNodeDesc desc) {
    if (desc instanceof exprNodeConstantDesc) {
      exprNodeConstantDesc d = (exprNodeConstantDesc)desc;
      return d.getValue() == null;
    } else if (desc instanceof exprNodeNullDesc) {
      return false;
    } else if (desc instanceof exprNodeFieldDesc) {
      exprNodeFieldDesc d = (exprNodeFieldDesc)desc;
      return mightBeUnknown(d.getDesc());
    } else if (desc instanceof exprNodeGenericFuncDesc) {
      exprNodeGenericFuncDesc d = (exprNodeGenericFuncDesc)desc;
      for(int i=0; i<d.getChildren().size(); i++) {
        if (mightBeUnknown(d.getChildExprs().get(i))) {
          return true;
        }
      }
      return false;
    } else if (desc instanceof exprNodeColumnDesc) {
      return false;
    }
    return false;
  }
  
  public boolean hasPartitionPredicate(ASTNode expr) {

    int tokType = expr.getType();
    boolean hasPPred = false;
    switch (tokType) {
      case HiveParser.TOK_TABLE_OR_COL: {
        // Must be a column. If it's a table then it's already processed by "case DOT" below. 
        String colName = BaseSemanticAnalyzer.unescapeIdentifier(expr.getChild(0).getText());

        return tab.isPartitionKey(colName);
      }
      case HiveParser.DOT: {
        assert(expr.getChildCount() == 2);
        ASTNode left = (ASTNode)expr.getChild(0);
        ASTNode right = (ASTNode)expr.getChild(1);
        
        if (left.getType() == HiveParser.TOK_TABLE_OR_COL) {
          // Is "left" a table?
          String tableOrCol = BaseSemanticAnalyzer.unescapeIdentifier(left.getChild(0).getText());
          if (metaData.getAliasToTable().get(tableOrCol.toLowerCase()) != null) {
            // "left" is a table
            String colName = BaseSemanticAnalyzer.unescapeIdentifier(right.getText());
            return tableAlias.equalsIgnoreCase(tableOrCol) && tab.isPartitionKey(colName);
          }
        }
        // else fall through to default
      }
      default: {
        boolean isFunction = (expr.getType() == HiveParser.TOK_FUNCTION);
        
        // Create all children
        int childrenBegin = (isFunction ? 1 : 0);
        for (int ci=childrenBegin; ci<expr.getChildCount(); ci++) {
          hasPPred = (hasPPred || hasPartitionPredicate((ASTNode)expr.getChild(ci)));
        }
        break;
      }
    }

    return hasPPred;
  }

  /** Add an expression */
  @SuppressWarnings("nls")
  public void addExpression(ASTNode expr) throws SemanticException {
    LOG.debug("adding pruning Tree = " + expr.toStringTree());
    ExprNodeTempDesc temp = genExprNodeDesc(expr);
    LOG.debug("new pruning Tree = " + temp);
    exprNodeDesc desc = temp.getDesc();
    // Ignore null constant expressions
    if (!(desc instanceof exprNodeConstantDesc) || ((exprNodeConstantDesc)desc).getValue() != null ) {
      LOG.trace("adding pruning expr = " + desc);
      if (this.prunerExpr == null)
        this.prunerExpr = desc;
      else
        this.prunerExpr = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("OR", this.prunerExpr, desc);
    }
  }

  /** 
   * Add an expression from the JOIN condition. Since these expressions will be used for all the where clauses, they 
   * are always ANDed. Then we walk through the remaining filters (in the where clause) and OR them with the existing
   * condition.
   */
  @SuppressWarnings("nls")
  public void addJoinOnExpression(ASTNode expr) throws SemanticException {
    LOG.trace("adding pruning Tree = " + expr.toStringTree());
    exprNodeDesc desc = genExprNodeDesc(expr).getDesc();
    // Ignore null constant expressions
    if (!(desc instanceof exprNodeConstantDesc) || ((exprNodeConstantDesc)desc).getValue() != null ) {
      LOG.trace("adding pruning expr = " + desc);
      if (this.prunerExpr == null)
        this.prunerExpr = desc;
      else
        this.prunerExpr = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("AND", this.prunerExpr, desc);
    }
  }

  /** 
   * From the table metadata prune the partitions to return the partitions.
   * Evaluate the parition pruner for each partition and return confirmed and unknown partitions separately
   */
  @SuppressWarnings("nls")
  public PrunedPartitionList prune() throws HiveException {
    return org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner.prune(this.tab,
        this.prunerExpr, conf, this.tableAlias);
  }

  public Table getTable() {
    return this.tab;
  }
}
