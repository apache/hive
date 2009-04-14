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

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFuncDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeIndexDesc;
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
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PartitionPruner {
    
  // The log
  @SuppressWarnings("nls")
  private static final Log LOG = LogFactory.getLog("hive.ql.parse.PartitionPruner");
 
  private String tableAlias;

  private QBMetaData metaData;
  
  private Table tab;

  private exprNodeDesc prunerExpr;
  
  // is set to true if the expression only contains partitioning columns and not any other column reference.
  // This is used to optimize select * from table where ... scenario, when the where condition only references
  // partitioning columns - the partitions are identified and streamed directly to the client without requiring 
  // a map-reduce job
  private boolean containsPartCols;

  /** Creates a new instance of PartitionPruner */
  public PartitionPruner(String tableAlias, QBMetaData metaData) {
    this.tableAlias = tableAlias;
    this.metaData = metaData;
    this.tab = metaData.getTableForAlias(tableAlias);
    this.prunerExpr = null;
    containsPartCols = true;
  }

  public boolean containsPartitionCols() {
    return containsPartCols;
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
  private exprNodeDesc genExprNodeDesc(ASTNode expr)
  throws SemanticException {
    //  We recursively create the exprNodeDesc.  Base cases:  when we encounter 
    //  a column ref, we convert that into an exprNodeColumnDesc;  when we encounter 
    //  a constant, we convert that into an exprNodeConstantDesc.  For others we just 
    //  build the exprNodeFuncDesc with recursively built children.

    exprNodeDesc desc = null;

    //  Is this a simple expr node (not a TOK_COLREF or a TOK_FUNCTION or an operator)?
    desc = SemanticAnalyzer.genSimpleExprNodeDesc(expr);
    if (desc != null) {
      return desc;
    }

    int tokType = expr.getType();
    switch (tokType) {
      case HiveParser.TOK_COLREF: {

        String tabAlias = null;
        String colName = null;
        if (expr.getChildCount() != 1) {
          assert(expr.getChildCount() == 2);
          tabAlias = BaseSemanticAnalyzer.unescapeIdentifier(expr.getChild(0).getText());
          colName = BaseSemanticAnalyzer.unescapeIdentifier(expr.getChild(1).getText());
        }
        else {
          colName = BaseSemanticAnalyzer.unescapeIdentifier(expr.getChild(0).getText());
          tabAlias = SemanticAnalyzer.getTabAliasForCol(this.metaData, colName, (ASTNode)expr.getChild(0));
        }

        // Set value to null if it's not partition column
        if (tabAlias.equalsIgnoreCase(tableAlias) && tab.isPartitionKey(colName)) {
          desc = new exprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, colName); 
        } else {
          try {
            // might be a column from another table
            Table t = this.metaData.getTableForAlias(tabAlias);
            if (t.isPartitionKey(colName)) {
              desc = new exprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, null);
            }
            else {
              TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(
                                                                               this.metaData.getTableForAlias(tabAlias).getDeserializer().getObjectInspector());
              desc = new exprNodeConstantDesc(((StructTypeInfo)typeInfo).getStructFieldTypeInfo(colName), null);
              containsPartCols = false;
            }
          } catch (SerDeException e){
            throw new RuntimeException(e);
          }
        }
        break;
      }

      default: {
        boolean isFunction = (expr.getType() == HiveParser.TOK_FUNCTION);
        
        // Create all children
        int childrenBegin = (isFunction ? 1 : 0);
        ArrayList<exprNodeDesc> children = new ArrayList<exprNodeDesc>(expr.getChildCount() - childrenBegin);
        for (int ci=childrenBegin; ci<expr.getChildCount(); ci++) {
          exprNodeDesc child = genExprNodeDesc((ASTNode)expr.getChild(ci));
          assert(child.getTypeInfo() != null);
          children.add(child);
        }

        // Create function desc
        desc = TypeCheckProcFactory.DefaultExprProcessor.getXpathOrFuncExprNodeDesc(expr, isFunction, children);
        
        if (desc instanceof exprNodeFuncDesc && (
            ((exprNodeFuncDesc)desc).getUDFMethod().getDeclaringClass().equals(UDFOPAnd.class) 
            || ((exprNodeFuncDesc)desc).getUDFMethod().getDeclaringClass().equals(UDFOPOr.class)
            || ((exprNodeFuncDesc)desc).getUDFMethod().getDeclaringClass().equals(UDFOPNot.class))) {
          // do nothing because "And" and "Or" and "Not" supports null value evaluation
          // NOTE: In the future all UDFs that treats null value as UNKNOWN (both in parameters and return 
          // values) should derive from a common base class UDFNullAsUnknown, so instead of listing the classes
          // here we would test whether a class is derived from that base class. 
        } else if ((desc instanceof exprNodeFuncDesc && 
            ((exprNodeFuncDesc)desc).getUDFClass().getAnnotation(UDFType.class) != null && 
            ((exprNodeFuncDesc)desc).getUDFClass().getAnnotation(UDFType.class).deterministic() == false) ||
            mightBeUnknown(desc)) {
           // If its a non-deterministic UDF or if any child is null, set this node to null
          LOG.trace("Pruner function might be unknown: " + expr.toStringTree());
          desc = new exprNodeConstantDesc(desc.getTypeInfo(), null);    
        }
        break;
      }
    }
    return desc;
  }  
  
  public static boolean mightBeUnknown(exprNodeDesc desc) {
    if (desc instanceof exprNodeConstantDesc) {
      exprNodeConstantDesc d = (exprNodeConstantDesc)desc;
      return d.getValue() == null;
    } else if (desc instanceof exprNodeNullDesc) {
      return false;
    } else if (desc instanceof exprNodeIndexDesc) {
      exprNodeIndexDesc d = (exprNodeIndexDesc)desc;
      return mightBeUnknown(d.getDesc()) || mightBeUnknown(d.getIndex());
    } else if (desc instanceof exprNodeFieldDesc) {
      exprNodeFieldDesc d = (exprNodeFieldDesc)desc;
      return mightBeUnknown(d.getDesc());
    } else if (desc instanceof exprNodeFuncDesc) {
      exprNodeFuncDesc d = (exprNodeFuncDesc)desc;
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
      case HiveParser.TOK_COLREF: {

        assert(expr.getChildCount() == 2);
        String tabAlias = BaseSemanticAnalyzer.unescapeIdentifier(expr.getChild(0).getText());
        String colName = BaseSemanticAnalyzer.unescapeIdentifier(expr.getChild(1).getText());
        if (tabAlias.equalsIgnoreCase(tableAlias) && tab.isPartitionKey(colName)) {
          hasPPred = true;
        }
        break;
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
    LOG.trace("adding pruning Tree = " + expr.toStringTree());
    exprNodeDesc desc = genExprNodeDesc(expr);
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
    exprNodeDesc desc = genExprNodeDesc(expr);
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
   * list of the partitions satisfying the pruning criteria - contains both confirmed and unknown partitions
   */
  public static class PrunedPartitionList {
    // confirmed partitions - satisfy the partition criteria
    private Set<Partition>  confirmedPartns;

    // unknown partitions - may/may not satisfy the partition criteria
    private Set<Partition>  unknownPartns;

    /**
     * @param confirmedPartns  confirmed paritions
     * @param unknownPartns    unknown partitions
     */
    public PrunedPartitionList(Set<Partition> confirmedPartns, Set<Partition> unknownPartns) {
      this.confirmedPartns  = confirmedPartns;
      this.unknownPartns    = unknownPartns;
    }

    /**
     * get confirmed partitions
     * @return confirmedPartns  confirmed paritions
     */
    public Set<Partition>  getConfirmedPartns() {
      return confirmedPartns;
    }

    /**
     * get unknown partitions
     * @return unknownPartns  unknown paritions
     */
    public Set<Partition>  getUnknownPartns() {
      return unknownPartns;
    }

    /**
     * set confirmed partitions
     * @param confirmedPartns  confirmed paritions
     */
    public void setConfirmedPartns(Set<Partition> confirmedPartns) {
      this.confirmedPartns = confirmedPartns;
    }

    /**
     * set unknown partitions
     * @param unknownPartns    unknown partitions
     */
    public void setUnknownPartns(Set<Partition> unknownPartns) {
      this.unknownPartns   = unknownPartns;
    }
  }

  /** 
   * From the table metadata prune the partitions to return the partitions.
   * Evaluate the parition pruner for each partition and return confirmed and unknown partitions separately
   */
  @SuppressWarnings("nls")
  public PrunedPartitionList prune() throws HiveException {
    LOG.trace("Started pruning partiton");
    LOG.trace("tabname = " + this.tab.getName());
    LOG.trace("prune Expression = " + this.prunerExpr);

    LinkedHashSet<Partition> true_parts = new LinkedHashSet<Partition>();
    LinkedHashSet<Partition> unkn_parts = new LinkedHashSet<Partition>();

    try {
      StructObjectInspector rowObjectInspector = (StructObjectInspector)this.tab.getDeserializer().getObjectInspector();
      Object[] rowWithPart = new Object[2];
      InspectableObject inspectableObject = new InspectableObject();
     
      ExprNodeEvaluator evaluator = null;
      if (this.prunerExpr != null)
        evaluator = ExprNodeEvaluatorFactory.get(this.prunerExpr);
      for(Partition part: Hive.get().getPartitions(this.tab)) {
        // Set all the variables here
        LinkedHashMap<String, String> partSpec = part.getSpec();

        // Create the row object
        ArrayList<String> partNames = new ArrayList<String>();
        ArrayList<String> partValues = new ArrayList<String>();
        ArrayList<ObjectInspector> partObjectInspectors = new ArrayList<ObjectInspector>();
        for(Map.Entry<String,String>entry : partSpec.entrySet()) {
          partNames.add(entry.getKey());
          partValues.add(entry.getValue());
          partObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector); 
        }
        StructObjectInspector partObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(partNames, partObjectInspectors);
        
        rowWithPart[1] = partValues;
        ArrayList<StructObjectInspector> ois = new ArrayList<StructObjectInspector>(2);
        ois.add(rowObjectInspector);
        ois.add(partObjectInspector);
        StructObjectInspector rowWithPartObjectInspector = ObjectInspectorFactory.getUnionStructObjectInspector(ois);
        
        // evaluate the expression tree
        if (evaluator != null) {
          evaluator.evaluate(rowWithPart, rowWithPartObjectInspector, inspectableObject);
          Boolean r = (Boolean) ((PrimitiveObjectInspector)inspectableObject.oi).getPrimitiveJavaObject(inspectableObject.o);
          LOG.trace("prune result for partition " + partSpec + ": " + r);
          if (Boolean.TRUE.equals(r)) {
            LOG.debug("retained partition: " + partSpec);
            true_parts.add(part);
          } 
          else if (Boolean.FALSE.equals(r)) {
            LOG.trace("pruned partition: " + partSpec);
          } 
          else {
            LOG.debug("unknown partition: " + partSpec);
            unkn_parts.add(part);
          }
        }
        else {
          // is there is no parition pruning, all of them are needed
          true_parts.add(part);
        }
      }
    }
    catch (Exception e) {
      throw new HiveException(e);
    }

    // Now return the set of partitions
    return new PrunedPartitionList(true_parts, unkn_parts);
  }

  public Table getTable() {
    return this.tab;
  }
}
