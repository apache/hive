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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.*;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFConcat;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLower;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFRound;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper;

public class Vectorizer implements PhysicalPlanResolver {

  protected static transient final Log LOG = LogFactory.getLog(Vectorizer.class);

  Set<String> supportedDataTypes = new HashSet<String>();
  List<Task<? extends Serializable>> vectorizableTasks =
      new ArrayList<Task<? extends Serializable>>();
  Set<Class<?>> supportedGenericUDFs = new HashSet<Class<?>>();

  Set<String> supportedAggregationUdfs = new HashSet<String>();

  private PhysicalContext physicalContext = null;;

  public Vectorizer() {
    supportedDataTypes.add("int");
    supportedDataTypes.add("smallint");
    supportedDataTypes.add("tinyint");
    supportedDataTypes.add("bigint");
    supportedDataTypes.add("integer");
    supportedDataTypes.add("long");
    supportedDataTypes.add("short");
    supportedDataTypes.add("timestamp");
    supportedDataTypes.add("boolean");
    supportedDataTypes.add("string");
    supportedDataTypes.add("byte");
    supportedDataTypes.add("float");
    supportedDataTypes.add("double");

    supportedGenericUDFs.add(UDFOPNegative.class);
    supportedGenericUDFs.add(UDFOPPositive.class);
    supportedGenericUDFs.add(UDFOPPlus.class);
    supportedGenericUDFs.add(UDFOPMinus.class);
    supportedGenericUDFs.add(UDFOPMultiply.class);
    supportedGenericUDFs.add(UDFOPDivide.class);
    supportedGenericUDFs.add(UDFOPMod.class);

    supportedGenericUDFs.add(GenericUDFOPEqualOrLessThan.class);
    supportedGenericUDFs.add(GenericUDFOPEqualOrGreaterThan.class);
    supportedGenericUDFs.add(GenericUDFOPGreaterThan.class);
    supportedGenericUDFs.add(GenericUDFOPLessThan.class);
    supportedGenericUDFs.add(GenericUDFOPNot.class);
    supportedGenericUDFs.add(GenericUDFOPNotEqual.class);
    supportedGenericUDFs.add(GenericUDFOPNotNull.class);
    supportedGenericUDFs.add(GenericUDFOPNull.class);
    supportedGenericUDFs.add(GenericUDFOPOr.class);
    supportedGenericUDFs.add(GenericUDFOPAnd.class);
    supportedGenericUDFs.add(GenericUDFOPEqual.class);
    supportedGenericUDFs.add(GenericUDFToUnixTimeStamp.class);

    supportedGenericUDFs.add(UDFHour.class);
    supportedGenericUDFs.add(UDFLength.class);
    supportedGenericUDFs.add(UDFMinute.class);
    supportedGenericUDFs.add(UDFSecond.class);
    supportedGenericUDFs.add(UDFYear.class);
    supportedGenericUDFs.add(UDFWeekOfYear.class);
    supportedGenericUDFs.add(UDFDayOfMonth.class);

    supportedGenericUDFs.add(UDFLike.class);
    supportedGenericUDFs.add(UDFRegExp.class);
    supportedGenericUDFs.add(UDFSubstr.class);
    supportedGenericUDFs.add(UDFLTrim.class);
    supportedGenericUDFs.add(UDFRTrim.class);
    supportedGenericUDFs.add(UDFTrim.class);

    supportedGenericUDFs.add(UDFSin.class);
    supportedGenericUDFs.add(UDFCos.class);
    supportedGenericUDFs.add(UDFTan.class);
    supportedGenericUDFs.add(UDFAsin.class);
    supportedGenericUDFs.add(UDFAcos.class);
    supportedGenericUDFs.add(UDFAtan.class);
    supportedGenericUDFs.add(UDFDegrees.class);
    supportedGenericUDFs.add(UDFRadians.class);
    supportedGenericUDFs.add(UDFFloor.class);
    supportedGenericUDFs.add(UDFCeil.class);
    supportedGenericUDFs.add(UDFExp.class);
    supportedGenericUDFs.add(UDFLn.class);
    supportedGenericUDFs.add(UDFLog2.class);
    supportedGenericUDFs.add(UDFLog10.class);
    supportedGenericUDFs.add(UDFLog.class);
    supportedGenericUDFs.add(UDFPower.class);
    supportedGenericUDFs.add(UDFPosMod.class);
    supportedGenericUDFs.add(GenericUDFRound.class);
    supportedGenericUDFs.add(UDFSqrt.class);
    supportedGenericUDFs.add(UDFSign.class);
    supportedGenericUDFs.add(UDFRand.class);
    supportedGenericUDFs.add(UDFBin.class);
    supportedGenericUDFs.add(UDFHex.class);
    supportedGenericUDFs.add(UDFConv.class);

    supportedGenericUDFs.add(GenericUDFLower.class);
    supportedGenericUDFs.add(GenericUDFUpper.class);
    supportedGenericUDFs.add(GenericUDFConcat.class);
    supportedGenericUDFs.add(GenericUDFAbs.class);
    supportedGenericUDFs.add(GenericUDFBetween.class);
    supportedGenericUDFs.add(GenericUDFIn.class);

    // For type casts
    supportedGenericUDFs.add(UDFToLong.class);
    supportedGenericUDFs.add(UDFToInteger.class);
    supportedGenericUDFs.add(UDFToShort.class);
    supportedGenericUDFs.add(UDFToByte.class);
    supportedGenericUDFs.add(UDFToBoolean.class);
    supportedGenericUDFs.add(UDFToFloat.class);
    supportedGenericUDFs.add(UDFToDouble.class);
    supportedGenericUDFs.add(UDFToString.class);
    supportedGenericUDFs.add(GenericUDFTimestamp.class);

    supportedAggregationUdfs.add("min");
    supportedAggregationUdfs.add("max");
    supportedAggregationUdfs.add("count");
    supportedAggregationUdfs.add("sum");
    supportedAggregationUdfs.add("avg");
    supportedAggregationUdfs.add("variance");
    supportedAggregationUdfs.add("var_pop");
    supportedAggregationUdfs.add("var_samp");
    supportedAggregationUdfs.add("std");
    supportedAggregationUdfs.add("stddev");
    supportedAggregationUdfs.add("stddev_pop");
    supportedAggregationUdfs.add("stddev_samp");
  }

  class VectorizationDispatcher implements Dispatcher {

    public VectorizationDispatcher(PhysicalContext pctx) {
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
        throws SemanticException {
      Task<? extends Serializable> currTask = (Task<? extends Serializable>) nd;
      if (currTask instanceof MapRedTask) {
        boolean ret = validateMRTask((MapRedTask) currTask);
        if (ret) {
          vectorizeMRTask((MapRedTask) currTask);
        }
      }
      return null;
    }

    private boolean validateMRTask(MapRedTask mrTask) throws SemanticException {
      MapWork mapWork = mrTask.getWork().getMapWork();

      // Validate the input format
      for (String path : mapWork.getPathToPartitionInfo().keySet()) {
        PartitionDesc pd = mapWork.getPathToPartitionInfo().get(path);
        List<Class<?>> interfaceList =
            Arrays.asList(pd.getInputFileFormatClass().getInterfaces());
        if (!interfaceList.contains(VectorizedInputFormatInterface.class)) {
          LOG.info("Input format: " + pd.getInputFileFormatClassName()
              + ", doesn't provide vectorized input");
          return false;
        }
      }
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      ValidationNodeProcessor vnp = new ValidationNodeProcessor();
      opRules.put(new RuleRegExp("R1", TableScanOperator.getOperatorName() + ".*"
          + FileSinkOperator.getOperatorName()), vnp);
      opRules.put(new RuleRegExp("R2", TableScanOperator.getOperatorName() + ".*"
          + ReduceSinkOperator.getOperatorName()), vnp);
      Dispatcher disp = new DefaultRuleDispatcher(vnp, opRules, null);
      GraphWalker ogw = new DefaultGraphWalker(disp);
      // iterator the mapper operator tree
      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.addAll(mapWork.getAliasToWork().values());
      HashMap<Node, Object> nodeOutput = new HashMap<Node, Object>();
      ogw.startWalking(topNodes, nodeOutput);
      for (Node n : nodeOutput.keySet()) {
        if (nodeOutput.get(n) != null) {
          if (!((Boolean)nodeOutput.get(n)).booleanValue()) {
            return false;
          }
        }
      }
      return true;
    }

    private void vectorizeMRTask(MapRedTask mrTask) throws SemanticException {
      LOG.info("Vectorizing task...");
      MapWork mapWork = mrTask.getWork().getMapWork();
      mapWork.setVectorMode(true);
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      VectorizationNodeProcessor vnp = new VectorizationNodeProcessor(mrTask);
      opRules.put(new RuleRegExp("R1", TableScanOperator.getOperatorName() + ".*" +
          ReduceSinkOperator.getOperatorName()), vnp);
      opRules.put(new RuleRegExp("R2", TableScanOperator.getOperatorName() + ".*"
          + FileSinkOperator.getOperatorName()), vnp);
      Dispatcher disp = new DefaultRuleDispatcher(vnp, opRules, null);
      GraphWalker ogw = new PreOrderWalker(disp);
      // iterator the mapper operator tree
      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.addAll(mapWork.getAliasToWork().values());
      HashMap<Node, Object> nodeOutput = new HashMap<Node, Object>();
      ogw.startWalking(topNodes, nodeOutput);
      mapWork.setScratchColumnVectorTypes(vnp.getScratchColumnVectorTypes());
      mapWork.setScratchColumnMap(vnp.getScratchColumnMap());
      return;
    }
  }

  class ValidationNodeProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      for (Node n : stack) {
        Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) n;
        if (op.getType().equals(OperatorType.REDUCESINK) &&
            op.getParentOperators().get(0).getType().equals(OperatorType.GROUPBY)) {
          return new Boolean(true);
        }
        boolean ret = validateOperator(op);
        if (!ret) {
          LOG.info("Operator: " + op.getName() + " could not be vectorized.");
          return new Boolean(false);
        }
      }
      return new Boolean(true);
    }
  }

  class VectorizationNodeProcessor implements NodeProcessor {

    private final MapWork mWork;
    private final Map<String, VectorizationContext> vectorizationContexts =
        new HashMap<String, VectorizationContext>();

    private final Map<Operator<? extends OperatorDesc>, VectorizationContext> vContextsByTSOp =
        new HashMap<Operator<? extends OperatorDesc>, VectorizationContext>();

    private final Set<Operator<? extends OperatorDesc>> opsDone =
        new HashSet<Operator<? extends OperatorDesc>>();

    public VectorizationNodeProcessor(MapRedTask mrTask) {
      this.mWork = mrTask.getWork().getMapWork();
    }

    public Map<String, Map<Integer, String>> getScratchColumnVectorTypes() {
      Map<String, Map<Integer, String>> scratchColumnVectorTypes =
          new HashMap<String, Map<Integer, String>>();
      for (String onefile : vectorizationContexts.keySet()) {
        VectorizationContext vc = vectorizationContexts.get(onefile);
        Map<Integer, String> cmap = vc.getOutputColumnTypeMap();
        scratchColumnVectorTypes.put(onefile, cmap);
      }
      return scratchColumnVectorTypes;
    }

    public Map<String, Map<String, Integer>> getScratchColumnMap() {
      Map<String, Map<String, Integer>> scratchColumnMap =
          new HashMap<String, Map<String, Integer>>();
      for(String oneFile: vectorizationContexts.keySet()) {
        VectorizationContext vc = vectorizationContexts.get(oneFile);
        Map<String, Integer> cmap = vc.getColumnMap();
        scratchColumnMap.put(oneFile, cmap);
      }
      return scratchColumnMap;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      Node firstOp = stack.firstElement();
      TableScanOperator tsOp = null;

      tsOp = (TableScanOperator) firstOp;

      VectorizationContext vContext = vContextsByTSOp.get(tsOp);
      if (vContext == null) {
        String fileKey = "";
        for (String onefile : mWork.getPathToAliases().keySet()) {
          List<String> aliases = mWork.getPathToAliases().get(onefile);
          for (String alias : aliases) {
            Operator<? extends OperatorDesc> op = mWork.getAliasToWork().get(alias);
            if (op == tsOp) {
              fileKey = onefile;
              if (vContext == null) {
                vContext = getVectorizationContext(tsOp, physicalContext);
              }
              vContext.setFileKey(fileKey);
              vectorizationContexts.put(fileKey, vContext);
              break;
            }
          }
        }
        vContextsByTSOp.put(tsOp, vContext);
      }

      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      if (op.getType().equals(OperatorType.REDUCESINK) &&
          op.getParentOperators().get(0).getType().equals(OperatorType.GROUPBY)) {
        // No need to vectorize
        if (!opsDone.contains(op)) {
          opsDone.add(op);
        }
      } else {
        try {
          if (!opsDone.contains(op)) {
            Operator<? extends OperatorDesc> vectorOp =
                vectorizeOperator(op, vContext);
            opsDone.add(op);
            if (vectorOp != op) {
              opsDone.add(vectorOp);
            }
          }
        } catch (HiveException e) {
          throw new SemanticException(e);
        }
      }
      return null;
    }
  }

  private static class ValidatorVectorizationContext extends VectorizationContext {
    private ValidatorVectorizationContext() {
      super(null, -1);
    }

    @Override
    protected int getInputColumnIndex(String name) {
      return 0;
    }

    @Override
    protected int getInputColumnIndex(ExprNodeColumnDesc colExpr) {
      return 0;
    }
  }

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    this.physicalContext  = pctx;
    boolean vectorPath = HiveConf.getBoolVar(pctx.getConf(),
        HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED);
    if (!vectorPath) {
      LOG.info("Vectorization is disabled");
      return pctx;
    }
    // create dispatcher and graph walker
    Dispatcher disp = new VectorizationDispatcher(pctx);
    TaskGraphWalker ogw = new TaskGraphWalker(disp);

    // get all the tasks nodes from root task
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());

    // begin to walk through the task tree.
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  boolean validateOperator(Operator<? extends OperatorDesc> op) {
    boolean ret = false;
    switch (op.getType()) {
      case MAPJOIN:
        if (op instanceof MapJoinOperator) {
          ret = validateMapJoinOperator((MapJoinOperator) op);
        }
        break;
      case GROUPBY:
        ret = validateGroupByOperator((GroupByOperator) op);
        break;
      case FILTER:
        ret = validateFilterOperator((FilterOperator) op);
        break;
      case SELECT:
        ret = validateSelectOperator((SelectOperator) op);
        break;
      case REDUCESINK:
        ret = validateReduceSinkOperator((ReduceSinkOperator) op);
        break;
      case FILESINK:
      case TABLESCAN:
      case LIMIT:
        ret = true;
        break;
      default:
        ret = false;
        break;
    }
    return ret;
  }

  private boolean validateMapJoinOperator(MapJoinOperator op) {
    MapJoinDesc desc = op.getConf();
    byte posBigTable = (byte) desc.getPosBigTable();
    List<ExprNodeDesc> filterExprs = desc.getFilters().get(posBigTable);
    List<ExprNodeDesc> keyExprs = desc.getKeys().get(posBigTable);
    List<ExprNodeDesc> valueExprs = desc.getExprs().get(posBigTable);
    return validateExprNodeDesc(filterExprs, VectorExpressionDescriptor.Mode.FILTER) &&
        validateExprNodeDesc(keyExprs) &&
        validateExprNodeDesc(valueExprs);
  }

  private boolean validateReduceSinkOperator(ReduceSinkOperator op) {
    List<ExprNodeDesc> keyDescs = op.getConf().getKeyCols();
    List<ExprNodeDesc> partitionDescs = op.getConf().getPartitionCols();
    List<ExprNodeDesc> valueDesc = op.getConf().getValueCols();
    return validateExprNodeDesc(keyDescs) && validateExprNodeDesc(partitionDescs) &&
        validateExprNodeDesc(valueDesc);
  }

  private boolean validateSelectOperator(SelectOperator op) {
    List<ExprNodeDesc> descList = op.getConf().getColList();
    for (ExprNodeDesc desc : descList) {
      boolean ret = validateExprNodeDesc(desc);
      if (!ret) {
        return false;
      }
    }
    return true;
  }

  private boolean validateFilterOperator(FilterOperator op) {
    ExprNodeDesc desc = op.getConf().getPredicate();
    return validateExprNodeDesc(desc, VectorExpressionDescriptor.Mode.FILTER);
  }

  private boolean validateGroupByOperator(GroupByOperator op) {
    boolean ret = validateExprNodeDesc(op.getConf().getKeys());
    if (!ret) {
      return false;
    }
    return validateAggregationDesc(op.getConf().getAggregators());
  }

  private boolean validateExprNodeDesc(List<ExprNodeDesc> descs) {
    return validateExprNodeDesc(descs, VectorExpressionDescriptor.Mode.PROJECTION);
  }

  private boolean validateExprNodeDesc(List<ExprNodeDesc> descs, VectorExpressionDescriptor.Mode mode) {
    for (ExprNodeDesc d : descs) {
      boolean ret = validateExprNodeDesc(d, mode);
      if (!ret) {
        return false;
      }
    }
    return true;
  }

  private boolean validateAggregationDesc(List<AggregationDesc> descs) {
    for (AggregationDesc d : descs) {
      boolean ret = validateAggregationDesc(d);
      if (!ret) {
        return false;
      }
    }
    return true;
  }

  private boolean validateExprNodeDescRecursive(ExprNodeDesc desc) {
    boolean ret = validateDataType(desc.getTypeInfo().getTypeName());
    if (!ret) {
      return false;
    }
    if (desc instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc d = (ExprNodeGenericFuncDesc) desc;
      boolean r = validateGenericUdf(d);
      if (!r) {
        return false;
      }
    }
    if (desc.getChildren() != null) {
      for (ExprNodeDesc d: desc.getChildren()) {
        boolean r = validateExprNodeDescRecursive(d);
        if (!r) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean validateExprNodeDesc(ExprNodeDesc desc) {
    return validateExprNodeDesc(desc, VectorExpressionDescriptor.Mode.PROJECTION);
  }

  boolean validateExprNodeDesc(ExprNodeDesc desc, VectorExpressionDescriptor.Mode mode) {
    if (!validateExprNodeDescRecursive(desc)) {
      return false;
    }
    try {
      VectorizationContext vc = new ValidatorVectorizationContext();
      if (vc.getVectorExpression(desc, mode) == null) {
        return false;
      }
    } catch (HiveException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to vectorize", e);
      }
      return false;
    }
    return true;
  }

  private boolean validateGenericUdf(ExprNodeGenericFuncDesc genericUDFExpr) {
    if (VectorizationContext.isCustomUDF(genericUDFExpr)) {
      return true;
    }
    GenericUDF genericUDF = genericUDFExpr.getGenericUDF();
    if (genericUDF instanceof GenericUDFBridge) {
      Class<? extends UDF> udf = ((GenericUDFBridge) genericUDF).getUdfClass();
      return supportedGenericUDFs.contains(udf);
    } else {
      return supportedGenericUDFs.contains(genericUDF.getClass());
    }
  }

  private boolean validateAggregationDesc(AggregationDesc aggDesc) {
    if (!supportedAggregationUdfs.contains(aggDesc.getGenericUDAFName().toLowerCase())) {
      return false;
    }
    if (aggDesc.getParameters() != null) {
      return validateExprNodeDesc(aggDesc.getParameters());
    }
    return true;
  }

  private boolean validateDataType(String type) {
    return supportedDataTypes.contains(type.toLowerCase());
  }

  private VectorizationContext getVectorizationContext(TableScanOperator op,
      PhysicalContext pctx) {
    RowResolver rr = pctx.getParseContext().getOpParseCtx().get(op).getRowResolver();

    Map<String, Integer> cmap = new HashMap<String, Integer>();
    int columnCount = 0;
    for (ColumnInfo c : rr.getColumnInfos()) {
      if (!c.getIsVirtualCol()) {
        cmap.put(c.getInternalName(), columnCount++);
      }
    }
    Table tab = pctx.getParseContext().getTopToTable().get(op);
    if (tab.getPartitionKeys() != null) {
      for (FieldSchema fs : tab.getPartitionKeys()) {
        cmap.put(fs.getName(), columnCount++);
      }
    }
    return new VectorizationContext(cmap, columnCount);
  }

  Operator<? extends OperatorDesc> vectorizeOperator(Operator<? extends OperatorDesc> op,
      VectorizationContext vContext) throws HiveException {
    Operator<? extends OperatorDesc> vectorOp = null;

    switch (op.getType()) {
      case MAPJOIN:
      case GROUPBY:
      case FILTER:
      case SELECT:
      case FILESINK:
      case REDUCESINK:
      case LIMIT:
        vectorOp = OperatorFactory.getVectorOperator(op.getConf(), vContext);
        break;
      default:
        vectorOp = op;
        break;
    }

    if (vectorOp != op) {
      if (op.getParentOperators() != null) {
        vectorOp.setParentOperators(op.getParentOperators());
        for (Operator<? extends OperatorDesc> p : op.getParentOperators()) {
          p.replaceChild(op, vectorOp);
        }
      }
      if (op.getChildOperators() != null) {
        vectorOp.setChildOperators(op.getChildOperators());
        for (Operator<? extends OperatorDesc> c : op.getChildOperators()) {
          c.replaceParent(op, vectorOp);
        }
      }
      ((AbstractOperatorDesc) vectorOp.getConf()).setVectorMode(true);
    }
    return vectorOp;
  }
}
