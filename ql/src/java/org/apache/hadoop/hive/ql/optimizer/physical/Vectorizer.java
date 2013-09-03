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
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
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
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.UDFDayOfMonth;
import org.apache.hadoop.hive.ql.udf.UDFHour;
import org.apache.hadoop.hive.ql.udf.UDFLength;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.UDFLower;
import org.apache.hadoop.hive.ql.udf.UDFMinute;
import org.apache.hadoop.hive.ql.udf.UDFOPDivide;
import org.apache.hadoop.hive.ql.udf.UDFOPMinus;
import org.apache.hadoop.hive.ql.udf.UDFOPMod;
import org.apache.hadoop.hive.ql.udf.UDFOPMultiply;
import org.apache.hadoop.hive.ql.udf.UDFOPNegative;
import org.apache.hadoop.hive.ql.udf.UDFOPPlus;
import org.apache.hadoop.hive.ql.udf.UDFOPPositive;
import org.apache.hadoop.hive.ql.udf.UDFSecond;
import org.apache.hadoop.hive.ql.udf.UDFUpper;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.apache.hadoop.hive.ql.udf.UDFYear;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUnixTimeStamp;

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
    supportedGenericUDFs.add(UDFLower.class);
    supportedGenericUDFs.add(UDFUpper.class);

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
          LOG.debug("Input format: " + pd.getInputFileFormatClassName()
              + ", doesn't provide vectorized input");
          System.err.println("Input format: " + pd.getInputFileFormatClassName()
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
      System.err.println("Going down the vectorized path");
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
          System.err.println("Operator: "+op.getName()+", could not be vectorized");
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
              break;
            }
          }
        }
        vContext = getVectorizationContext(tsOp, physicalContext);
        vectorizationContexts.put(fileKey, vContext);
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

  private boolean validateOperator(Operator<? extends OperatorDesc> op) {
    boolean ret = false;
    switch (op.getType()) {
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
        ret = true;
        break;
      default:
        ret = false;
        break;
    }
    return ret;
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
    return validateExprNodeDesc(desc);
  }

  private boolean validateGroupByOperator(GroupByOperator op) {
    boolean ret = validateExprNodeDesc(op.getConf().getKeys());
    if (!ret) {
      return false;
    }
    return validateAggregationDesc(op.getConf().getAggregators());
  }

  private boolean validateExprNodeDesc(List<ExprNodeDesc> descs) {
    for (ExprNodeDesc d : descs) {
      boolean ret = validateExprNodeDesc(d);
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

  private boolean validateExprNodeDesc(ExprNodeDesc desc) {
    boolean ret = validateDataType(desc.getTypeInfo().getTypeName());
    if (!ret) {
      return false;
    }
    if (desc instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc d = (ExprNodeGenericFuncDesc) desc;
      boolean r = validateGenericUdf(d.getGenericUDF());
      if (!r) {
        return false;
      }
    }
    if (desc.getChildren() != null) {
      for (ExprNodeDesc d: desc.getChildren()) {
        validateExprNodeDesc(d);
      }
    }
    return true;
  }

  private boolean validateGenericUdf(GenericUDF genericUDF) {
    if (genericUDF instanceof GenericUDFBridge) {
      Class<? extends UDF> udf = ((GenericUDFBridge) genericUDF).getUdfClass();
      return supportedGenericUDFs.contains(udf);
    } else {
      return supportedGenericUDFs.contains(genericUDF.getClass());
    }
  }

  private boolean validateAggregationDesc(AggregationDesc aggDesc) {
    return supportedAggregationUdfs.contains(aggDesc.getGenericUDAFName().toLowerCase());
  }

  private boolean validateDataType(String type) {
    return supportedDataTypes.contains(type.toLowerCase());
  }

  private VectorizationContext getVectorizationContext(Operator<? extends OperatorDesc> op,
      PhysicalContext pctx) {
    RowResolver rr = pctx.getParseContext().getOpParseCtx().get(op).getRowResolver();

    Map<String, Integer> cmap = new HashMap<String, Integer>();
    int columnCount = 0;
    for (ColumnInfo c : rr.getColumnInfos()) {
      if (!c.getIsVirtualCol()) {
        cmap.put(c.getInternalName(), columnCount++);
      }
    }
    return new VectorizationContext(cmap, columnCount);
  }

  private Operator<? extends OperatorDesc> vectorizeOperator(Operator<? extends OperatorDesc> op,
      VectorizationContext vContext) throws HiveException {
    Operator<? extends OperatorDesc> vectorOp = null;

    switch (op.getType()) {
      case GROUPBY:
      case FILTER:
      case SELECT:
      case FILESINK:
      case REDUCESINK:
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
    }
    return vectorOp;
  }
}
