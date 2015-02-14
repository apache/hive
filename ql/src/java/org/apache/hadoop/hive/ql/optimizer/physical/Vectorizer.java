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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContextRegion;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
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
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AbstractOperatorDesc;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.VectorGroupByDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.UDFAcos;
import org.apache.hadoop.hive.ql.udf.UDFAsin;
import org.apache.hadoop.hive.ql.udf.UDFAtan;
import org.apache.hadoop.hive.ql.udf.UDFBin;
import org.apache.hadoop.hive.ql.udf.UDFConv;
import org.apache.hadoop.hive.ql.udf.UDFCos;
import org.apache.hadoop.hive.ql.udf.UDFDayOfMonth;
import org.apache.hadoop.hive.ql.udf.UDFDegrees;
import org.apache.hadoop.hive.ql.udf.UDFExp;
import org.apache.hadoop.hive.ql.udf.UDFHex;
import org.apache.hadoop.hive.ql.udf.UDFHour;
import org.apache.hadoop.hive.ql.udf.UDFLength;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.UDFLn;
import org.apache.hadoop.hive.ql.udf.UDFLog;
import org.apache.hadoop.hive.ql.udf.UDFLog10;
import org.apache.hadoop.hive.ql.udf.UDFLog2;
import org.apache.hadoop.hive.ql.udf.UDFMinute;
import org.apache.hadoop.hive.ql.udf.UDFMonth;
import org.apache.hadoop.hive.ql.udf.UDFRadians;
import org.apache.hadoop.hive.ql.udf.UDFRand;
import org.apache.hadoop.hive.ql.udf.UDFRegExp;
import org.apache.hadoop.hive.ql.udf.UDFSecond;
import org.apache.hadoop.hive.ql.udf.UDFSign;
import org.apache.hadoop.hive.ql.udf.UDFSin;
import org.apache.hadoop.hive.ql.udf.UDFSqrt;
import org.apache.hadoop.hive.ql.udf.UDFSubstr;
import org.apache.hadoop.hive.ql.udf.UDFTan;
import org.apache.hadoop.hive.ql.udf.UDFToBoolean;
import org.apache.hadoop.hive.ql.udf.UDFToByte;
import org.apache.hadoop.hive.ql.udf.UDFToDouble;
import org.apache.hadoop.hive.ql.udf.UDFToFloat;
import org.apache.hadoop.hive.ql.udf.UDFToInteger;
import org.apache.hadoop.hive.ql.udf.UDFToLong;
import org.apache.hadoop.hive.ql.udf.UDFToShort;
import org.apache.hadoop.hive.ql.udf.UDFToString;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.apache.hadoop.hive.ql.udf.UDFYear;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class Vectorizer implements PhysicalPlanResolver {

  protected static transient final Log LOG = LogFactory.getLog(Vectorizer.class);

  Pattern supportedDataTypesPattern;
  List<Task<? extends Serializable>> vectorizableTasks =
      new ArrayList<Task<? extends Serializable>>();
  Set<Class<?>> supportedGenericUDFs = new HashSet<Class<?>>();

  Set<String> supportedAggregationUdfs = new HashSet<String>();

  private PhysicalContext physicalContext = null;;

  public Vectorizer() {

    StringBuilder patternBuilder = new StringBuilder();
    patternBuilder.append("int");
    patternBuilder.append("|smallint");
    patternBuilder.append("|tinyint");
    patternBuilder.append("|bigint");
    patternBuilder.append("|integer");
    patternBuilder.append("|long");
    patternBuilder.append("|short");
    patternBuilder.append("|timestamp");
    patternBuilder.append("|boolean");
    patternBuilder.append("|binary");
    patternBuilder.append("|string");
    patternBuilder.append("|byte");
    patternBuilder.append("|float");
    patternBuilder.append("|double");
    patternBuilder.append("|date");
    patternBuilder.append("|void");

    // Decimal types can be specified with different precision and scales e.g. decimal(10,5),
    // as opposed to other data types which can be represented by constant strings.
    // The regex matches only the "decimal" prefix of the type.
    patternBuilder.append("|decimal.*");

    // CHAR and VARCHAR types can be specified with maximum length.
    patternBuilder.append("|char.*");
    patternBuilder.append("|varchar.*");

    supportedDataTypesPattern = Pattern.compile(patternBuilder.toString());

    supportedGenericUDFs.add(GenericUDFOPPlus.class);
    supportedGenericUDFs.add(GenericUDFOPMinus.class);
    supportedGenericUDFs.add(GenericUDFOPMultiply.class);
    supportedGenericUDFs.add(GenericUDFOPDivide.class);
    supportedGenericUDFs.add(GenericUDFOPMod.class);
    supportedGenericUDFs.add(GenericUDFOPNegative.class);
    supportedGenericUDFs.add(GenericUDFOPPositive.class);

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
    supportedGenericUDFs.add(UDFLength.class);

    supportedGenericUDFs.add(UDFYear.class);
    supportedGenericUDFs.add(UDFMonth.class);
    supportedGenericUDFs.add(UDFDayOfMonth.class);
    supportedGenericUDFs.add(UDFHour.class);
    supportedGenericUDFs.add(UDFMinute.class);
    supportedGenericUDFs.add(UDFSecond.class);
    supportedGenericUDFs.add(UDFWeekOfYear.class);
    supportedGenericUDFs.add(GenericUDFToUnixTimeStamp.class);

    supportedGenericUDFs.add(GenericUDFDateAdd.class);
    supportedGenericUDFs.add(GenericUDFDateSub.class);
    supportedGenericUDFs.add(GenericUDFDate.class);
    supportedGenericUDFs.add(GenericUDFDateDiff.class);

    supportedGenericUDFs.add(UDFLike.class);
    supportedGenericUDFs.add(UDFRegExp.class);
    supportedGenericUDFs.add(UDFSubstr.class);
    supportedGenericUDFs.add(GenericUDFLTrim.class);
    supportedGenericUDFs.add(GenericUDFRTrim.class);
    supportedGenericUDFs.add(GenericUDFTrim.class);

    supportedGenericUDFs.add(UDFSin.class);
    supportedGenericUDFs.add(UDFCos.class);
    supportedGenericUDFs.add(UDFTan.class);
    supportedGenericUDFs.add(UDFAsin.class);
    supportedGenericUDFs.add(UDFAcos.class);
    supportedGenericUDFs.add(UDFAtan.class);
    supportedGenericUDFs.add(UDFDegrees.class);
    supportedGenericUDFs.add(UDFRadians.class);
    supportedGenericUDFs.add(GenericUDFFloor.class);
    supportedGenericUDFs.add(GenericUDFCeil.class);
    supportedGenericUDFs.add(UDFExp.class);
    supportedGenericUDFs.add(UDFLn.class);
    supportedGenericUDFs.add(UDFLog2.class);
    supportedGenericUDFs.add(UDFLog10.class);
    supportedGenericUDFs.add(UDFLog.class);
    supportedGenericUDFs.add(GenericUDFPower.class);
    supportedGenericUDFs.add(GenericUDFRound.class);
    supportedGenericUDFs.add(GenericUDFPosMod.class);
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
    supportedGenericUDFs.add(GenericUDFCase.class);
    supportedGenericUDFs.add(GenericUDFWhen.class);
    supportedGenericUDFs.add(GenericUDFCoalesce.class);
    supportedGenericUDFs.add(GenericUDFElt.class);
    supportedGenericUDFs.add(GenericUDFInitCap.class);

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
    supportedGenericUDFs.add(GenericUDFToDecimal.class);
    supportedGenericUDFs.add(GenericUDFToDate.class);
    supportedGenericUDFs.add(GenericUDFToChar.class);
    supportedGenericUDFs.add(GenericUDFToVarchar.class);

    // For conditional expressions
    supportedGenericUDFs.add(GenericUDFIf.class);

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

    private PhysicalContext pctx;

    private List<String> reduceColumnNames;
    private List<TypeInfo> reduceTypeInfos;

    public VectorizationDispatcher(PhysicalContext pctx) {
      this.pctx = pctx;
      reduceColumnNames = null;
      reduceTypeInfos = null;
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
        throws SemanticException {
      Task<? extends Serializable> currTask = (Task<? extends Serializable>) nd;
      if (currTask instanceof MapRedTask) {
        convertMapWork(((MapRedTask) currTask).getWork().getMapWork(), false);
      } else if (currTask instanceof TezTask) {
        TezWork work = ((TezTask) currTask).getWork();
        for (BaseWork w: work.getAllWork()) {
          if (w instanceof MapWork) {
            convertMapWork((MapWork) w, true);
          } else if (w instanceof ReduceWork) {
            // We are only vectorizing Reduce under Tez.
            if (HiveConf.getBoolVar(pctx.getConf(),
                        HiveConf.ConfVars.HIVE_VECTORIZATION_REDUCE_ENABLED)) {
              convertReduceWork((ReduceWork) w);
            }
          }
        }
      } else if (currTask instanceof SparkTask) {
        SparkWork sparkWork = (SparkWork) currTask.getWork();
        for (BaseWork baseWork : sparkWork.getAllWork()) {
          if (baseWork instanceof MapWork) {
            convertMapWork((MapWork) baseWork, false);
          } else if (baseWork instanceof ReduceWork
              && HiveConf.getBoolVar(pctx.getConf(),
                  HiveConf.ConfVars.HIVE_VECTORIZATION_REDUCE_ENABLED)) {
            convertReduceWork((ReduceWork) baseWork);
          }
        }
      }
      return null;
    }

    private void convertMapWork(MapWork mapWork, boolean isTez) throws SemanticException {
      boolean ret = validateMapWork(mapWork, isTez);
      if (ret) {
        vectorizeMapWork(mapWork);
      }
    }

    private void addMapWorkRules(Map<Rule, NodeProcessor> opRules, NodeProcessor np) {
      opRules.put(new RuleRegExp("R1", TableScanOperator.getOperatorName() + ".*"
          + FileSinkOperator.getOperatorName()), np);
      opRules.put(new RuleRegExp("R2", TableScanOperator.getOperatorName() + ".*"
          + ReduceSinkOperator.getOperatorName()), np);
    }

    private boolean validateMapWork(MapWork mapWork, boolean isTez) throws SemanticException {
      LOG.info("Validating MapWork...");

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
      MapWorkValidationNodeProcessor vnp = new MapWorkValidationNodeProcessor(mapWork, isTez);
      addMapWorkRules(opRules, vnp);
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

    private void vectorizeMapWork(MapWork mapWork) throws SemanticException {
      LOG.info("Vectorizing MapWork...");
      mapWork.setVectorMode(true);
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      MapWorkVectorizationNodeProcessor vnp = new MapWorkVectorizationNodeProcessor(mapWork);
      addMapWorkRules(opRules, vnp);
      Dispatcher disp = new DefaultRuleDispatcher(vnp, opRules, null);
      GraphWalker ogw = new PreOrderWalker(disp);
      // iterator the mapper operator tree
      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.addAll(mapWork.getAliasToWork().values());
      HashMap<Node, Object> nodeOutput = new HashMap<Node, Object>();
      ogw.startWalking(topNodes, nodeOutput);

      Map<String, Map<Integer, String>> allScratchColumnVectorTypeMaps = vnp.getAllScratchColumnVectorTypeMaps();
      mapWork.setAllScratchColumnVectorTypeMaps(allScratchColumnVectorTypeMaps);
      Map<String, Map<String, Integer>> allColumnVectorMaps = vnp.getAllColumnVectorMaps();
      mapWork.setAllColumnVectorMaps(allColumnVectorMaps);

      if (LOG.isDebugEnabled()) {
        debugDisplayAllMaps(allColumnVectorMaps, allScratchColumnVectorTypeMaps);
      }

      return;
    }

    private void convertReduceWork(ReduceWork reduceWork) throws SemanticException {
      boolean ret = validateReduceWork(reduceWork);
      if (ret) {
        vectorizeReduceWork(reduceWork);
      }
    }

    private boolean getOnlyStructObjectInspectors(ReduceWork reduceWork) throws SemanticException {
      try {
        // Check key ObjectInspector.
        ObjectInspector keyObjectInspector = reduceWork.getKeyObjectInspector();
        if (keyObjectInspector == null || !(keyObjectInspector instanceof StructObjectInspector)) {
          return false;
        }
        StructObjectInspector keyStructObjectInspector = (StructObjectInspector)keyObjectInspector;
        List<? extends StructField> keyFields = keyStructObjectInspector.getAllStructFieldRefs();

        // Tez doesn't use tagging...
        if (reduceWork.getNeedsTagging()) {
          return false;
        }

        // Check value ObjectInspector.
        ObjectInspector valueObjectInspector = reduceWork.getValueObjectInspector();
        if (valueObjectInspector == null ||
                !(valueObjectInspector instanceof StructObjectInspector)) {
          return false;
        }
        StructObjectInspector valueStructObjectInspector = (StructObjectInspector)valueObjectInspector;
        List<? extends StructField> valueFields = valueStructObjectInspector.getAllStructFieldRefs();

        reduceColumnNames = new ArrayList<String>();
        reduceTypeInfos = new ArrayList<TypeInfo>();

        for (StructField field: keyFields) {
          reduceColumnNames.add(Utilities.ReduceField.KEY.toString() + "." + field.getFieldName());
          reduceTypeInfos.add(TypeInfoUtils.getTypeInfoFromTypeString(field.getFieldObjectInspector().getTypeName()));
        }
        for (StructField field: valueFields) {
          reduceColumnNames.add(Utilities.ReduceField.VALUE.toString() + "." + field.getFieldName());
          reduceTypeInfos.add(TypeInfoUtils.getTypeInfoFromTypeString(field.getFieldObjectInspector().getTypeName()));
        }
      } catch (Exception e) {
        throw new SemanticException(e);
      }
      return true;
    }

    private void addReduceWorkRules(Map<Rule, NodeProcessor> opRules, NodeProcessor np) {
      opRules.put(new RuleRegExp("R1", ExtractOperator.getOperatorName() + ".*"), np);
      opRules.put(new RuleRegExp("R2", GroupByOperator.getOperatorName() + ".*"), np);
      opRules.put(new RuleRegExp("R3", SelectOperator.getOperatorName() + ".*"), np);
    }

    private boolean validateReduceWork(ReduceWork reduceWork) throws SemanticException {
      LOG.info("Validating ReduceWork...");

      // Validate input to ReduceWork.
      if (!getOnlyStructObjectInspectors(reduceWork)) {
        return false;
      }
      // Now check the reduce operator tree.
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      ReduceWorkValidationNodeProcessor vnp = new ReduceWorkValidationNodeProcessor();
      addReduceWorkRules(opRules, vnp);
      Dispatcher disp = new DefaultRuleDispatcher(vnp, opRules, null);
      GraphWalker ogw = new DefaultGraphWalker(disp);
      // iterator the reduce operator tree
      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.add(reduceWork.getReducer());
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

    private void vectorizeReduceWork(ReduceWork reduceWork) throws SemanticException {
      LOG.info("Vectorizing ReduceWork...");
      reduceWork.setVectorMode(true);
 
      // For some reason, the DefaultGraphWalker does not descend down from the reducer Operator as
      // expected.  We need to descend down, otherwise it breaks our algorithm that determines
      // VectorizationContext...  Do we use PreOrderWalker instead of DefaultGraphWalker.
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      ReduceWorkVectorizationNodeProcessor vnp =
              new ReduceWorkVectorizationNodeProcessor(reduceColumnNames);
      addReduceWorkRules(opRules, vnp);
      Dispatcher disp = new DefaultRuleDispatcher(vnp, opRules, null);
      GraphWalker ogw = new PreOrderWalker(disp);
      // iterator the reduce operator tree
      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.add(reduceWork.getReducer());
      LOG.info("vectorizeReduceWork reducer Operator: " +
              reduceWork.getReducer().getName() + "...");
      HashMap<Node, Object> nodeOutput = new HashMap<Node, Object>();
      ogw.startWalking(topNodes, nodeOutput);

      // Necessary since we are vectorizing the root operator in reduce.
      reduceWork.setReducer(vnp.getRootVectorOp());

      Operator<? extends OperatorDesc> reducer = reduceWork.getReducer();
      if (reducer.getType().equals(OperatorType.EXTRACT)) {
        ((VectorExtractOperator)reducer).setReduceTypeInfos(reduceTypeInfos);
      }

      Map<String, Map<Integer, String>> allScratchColumnVectorTypeMaps = vnp.getAllScratchColumnVectorTypeMaps();
      reduceWork.setAllScratchColumnVectorTypeMaps(allScratchColumnVectorTypeMaps);
      Map<String, Map<String, Integer>> allColumnVectorMaps = vnp.getAllColumnVectorMaps();
      reduceWork.setAllColumnVectorMaps(allColumnVectorMaps);


      if (LOG.isDebugEnabled()) {
        debugDisplayAllMaps(allColumnVectorMaps, allScratchColumnVectorTypeMaps);
      }
    }
  }

  class MapWorkValidationNodeProcessor implements NodeProcessor {

    private MapWork mapWork;
    private boolean isTez;

    public MapWorkValidationNodeProcessor(MapWork mapWork, boolean isTez) {
      this.mapWork = mapWork;
      this.isTez = isTez;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      for (Node n : stack) {
        Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) n;
        if (nonVectorizableChildOfGroupBy(op)) {
          return new Boolean(true);
        }
        boolean ret = validateMapWorkOperator(op, mapWork, isTez);
        if (!ret) {
          LOG.info("MapWork Operator: " + op.getName() + " could not be vectorized.");
          return new Boolean(false);
        }
      }
      return new Boolean(true);
    }
  }

  class ReduceWorkValidationNodeProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      for (Node n : stack) {
        Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) n;
        if (nonVectorizableChildOfGroupBy(op)) {
          return new Boolean(true);
        }
        boolean ret = validateReduceWorkOperator(op);
        if (!ret) {
          LOG.info("ReduceWork Operator: " + op.getName() + " could not be vectorized.");
          return new Boolean(false);
        }
      }
      return new Boolean(true);
    }
  }

  // This class has common code used by both MapWorkVectorizationNodeProcessor and
  // ReduceWorkVectorizationNodeProcessor.
  class VectorizationNodeProcessor implements NodeProcessor {

    // This is used to extract scratch column types for each file key
    protected final Map<String, VectorizationContext> scratchColumnContext =
        new HashMap<String, VectorizationContext>();

    protected final Map<Operator<? extends OperatorDesc>, VectorizationContext> vContextsByOp =
        new HashMap<Operator<? extends OperatorDesc>, VectorizationContext>();

    protected final Set<Operator<? extends OperatorDesc>> opsDone =
        new HashSet<Operator<? extends OperatorDesc>>();

    public Map<String, Map<Integer, String>> getAllScratchColumnVectorTypeMaps() {
      Map<String, Map<Integer, String>> allScratchColumnVectorTypeMaps =
          new HashMap<String, Map<Integer, String>>();
      for (String onefile : scratchColumnContext.keySet()) {
        VectorizationContext vc = scratchColumnContext.get(onefile);
        Map<Integer, String> cmap = vc.getScratchColumnTypeMap();
        allScratchColumnVectorTypeMaps.put(onefile, cmap);
      }
      return allScratchColumnVectorTypeMaps;
    }

    public Map<String, Map<String, Integer>> getAllColumnVectorMaps() {
      Map<String, Map<String, Integer>> allColumnVectorMaps =
          new HashMap<String, Map<String, Integer>>();
      for(String oneFile: scratchColumnContext.keySet()) {
        VectorizationContext vc = scratchColumnContext.get(oneFile);
        Map<String, Integer> cmap = vc.getProjectionColumnMap();
        allColumnVectorMaps.put(oneFile, cmap);
      }
      return allColumnVectorMaps;
    }

    public VectorizationContext walkStackToFindVectorizationContext(Stack<Node> stack,
            Operator<? extends OperatorDesc> op) throws SemanticException {
      VectorizationContext vContext = null;
      if (stack.size() <= 1) {
        throw new SemanticException(
            String.format("Expected operator stack for operator %s to have at least 2 operators",
                  op.getName()));
      }
      // Walk down the stack of operators until we found one willing to give us a context.
      // At the bottom will be the root operator, guaranteed to have a context
      int i= stack.size()-2;
      while (vContext == null) {
        if (i < 0) {
          return null;
        }
        Operator<? extends OperatorDesc> opParent = (Operator<? extends OperatorDesc>) stack.get(i);
        vContext = vContextsByOp.get(opParent);
        --i;
      }
      return vContext;
    }

    public Operator<? extends OperatorDesc> doVectorize(Operator<? extends OperatorDesc> op,
            VectorizationContext vContext) throws SemanticException {
      Operator<? extends OperatorDesc> vectorOp = op;
      try {
        if (!opsDone.contains(op)) {
          vectorOp = vectorizeOperator(op, vContext);
          opsDone.add(op);
          if (vectorOp != op) {
            opsDone.add(vectorOp);
          }
          if (vectorOp instanceof VectorizationContextRegion) {
            VectorizationContextRegion vcRegion = (VectorizationContextRegion) vectorOp;
            VectorizationContext vOutContext = vcRegion.getOuputVectorizationContext();
            vContextsByOp.put(op, vOutContext);
            scratchColumnContext.put(vOutContext.getFileKey(), vOutContext);
          }
        }
      } catch (HiveException e) {
        throw new SemanticException(e);
      }
      return vectorOp;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      throw new SemanticException("Must be overridden");
    }
  }
  
  class MapWorkVectorizationNodeProcessor extends VectorizationNodeProcessor {

    private final MapWork mWork;

    public MapWorkVectorizationNodeProcessor(MapWork mWork) {
      this.mWork = mWork;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      LOG.info("MapWorkVectorizationNodeProcessor processing Operator: " + op.getName() + "...");

      VectorizationContext vContext = null;

      if (op instanceof TableScanOperator) {
        vContext = getVectorizationContext(op, physicalContext);
        for (String onefile : mWork.getPathToAliases().keySet()) {
          List<String> aliases = mWork.getPathToAliases().get(onefile);
          for (String alias : aliases) {
            Operator<? extends OperatorDesc> opRoot = mWork.getAliasToWork().get(alias);
            if (op == opRoot) {
              // The same vectorization context is copied multiple times into
              // the MapWork scratch columnMap
              // Each partition gets a copy
              //
              vContext.setFileKey(onefile);
              scratchColumnContext.put(onefile, vContext);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Vectorized MapWork operator " + op.getName() + " vectorization context " + vContext.toString());
              }
              break;
            }
          }
        }
        vContextsByOp.put(op, vContext);
      } else {
        vContext = walkStackToFindVectorizationContext(stack, op);
        if (vContext == null) {
          throw new SemanticException(
              String.format("Did not find vectorization context for operator %s in operator stack",
                      op.getName()));
        }
      }

      assert vContext != null;

      // When Vectorized GROUPBY outputs rows instead of vectorized row batchs, we don't
      // vectorize the operators below it.
      if (nonVectorizableChildOfGroupBy(op)) {
        // No need to vectorize
        if (!opsDone.contains(op)) {
            opsDone.add(op);
          }
        return null;
      }

      Operator<? extends OperatorDesc> vectorOp = doVectorize(op, vContext);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Vectorized MapWork operator " + vectorOp.getName() + " vectorization context " + vContext.toString());
        if (vectorOp instanceof VectorizationContextRegion) {
          VectorizationContextRegion vcRegion = (VectorizationContextRegion) vectorOp;
          VectorizationContext vOutContext = vcRegion.getOuputVectorizationContext();
          LOG.debug("Vectorized MapWork operator " + vectorOp.getName() + " added vectorization context " + vContext.toString());
        }
      }

      return null;
    }
  }

  class ReduceWorkVectorizationNodeProcessor extends VectorizationNodeProcessor {

    private List<String> reduceColumnNames;
    
    private VectorizationContext reduceShuffleVectorizationContext;

    private Operator<? extends OperatorDesc> rootVectorOp;

    public Operator<? extends OperatorDesc> getRootVectorOp() {
      return rootVectorOp;
    }

    public ReduceWorkVectorizationNodeProcessor(List<String> reduceColumnNames) {
      this.reduceColumnNames =  reduceColumnNames;
      rootVectorOp = null;
      reduceShuffleVectorizationContext = null;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      LOG.info("ReduceWorkVectorizationNodeProcessor processing Operator: " +
              op.getName() + "...");

      VectorizationContext vContext = null;

      boolean saveRootVectorOp = false;

      if (op.getParentOperators().size() == 0) {
        LOG.info("ReduceWorkVectorizationNodeProcessor process reduceColumnNames " + reduceColumnNames.toString());

        vContext = new VectorizationContext(reduceColumnNames);
        vContext.setFileKey("_REDUCE_SHUFFLE_");
        scratchColumnContext.put("_REDUCE_SHUFFLE_", vContext);
        reduceShuffleVectorizationContext = vContext;
        saveRootVectorOp = true;

        if (LOG.isDebugEnabled()) {
          LOG.debug("Vectorized ReduceWork reduce shuffle vectorization context " + vContext.toString());
        }
      } else {
        vContext = walkStackToFindVectorizationContext(stack, op);
        if (vContext == null) {
          // If we didn't find a context among the operators, assume the top -- reduce shuffle's
          // vectorization context.
          vContext = reduceShuffleVectorizationContext;
        }
      }

      assert vContext != null;

      // When Vectorized GROUPBY outputs rows instead of vectorized row batchs, we don't
      // vectorize the operators below it.
      if (nonVectorizableChildOfGroupBy(op)) {
        // No need to vectorize
        if (!opsDone.contains(op)) {
          opsDone.add(op);
        }
        return null;
      }

      Operator<? extends OperatorDesc> vectorOp = doVectorize(op, vContext);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Vectorized ReduceWork operator " + vectorOp.getName() + " vectorization context " + vContext.toString());
        if (vectorOp instanceof VectorizationContextRegion) {
          VectorizationContextRegion vcRegion = (VectorizationContextRegion) vectorOp;
          VectorizationContext vOutContext = vcRegion.getOuputVectorizationContext();
          LOG.debug("Vectorized ReduceWork operator " + vectorOp.getName() + " added vectorization context " + vContext.toString());
        }
      }
      if (vectorOp instanceof VectorGroupByOperator) {
        VectorGroupByOperator groupBy = (VectorGroupByOperator) vectorOp;
        VectorGroupByDesc vectorDesc = groupBy.getConf().getVectorDesc();
        vectorDesc.setVectorGroupBatches(true);
      }
      if (saveRootVectorOp && op != vectorOp) {
        rootVectorOp = vectorOp;
      }

      return null;
    }
  }

  private static class ValidatorVectorizationContext extends VectorizationContext {
    private ValidatorVectorizationContext() {
      super();
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

  boolean validateMapWorkOperator(Operator<? extends OperatorDesc> op, MapWork mWork, boolean isTez) {
    boolean ret = false;
    switch (op.getType()) {
      case MAPJOIN:
        if (op instanceof MapJoinOperator) {
          ret = validateMapJoinOperator((MapJoinOperator) op);
        } else if (op instanceof SMBMapJoinOperator) {
          ret = validateSMBMapJoinOperator((SMBMapJoinOperator) op);
        }
        break;
      case GROUPBY:
        ret = validateGroupByOperator((GroupByOperator) op, false, isTez);
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
      case TABLESCAN:
        ret = validateTableScanOperator((TableScanOperator) op, mWork);
        break;
      case FILESINK:
      case LIMIT:
      case EVENT:
        ret = true;
        break;
      default:
        ret = false;
        break;
    }
    return ret;
  }

  boolean validateReduceWorkOperator(Operator<? extends OperatorDesc> op) {
    boolean ret = false;
    switch (op.getType()) {
      case EXTRACT:
        ret = validateExtractOperator((ExtractOperator) op);
        break;
      case MAPJOIN:
        // Does MAPJOIN actually get planned in Reduce?
        if (op instanceof MapJoinOperator) {
          ret = validateMapJoinOperator((MapJoinOperator) op);
        } else if (op instanceof SMBMapJoinOperator) {
          ret = validateSMBMapJoinOperator((SMBMapJoinOperator) op);
        }
        break;
      case GROUPBY:
        if (HiveConf.getBoolVar(physicalContext.getConf(),
                    HiveConf.ConfVars.HIVE_VECTORIZATION_REDUCE_GROUPBY_ENABLED)) {
          ret = validateGroupByOperator((GroupByOperator) op, true, true);
        } else {
          ret = false;
        }
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
        ret = validateFileSinkOperator((FileSinkOperator) op);
        break;
      case LIMIT:
      case EVENT:
        ret = true;
        break;
      default:
        ret = false;
        break;
    }
    return ret;
  }

  public Boolean nonVectorizableChildOfGroupBy(Operator<? extends OperatorDesc> op) {
    Operator<? extends OperatorDesc> currentOp = op;
    while (currentOp.getParentOperators().size() > 0) {
      currentOp = currentOp.getParentOperators().get(0);
      if (currentOp.getType().equals(OperatorType.GROUPBY)) {
        GroupByDesc desc = (GroupByDesc)currentOp.getConf();
        boolean isVectorOutput = desc.getVectorDesc().isVectorOutput();
        if (isVectorOutput) {
          // This GROUP BY does vectorize its output.
          return false;
        }
        return true;
      }
    }
    return false;
  }

  private boolean validateSMBMapJoinOperator(SMBMapJoinOperator op) {
    SMBJoinDesc desc = op.getConf();
    // Validation is the same as for map join, since the 'small' tables are not vectorized
    return validateMapJoinDesc(desc);
  }

  private boolean validateTableScanOperator(TableScanOperator op, MapWork mWork) {
    TableScanDesc desc = op.getConf();
    if (desc.isGatherStats()) {
      return false;
    }

    String columns = "";
    String types = "";
    String partitionColumns = "";
    String partitionTypes = "";
    boolean haveInfo = false;

    // This over-reaches slightly, since we can have > 1 table-scan  per map-work.
    // It needs path to partition, path to alias, then check the alias == the same table-scan, to be accurate.
    // That said, that is a TODO item to be fixed when we support >1 TableScans per vectorized pipeline later.
    LinkedHashMap<String, PartitionDesc> partitionDescs = mWork.getPathToPartitionInfo();

    // For vectorization, compare each partition information for against the others.
    // We assume the table information will be from one of the partitions, so it will
    // work to focus on the partition information and not compare against the TableScanOperator
    // columns (in the VectorizationContext)....
    for (Map.Entry<String, PartitionDesc> entry : partitionDescs.entrySet()) {
      PartitionDesc partDesc = entry.getValue();
      if (partDesc.getPartSpec() == null || partDesc.getPartSpec().isEmpty()) {
        // No partition information -- we match because we would default to using the table description.
        continue;
      }
      Properties partProps = partDesc.getProperties();
      if (!haveInfo) {
        columns = partProps.getProperty(hive_metastoreConstants.META_TABLE_COLUMNS);
        types = partProps.getProperty(hive_metastoreConstants.META_TABLE_COLUMN_TYPES);
        partitionColumns = partProps.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
        partitionTypes = partProps.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES);
        haveInfo = true;
      } else {
        String nextColumns = partProps.getProperty(hive_metastoreConstants.META_TABLE_COLUMNS);
        String nextTypes = partProps.getProperty(hive_metastoreConstants.META_TABLE_COLUMN_TYPES);
        String nextPartitionColumns = partProps.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
        String nextPartitionTypes = partProps.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES);
        if (!columns.equalsIgnoreCase(nextColumns)) {
          LOG.info(
                  String.format("Could not vectorize partition %s.  Its column names %s do not match the other column names %s",
                            entry.getKey(), nextColumns, columns));
          return false;
        }
        if (!types.equalsIgnoreCase(nextTypes)) {
          LOG.info(
                  String.format("Could not vectorize partition %s.  Its column types %s do not match the other column types %s",
                              entry.getKey(), nextTypes, types));
          return false;
        }
        if (!partitionColumns.equalsIgnoreCase(nextPartitionColumns)) {
          LOG.info(
                 String.format("Could not vectorize partition %s.  Its partition column names %s do not match the other partition column names %s",
                              entry.getKey(), nextPartitionColumns, partitionColumns));
          return false;
        }
        if (!partitionTypes.equalsIgnoreCase(nextPartitionTypes)) {
          LOG.info(
                 String.format("Could not vectorize partition %s.  Its partition column types %s do not match the other partition column types %s",
                                entry.getKey(), nextPartitionTypes, partitionTypes));
          return false;
        }
      }
    }
    return true;
  }

  private boolean validateMapJoinOperator(MapJoinOperator op) {
    MapJoinDesc desc = op.getConf();
    return validateMapJoinDesc(desc);
  }
  
  private boolean validateMapJoinDesc(MapJoinDesc desc) {
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

  private boolean validateGroupByOperator(GroupByOperator op, boolean isReduce, boolean isTez) {
    GroupByDesc desc = op.getConf();
    VectorGroupByDesc vectorDesc = desc.getVectorDesc();

    if (desc.isGroupingSetsPresent()) {
      LOG.info("Grouping sets not supported in vector mode");
      return false;
    }
    boolean ret = validateExprNodeDesc(desc.getKeys());
    if (!ret) {
      return false;
    }
    ret = validateAggregationDesc(desc.getAggregators(), isReduce);
    if (!ret) {
      return false;
    }
    if (isReduce) {
      if (desc.isDistinct()) {
        LOG.info("Distinct not supported in reduce vector mode");
        return false;
      }
      // Sort-based GroupBy?
      if (desc.getMode() != GroupByDesc.Mode.COMPLETE &&
          desc.getMode() != GroupByDesc.Mode.PARTIAL1 &&
          desc.getMode() != GroupByDesc.Mode.PARTIAL2 &&
          desc.getMode() != GroupByDesc.Mode.MERGEPARTIAL) {
        LOG.info("Reduce vector mode not supported when input for GROUP BY not sorted");
        return false;
      }
      LOG.info("Reduce GROUP BY mode is " + desc.getMode().name());
      if (desc.getGroupKeyNotReductionKey()) {
        LOG.info("Reduce vector mode not supported when group key is not reduction key");
        return false;
      }
      if (!aggregatorsOutputIsPrimitive(desc.getAggregators(), isReduce)) {
        LOG.info("Reduce vector mode only supported when aggregate outputs are primitive types");
        return false;
      }
      if (desc.getKeys().size() > 0) {
        if (op.getParentOperators().size() > 0) {
          LOG.info("Reduce vector mode can only handle a key group GROUP BY operator when it is fed by reduce-shuffle");
          return false;
        }
        LOG.info("Reduce-side GROUP BY will process key groups");
        vectorDesc.setVectorGroupBatches(true);
      } else {
        LOG.info("Reduce-side GROUP BY will do global aggregation");
      }
      vectorDesc.setVectorOutput(true);
      vectorDesc.setIsReduce(true);
    }
    return true;
  }

  private boolean validateExtractOperator(ExtractOperator op) {
    ExprNodeDesc expr = op.getConf().getCol();
    boolean ret = validateExprNodeDesc(expr);
    if (!ret) {
      return false;
    }
    return true;
  }

  private boolean validateFileSinkOperator(FileSinkOperator op) {
   return true;
  }

  private boolean validateExprNodeDesc(List<ExprNodeDesc> descs) {
    return validateExprNodeDesc(descs, VectorExpressionDescriptor.Mode.PROJECTION);
  }

  private boolean validateExprNodeDesc(List<ExprNodeDesc> descs,
          VectorExpressionDescriptor.Mode mode) {
    for (ExprNodeDesc d : descs) {
      boolean ret = validateExprNodeDesc(d, mode);
      if (!ret) {
        return false;
      }
    }
    return true;
  }

  private boolean validateAggregationDesc(List<AggregationDesc> descs, boolean isReduce) {
    for (AggregationDesc d : descs) {
      boolean ret = validateAggregationDesc(d, isReduce);
      if (!ret) {
        return false;
      }
    }
    return true;
  }

  private boolean validateExprNodeDescRecursive(ExprNodeDesc desc) {
    if (desc instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc c = (ExprNodeColumnDesc) desc;
      // Currently, we do not support vectorized virtual columns (see HIVE-5570).
      if (VirtualColumn.VIRTUAL_COLUMN_NAMES.contains(c.getColumn())) {
        LOG.info("Cannot vectorize virtual column " + c.getColumn());
        return false;
      }
    }
    String typeName = desc.getTypeInfo().getTypeName();
    boolean ret = validateDataType(typeName);
    if (!ret) {
      LOG.info("Cannot vectorize " + desc.toString() + " of type " + typeName);
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
        // TODO: this cannot happen - VectorizationContext throws in such cases.
        LOG.info("getVectorExpression returned null");
        return false;
      }
    } catch (Exception e) {
      LOG.info("Failed to vectorize", e);
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

  private boolean validateAggregationDesc(AggregationDesc aggDesc, boolean isReduce) {
    if (!supportedAggregationUdfs.contains(aggDesc.getGenericUDAFName().toLowerCase())) {
      return false;
    }
    if (aggDesc.getParameters() != null && !validateExprNodeDesc(aggDesc.getParameters())) {
      return false;
    }
    // See if we can vectorize the aggregation.
    try {
      VectorizationContext vc = new ValidatorVectorizationContext();
      if (vc.getAggregatorExpression(aggDesc, isReduce) == null) {
        // TODO: this cannot happen - VectorizationContext throws in such cases.
        LOG.info("getAggregatorExpression returned null");
        return false;
      }
    } catch (Exception e) {
      LOG.info("Failed to vectorize", e);
      return false;
    }
    return true;
  }

  private boolean aggregatorsOutputIsPrimitive(List<AggregationDesc> descs, boolean isReduce) {
    for (AggregationDesc d : descs) {
      boolean ret = aggregatorsOutputIsPrimitive(d, isReduce);
      if (!ret) {
        return false;
      }
    }
    return true;
  }

  private boolean aggregatorsOutputIsPrimitive(AggregationDesc aggDesc, boolean isReduce) {
    VectorizationContext vc = new ValidatorVectorizationContext();
    VectorAggregateExpression vectorAggrExpr;
    try {
        vectorAggrExpr = vc.getAggregatorExpression(aggDesc, isReduce);
    } catch (Exception e) {
      // We should have already attempted to vectorize in validateAggregationDesc.
      LOG.info("Vectorization of aggreation should have succeeded ", e);
      return false;
    }

    ObjectInspector outputObjInspector = vectorAggrExpr.getOutputObjectInspector();
    if (outputObjInspector.getCategory() == ObjectInspector.Category.PRIMITIVE) {
      return true;
    }
    return false;
  }

  private boolean validateDataType(String type) {
    return supportedDataTypesPattern.matcher(type.toLowerCase()).matches();
  }

  private VectorizationContext getVectorizationContext(Operator op,
      PhysicalContext pctx) {
    RowSchema rs = op.getSchema();

    // Add all non-virtual columns to make a vectorization context for
    // the TableScan operator.
    VectorizationContext vContext = new VectorizationContext();
    for (ColumnInfo c : rs.getSignature()) {
      // Earlier, validation code should have eliminated virtual columns usage (HIVE-5560).
      if (!isVirtualColumn(c)) {
        vContext.addInitialColumn(c.getInternalName());
      }
    }
    vContext.finishedAddingInitialColumns();
    return vContext;
  }

  private void fixupParentChildOperators(Operator<? extends OperatorDesc> op, 
          Operator<? extends OperatorDesc> vectorOp) {
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
      case EXTRACT:
      case EVENT:
        vectorOp = OperatorFactory.getVectorOperator(op.getConf(), vContext);
        break;
      default:
        vectorOp = op;
        break;
    }

    if (vectorOp != op) {
      fixupParentChildOperators(op, vectorOp);
      ((AbstractOperatorDesc) vectorOp.getConf()).setVectorMode(true);
    }
    return vectorOp;
  }

  private boolean isVirtualColumn(ColumnInfo column) {

    // Not using method column.getIsVirtualCol() because partitioning columns are also
    // treated as virtual columns in ColumnInfo.
    if (VirtualColumn.VIRTUAL_COLUMN_NAMES.contains(column.getInternalName())) {
        return true;
    }
    return false;
  }

  public void debugDisplayAllMaps(Map<String, Map<String, Integer>> allColumnVectorMaps, 
          Map<String, Map<Integer, String>> allScratchColumnVectorTypeMaps) {

    // Context keys grow in length since they are a path...
    Comparator<String> comparerShorterString = new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        Integer length1 = o1.length();
        Integer length2 = o2.length();
        return length1.compareTo(length2);
      }};

    Comparator<Integer> comparerInteger = new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return o1.compareTo(o2);
      }};

    Map<String, Map<Integer, String>> sortedAllColumnVectorMaps = new TreeMap<String, Map<Integer, String>>(comparerShorterString);
    for (Map.Entry<String, Map<String, Integer>> entry : allColumnVectorMaps.entrySet()) {
      Map<Integer, String> sortedColumnMap = new TreeMap<Integer, String>(comparerInteger);
      for (Map.Entry<String, Integer> innerEntry : entry.getValue().entrySet()) {
        sortedColumnMap.put(innerEntry.getValue(), innerEntry.getKey());
      }
      sortedAllColumnVectorMaps.put(entry.getKey(), sortedColumnMap);
    }
    LOG.debug("sortedAllColumnVectorMaps " + sortedAllColumnVectorMaps);

    Map<String, Map<Integer, String>> sortedAllScratchColumnVectorTypeMap = new TreeMap<String, Map<Integer, String>>(comparerShorterString);
    for (Map.Entry<String, Map<Integer, String>> entry : allScratchColumnVectorTypeMaps.entrySet()) {
      Map<Integer, String> sortedScratchColumnTypeMap = new TreeMap<Integer, String>(comparerInteger);
      sortedScratchColumnTypeMap.putAll(entry.getValue());
      sortedAllScratchColumnVectorTypeMap.put(entry.getKey(), sortedScratchColumnTypeMap);
    }
    LOG.debug("sortedAllScratchColumnVectorTypeMap " + sortedAllScratchColumnVectorTypeMap);
  }
}
