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

package org.apache.hadoop.hive.ql.optimizer;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.Utilities.ReduceField;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.optimizer.graph.OperatorGraph;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * FIXME
 */
public class ParallelEdgeFixer extends Transform {

  static Operator d1 = null;
  static Operator d2 = null;
  static Operator d3 = null;
  protected static final Logger LOG = LoggerFactory.getLogger(ParallelEdgeFixer.class);

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    Set<OpGroup> groups = findOpGroups(pctx);
    fixParallelEdges(groups);
    try {
      new OperatorGraph(pctx).toDot(new File("/tmp/last_para.dot"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return pctx;
  }

  private void fixParallelEdges(Set<OpGroup> groups) {
    Map<Operator, OpGroup> op2group = new HashMap<>();
    for (OpGroup g : groups) {
      for (Operator<?> o : g.members) {
        op2group.put(o, g);
      }
    }

    for (OpGroup g : groups) {
      Set<OpGroup> ascendingGroups = new LinkedHashSet<>();
      for (Operator<?> o : g.members) {
        for (Operator<? extends OperatorDesc> p : o.getParentOperators()) {
          OpGroup parentGroup = op2group.get(p);
          if (parentGroup == g) {
            continue;
          }
          if (ascendingGroups.contains(parentGroup)) {
            fixParallelEdge(p, o);
          } else {
            ascendingGroups.add(parentGroup);
          }

        }
      }
    }

  }

  /**
   * Adds an intermediate reduce sink inbetweeen p and o.
   */

  private void fixParallelEdge2(Operator<? extends OperatorDesc> p, Operator<?> o) {
    Operator<? extends OperatorDesc> input = p;
    ReduceSinkDesc conf = (ReduceSinkDesc) p.getConf();
    List<ExprNodeDesc> keys = conf.getKeyCols();
    List<ExprNodeDesc> partition = conf.getPartitionCols();

  }

  public static Operator genMaterializedViewDataOrgPlan(List<ColumnInfo> sortColInfos,
      List<ColumnInfo> distributeColInfos, RowResolver inputRR, Operator input) {
    // In this case, we will introduce a RS and immediately after a SEL that restores
    // the row schema to what follow-up operations are expecting
    Set<String> keys = sortColInfos.stream().map(ColumnInfo::getInternalName).collect(Collectors.toSet());
    Set<String> distributeKeys =
        distributeColInfos.stream().map(ColumnInfo::getInternalName).collect(Collectors.toSet());
    List<ExprNodeDesc> keyCols = new ArrayList<>();
    List<String> keyColNames = new ArrayList<>();
    StringBuilder order = new StringBuilder();
    StringBuilder nullOrder = new StringBuilder();
    List<ExprNodeDesc> valCols = new ArrayList<>();
    List<String> valColNames = new ArrayList<>();
    List<ExprNodeDesc> partCols = new ArrayList<>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<>();
    Map<String, String> nameMapping = new HashMap<>();
    // map _col0 to KEY._col0, etc
    for (ColumnInfo ci : inputRR.getRowSchema().getSignature()) {
      ExprNodeColumnDesc e = new ExprNodeColumnDesc(ci);
      String columnName = ci.getInternalName();
      if (keys.contains(columnName)) {
        // key (sort column)
        keyColNames.add(columnName);
        keyCols.add(e);
        colExprMap.put(Utilities.ReduceField.KEY + "." + columnName, e);
        nameMapping.put(columnName, Utilities.ReduceField.KEY + "." + columnName);
        order.append("+");
        nullOrder.append("a");
      } else {
        // value
        valColNames.add(columnName);
        valCols.add(e);
        colExprMap.put(Utilities.ReduceField.VALUE + "." + columnName, e);
        nameMapping.put(columnName, Utilities.ReduceField.VALUE + "." + columnName);
      }
      if (distributeKeys.contains(columnName)) {
        // distribute column
        partCols.add(e.clone());
      }
    }
    // Create Key/Value TableDesc. When the operator plan is split into MR tasks,
    // the reduce operator will initialize Extract operator with information
    // from Key and Value TableDesc
    List<FieldSchema> fields = PlanUtils.getFieldSchemasFromColumnList(keyCols, keyColNames, 0, "");
    TableDesc keyTable = PlanUtils.getReduceKeyTableDesc(fields, order.toString(), nullOrder.toString());
    List<FieldSchema> valFields = PlanUtils.getFieldSchemasFromColumnList(valCols, valColNames, 0, "");
    TableDesc valueTable = PlanUtils.getReduceValueTableDesc(valFields);
    List<List<Integer>> distinctColumnIndices = new ArrayList<>();
    // Number of reducers is set to default (-1)
    ReduceSinkDesc rsConf = new ReduceSinkDesc(keyCols, keyCols.size(), valCols, keyColNames, distinctColumnIndices,
        valColNames, -1, partCols, -1, keyTable, valueTable, Operation.NOT_ACID);
    RowResolver rsRR = new RowResolver();
    List<ColumnInfo> rsSignature = new ArrayList<>();
    for (int index = 0; index < input.getSchema().getSignature().size(); index++) {
      ColumnInfo colInfo = new ColumnInfo(input.getSchema().getSignature().get(index));
      String[] nm = inputRR.reverseLookup(colInfo.getInternalName());
      String[] nm2 = inputRR.getAlternateMappings(colInfo.getInternalName());
      colInfo.setInternalName(nameMapping.get(colInfo.getInternalName()));
      rsSignature.add(colInfo);
      rsRR.put(nm[0], nm[1], colInfo);
      if (nm2 != null) {
        rsRR.addMappingOnly(nm2[0], nm2[1], colInfo);
      }
    }
    //FIXME
    Operator<?> result =
        /*putOpInsertMap*/(OperatorFactory.getAndMakeChild(rsConf, new RowSchema(rsSignature), input)/*, rsRR*/);
    result.setColumnExprMap(colExprMap);

    // Create SEL operator
    RowResolver selRR = new RowResolver();
    List<ColumnInfo> selSignature = new ArrayList<>();
    List<ExprNodeDesc> columnExprs = new ArrayList<>();
    List<String> colNames = new ArrayList<>();
    Map<String, ExprNodeDesc> selColExprMap = new HashMap<>();
    for (int index = 0; index < input.getSchema().getSignature().size(); index++) {
      ColumnInfo colInfo = new ColumnInfo(input.getSchema().getSignature().get(index));
      String[] nm = inputRR.reverseLookup(colInfo.getInternalName());
      String[] nm2 = inputRR.getAlternateMappings(colInfo.getInternalName());
      selSignature.add(colInfo);
      selRR.put(nm[0], nm[1], colInfo);
      if (nm2 != null) {
        selRR.addMappingOnly(nm2[0], nm2[1], colInfo);
      }
      String colName = colInfo.getInternalName();
      ExprNodeDesc exprNodeDesc;
      if (keys.contains(colName)) {
        exprNodeDesc =
            new ExprNodeColumnDesc(colInfo.getType(), ReduceField.KEY.toString() + "." + colName, null, false);
        columnExprs.add(exprNodeDesc);
      } else {
        exprNodeDesc =
            new ExprNodeColumnDesc(colInfo.getType(), ReduceField.VALUE.toString() + "." + colName, null, false);
        columnExprs.add(exprNodeDesc);
      }
      colNames.add(colName);
      selColExprMap.put(colName, exprNodeDesc);
    }
    SelectDesc selConf = new SelectDesc(columnExprs, colNames);
    result =
        /*putOpInsertMap*/(OperatorFactory.getAndMakeChild(selConf, new RowSchema(selSignature), result)/*, selRR*/);
    result.setColumnExprMap(selColExprMap);

    return result;
  }


  private void fixParallelEdge(Operator<? extends OperatorDesc> p, Operator<?> o) {
    LOG.info("Fixing parallel by adding a concentrator RS between {} -> {}", p, o);

    ReduceSinkDesc conf = (ReduceSinkDesc) p.getConf();
    ReduceSinkDesc newConf = (ReduceSinkDesc) conf.clone();

    Operator<SelectDesc> newSEL = buildSEL(p, conf);

    Operator<ReduceSinkDesc> newRS =
        OperatorFactory.getAndMakeChild(p.getCompilationOpContext(), newConf, new ArrayList<>());


    // alter old RS conf to forward only
    conf.setOutputName("forward_to_" + newRS);
    //    conf.setForwarding(true);
    conf.setTag(0);

    newConf.setKeyCols(createColumnRefs(conf.getKeyCols(), conf.getOutputKeyColumnNames()));
    newConf.setValueCols(createColumnRefs(conf.getValueCols(), conf.getOutputValueColumnNames()));
    newConf.setColumnExprMap(buildIdentityColumnExprMap(conf.getColumnExprMap()));
    //    newConf.setPartitionCols(partitionCols);
    newRS.setSchema(new RowSchema(p.getSchema()));

    p.replaceChild(o, newSEL);

    newSEL.setParentOperators(Lists.newArrayList(p));
    newSEL.setChildOperators(Lists.newArrayList(newRS));
    newRS.setParentOperators(Lists.newArrayList(newSEL));
    newRS.setChildOperators(Lists.newArrayList(o));

    //    newSEL.getChildOperators().add(newRS);

    o.replaceParent(p, newRS);

  }

  private Operator<SelectDesc> buildSEL(Operator<? extends OperatorDesc> p, ReduceSinkDesc conf) {
    List<ExprNodeDesc> colList = new ArrayList<>();
    List<String> outputColumnNames = new ArrayList<>();
    List<ColumnInfo> newColumns = new ArrayList<>();
    for (Entry<String, ExprNodeDesc> e : conf.getColumnExprMap().entrySet()) {

      String colName = e.getKey();
      if (colName.contains("reducesinkkey")) {
        int asd = 1;
      }

      ExprNodeDesc expr = e.getValue();
      ExprNodeDesc colRef = new ExprNodeColumnDesc(expr.getTypeInfo(), colName, colName, false);

      colList.add(colRef);
      String newColName = colName.replaceAll(".*\\.", "");
      //      if (colName.startsWith("KEY")) {
      //        newColName = conf.getKeyCols().get(0);
      //      }
      outputColumnNames.add(newColName);
      ColumnInfo newColInfo = new ColumnInfo(p.getSchema().getColumnInfo(colName));
      newColInfo.setInternalName(newColName);
      newColumns.add(newColInfo);

    }
    SelectDesc selConf = new SelectDesc(colList, outputColumnNames);
    Operator<SelectDesc> newSEL =
        OperatorFactory.getAndMakeChild(p.getCompilationOpContext(), selConf, new ArrayList<>());

    newSEL.setSchema(new RowSchema(newColumns));

    return newSEL;
  }

  private Map<String, ExprNodeDesc> buildIdentityColumnExprMap(Map<String, ExprNodeDesc> columnExprMap) {

    Map<String, ExprNodeDesc> ret = new HashMap<String, ExprNodeDesc>();
    for (Entry<String, ExprNodeDesc> e : columnExprMap.entrySet()) {
      String colName = e.getKey();
      ExprNodeDesc expr = e.getValue();

      ExprNodeDesc colRef = new ExprNodeColumnDesc(expr.getTypeInfo(), colName, colName, false);
      ret.put(colName, colRef);
    }
    return ret;

  }

  private List<ExprNodeDesc> createColumnRefs(List<ExprNodeDesc> oldExprs, List<String> newColumnNames) {
    List<ExprNodeDesc> newExprs = new ArrayList<ExprNodeDesc>();
    for (int i = 0; i < newColumnNames.size(); i++) {
      ExprNodeDesc expr = oldExprs.get(i);
      String name = newColumnNames.get(i);

      ExprNodeDesc colRef = new ExprNodeColumnDesc(expr.getTypeInfo(), name, name, false);
      newExprs.add(colRef);

    }
    return newExprs;
  }

  private void assignGroupVersions(Set<OpGroup> groups) {
    for (OpGroup opGroup : groups) {
      opGroup.analyzeBucketVersion();
      opGroup.setBucketVersion();
    }

  }

  static class BucketVersionProcessorCtx implements NodeProcessorCtx {
    Set<OpGroup> groups = new HashSet<OpGroup>();
  }

  private Set<OpGroup> findOpGroups(ParseContext pctx) throws SemanticException {

    BucketVersionProcessorCtx ctx = new BucketVersionProcessorCtx();

    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();

    SemanticDispatcher disp = new DefaultRuleDispatcher(new IdentifyBucketGroups(), opRules, ctx);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return ctx.groups;
  }

  /**
   * This rule decomposes the operator tree into group which may have different bucketing versions.
   */
  private static class IdentifyBucketGroups implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
        throws SemanticException {
      Operator<?> o = (Operator<?>) nd;
      OpGroup g;
      if (nodeOutputs.length == 0 || o instanceof ReduceSinkOperator) {
        g = newGroup(procCtx);
      } else {
        g = (OpGroup) nodeOutputs[0];
      }
      for (int i = 1; i < nodeOutputs.length; i++) {
        g.merge((OpGroup) nodeOutputs[i]);
      }
      g.add(o);
        return g;
    }

    private OpGroup newGroup(NodeProcessorCtx procCtx) {
      BucketVersionProcessorCtx ctx = (BucketVersionProcessorCtx) procCtx;
      OpGroup g = new OpGroup();
      ctx.groups.add(g);
      return g;
    }
  }

  /**
   * This class represents the version required by an Operator.
   */
  private static class OperatorBucketingVersionInfo {

    private Operator<?> op;
    private int bucketingVersion;

    public OperatorBucketingVersionInfo(Operator<?> op, int bucketingVersion) {
      this.op = op;
      this.bucketingVersion = bucketingVersion;
    }

    @Override
    public String toString() {
      return String.format("[op: %s, bucketingVersion=%d]", op, bucketingVersion);
    }
  }

  /**
   * A Group of operators which must have the same bucketing version.
   */
  public static class OpGroup {
    Set<Operator<?>> members = Sets.newIdentityHashSet();
    int version = -1;

    public OpGroup() {
    }

    public void add(Operator<?> o) {
      members.add(o);
    }

    public void setBucketVersion() {
      for (Operator<?> operator : members) {
        operator.getConf().setBucketingVersion(version);
        LOG.debug("Bucketing version for {} is set to {}", operator, version);
      }
    }

    List<OperatorBucketingVersionInfo> getBucketingVersions() {
      List<OperatorBucketingVersionInfo> ret = new ArrayList<>();
      for (Operator<?> operator : members) {
        if (operator instanceof TableScanOperator) {
          TableScanOperator tso = (TableScanOperator) operator;
          int bucketingVersion = tso.getConf().getTableMetadata().getBucketingVersion();
          int numBuckets = tso.getConf().getNumBuckets();
          if (numBuckets > 1) {
            ret.add(new OperatorBucketingVersionInfo(operator, bucketingVersion));
          } else {
            LOG.info("not considering bucketingVersion for: %s because it has %d<2 buckets ", tso, numBuckets);
          }
        }
        if (operator instanceof FileSinkOperator) {
          FileSinkOperator fso = (FileSinkOperator) operator;
          int bucketingVersion = fso.getConf().getTableInfo().getBucketingVersion();
          ret.add(new OperatorBucketingVersionInfo(operator, bucketingVersion));
        }
      }
      return ret;
    }

    public void analyzeBucketVersion() {
      List<OperatorBucketingVersionInfo> bucketingVersions = getBucketingVersions();
      try {
        for (OperatorBucketingVersionInfo info : bucketingVersions) {
          setVersion(info.bucketingVersion);
        }
      } catch (Exception e) {
        throw new RuntimeException("Error setting bucketingVersion for group: " + bucketingVersions, e);
      }
      if (version == -1) {
        // use version 2 if possible
        version = 2;
      }
    }

    private void setVersion(int newVersion) {
      if (version == newVersion || newVersion == -1) {
        return;
      }
      if (version == -1) {
        version = newVersion;
        return;
      }
      throw new RuntimeException("Unable to set version");
    }

    public void merge(OpGroup opGroup) {
      for (Operator<?> operator : opGroup.members) {
        add(operator);
      }
      opGroup.members.clear();
    }
  }
}
