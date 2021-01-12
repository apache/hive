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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.calcite.util.Pair;
import org.apache.commons.collections4.ListValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * FIXME
 */
public class ParallelEdgeFixer extends Transform {

  protected static final Logger LOG = LoggerFactory.getLogger(ParallelEdgeFixer.class);

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    Set<OpGroup> groups = findOpGroups(pctx);
    fixParallelEdges(groups);
    return pctx;
  }

  private static class Cmp1 implements Comparator<Pair<Operator, Operator>> {
    @Override
    public int compare(Pair<Operator, Operator> o1, Pair<Operator, Operator> o2) {
      return sig(o1).compareTo(sig(o2));

    }

    private String sig(Pair<Operator, Operator> o1) {
      return o1.left.toString() + o1.right.toString();
    }

  }

  /**
   * Adds an intermediate reduce sink inbetweeen p and o.
   */
  private void fixParallelEdges(Set<OpGroup> groups) {
    Map<Operator, OpGroup> op2group = new HashMap<>();
    for (OpGroup g : groups) {
      for (Operator<?> o : g.members) {
        op2group.put(o, g);
      }
    }

    for (OpGroup g : groups) {
      ListValuedMap<Pair<OpGroup, OpGroup>, Pair<Operator, Operator>> edgeOperators =
          new ArrayListValuedHashMap<Pair<OpGroup, OpGroup>, Pair<Operator, Operator>>();
      for (Operator<?> o : g.members) {
        for (Operator<? extends OperatorDesc> p : o.getParentOperators()) {
          OpGroup parentGroup = op2group.get(p);
          if (parentGroup == g) {
            continue;
          }
          edgeOperators.put(new Pair(parentGroup, g), new Pair(p, o));
        }
      }

      for (Pair<OpGroup, OpGroup> key : edgeOperators.keySet()) {
        List<Pair<Operator, Operator>> values = edgeOperators.get(key);
        if(values.size() <=1) {
          continue;
        }
        // operator order must in stabile order - or we end up with falky plans causing flaky tests...
        values.sort(new Cmp1());

        // remove one optionally unsupported edge (it will be kept as is)
        removeOneEdge(values);

        Iterator<Pair<Operator, Operator>> it = values.iterator();
        while (it.hasNext()) {
          Pair<Operator, Operator> pair = it.next();
          fixParallelEdge(pair.left, pair.right);
        }
      }
    }
  }

  private void removeOneEdge(List<Pair<Operator, Operator>> values) {
    Pair<Operator, Operator> toKeep = null;
    for (Pair<Operator, Operator> pair : values) {
      if (!isParallelEdgeSupported(pair)) {

        if (toKeep != null) {
          throw new RuntimeException("More than one operators which should not be reshuffled!");
        }
        toKeep = pair;
      }
    }
    toKeep=values.get(values.size() - 1);
    values.remove(toKeep);
  }

  private boolean isParallelEdgeSupported(Pair<Operator, Operator> pair) {
    ReduceSinkOperator rs = (ReduceSinkOperator) pair.left;
    Operator<?> child = pair.right;

    if (child instanceof MapJoinOperator) {
      return true;
    }

    if (child instanceof TableScanOperator) {
      return true;
    }
    return false;

  }

  /**
   * Fixes a parallel edge going into a mapjoin by introducing a concentrator RS.
   */
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

    newConf.setKeyCols(new ArrayList(conf.getKeyCols()));
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
      ExprNodeDesc expr = e.getValue();
      if (colName.contains("reducesinkkey")) {
        int asd = 1;
      }

      ExprNodeDesc colRef = new ExprNodeColumnDesc(expr.getTypeInfo(), colName, colName, false);

      colList.add(colRef);
      //      String newColName = colName.replaceAll(".*\\.", "");
      String newColName = extractColumnName(expr);
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

  private String extractColumnName(ExprNodeDesc expr) {
    if (expr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc exprNodeColumnDesc = (ExprNodeColumnDesc) expr;
      return exprNodeColumnDesc.getColumn();

    }
    throw new RuntimeException("unexpected mapping expression!");

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

  static class BucketVersionProcessorCtx implements NodeProcessorCtx {
    Set<OpGroup> groups = new LinkedHashSet<OpGroup>();
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
