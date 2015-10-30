/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.CollectOperator;
import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.DemuxOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.ForwardOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.HashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewForwardOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.ListSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MuxOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.SparkHashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.TemporaryHashSinkOperator;
import org.apache.hadoop.hive.ql.exec.UDTFOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorFilterOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorLimitOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSelectOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSparkHashTableSinkOperator;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.HashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.LateralViewJoinDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;
import org.apache.hadoop.hive.ql.plan.ScriptDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.SparkHashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;

public class OperatorComparatorFactory {
  private static final Map<Class<?>, OperatorComparator> comparatorMapping = Maps.newHashMap();
  private static final Logger LOG = LoggerFactory.getLogger(OperatorComparatorFactory.class);

  static {
    comparatorMapping.put(TableScanOperator.class, new TableScanOperatorComparator());
    comparatorMapping.put(SelectOperator.class, new SelectOperatorComparator());
    comparatorMapping.put(FilterOperator.class, new FilterOperatorComparator());
    comparatorMapping.put(GroupByOperator.class, new GroupByOperatorComparator());
    comparatorMapping.put(ReduceSinkOperator.class, new ReduceSinkOperatorComparator());
    comparatorMapping.put(FileSinkOperator.class, new FileSinkOperatorComparator());
    comparatorMapping.put(JoinOperator.class, new JoinOperatorComparator());
    comparatorMapping.put(MapJoinOperator.class, new MapJoinOperatorComparator());
    comparatorMapping.put(SMBMapJoinOperator.class, new SMBMapJoinOperatorComparator());
    comparatorMapping.put(LimitOperator.class, new LimitOperatorComparator());
    comparatorMapping.put(SparkHashTableSinkOperator.class, new SparkHashTableSinkOperatorComparator());
    comparatorMapping.put(VectorSparkHashTableSinkOperator.class,
        new SparkHashTableSinkOperatorComparator());
    comparatorMapping.put(LateralViewJoinOperator.class, new LateralViewJoinOperatorComparator());
    comparatorMapping.put(VectorGroupByOperator.class, new VectorGroupByOperatorComparator());
    comparatorMapping.put(CommonMergeJoinOperator.class, new MapJoinOperatorComparator());
    comparatorMapping.put(VectorFilterOperator.class, new FilterOperatorComparator());
    comparatorMapping.put(UDTFOperator.class, new UDTFOperatorComparator());
    comparatorMapping.put(VectorSelectOperator.class, new VectorSelectOperatorComparator());
    comparatorMapping.put(VectorLimitOperator.class, new LimitOperatorComparator());
    comparatorMapping.put(ScriptOperator.class, new ScriptOperatorComparator());
    comparatorMapping.put(TemporaryHashSinkOperator.class, new HashTableSinkOperatorComparator());
    // these operators does not have state, so they always equal with the same kind.
    comparatorMapping.put(UnionOperator.class, new AlwaysTrueOperatorComparator());
    comparatorMapping.put(ForwardOperator.class, new AlwaysTrueOperatorComparator());
    comparatorMapping.put(LateralViewForwardOperator.class, new AlwaysTrueOperatorComparator());
    comparatorMapping.put(DemuxOperator.class, new AlwaysTrueOperatorComparator());
    comparatorMapping.put(MuxOperator.class, new AlwaysTrueOperatorComparator());
    comparatorMapping.put(ListSinkOperator.class, new AlwaysTrueOperatorComparator());
    comparatorMapping.put(CollectOperator.class, new AlwaysTrueOperatorComparator());
    // do not support PTFOperator comparing now.
    comparatorMapping.put(PTFOperator.class, AlwaysFalseOperatorComparator.getInstance());
  }

  public static OperatorComparator getOperatorComparator(Class<? extends Operator> operatorClass) {
    OperatorComparator operatorComparator = comparatorMapping.get(operatorClass);
    if (operatorComparator == null) {
      LOG.warn("No OperatorComparator is registered for " + operatorClass.getName() +
          ". Default to always false comparator.");
      return AlwaysFalseOperatorComparator.getInstance();
    }

    return operatorComparator;
  }

  public interface OperatorComparator<T extends Operator<?>> {
    boolean equals(T op1, T op2);
  }

  static class AlwaysTrueOperatorComparator implements OperatorComparator<Operator<?>> {

    @Override
    public boolean equals(Operator<?> op1, Operator<?> op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      return true;
    }
  }

  static class AlwaysFalseOperatorComparator implements OperatorComparator<Operator<?>> {
    // the outer class is responsible for maintaining the comparator singleton
    private AlwaysFalseOperatorComparator() {
    }

    private static final AlwaysFalseOperatorComparator instance =
        new AlwaysFalseOperatorComparator();

    public static AlwaysFalseOperatorComparator getInstance() {
      return instance;
    }

    @Override
    public boolean equals(Operator<?> op1, Operator<?> op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      return false;
    }
  }

  static class TableScanOperatorComparator implements OperatorComparator<TableScanOperator> {

    @Override
    public boolean equals(TableScanOperator op1, TableScanOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      TableScanDesc op1Conf = op1.getConf();
      TableScanDesc op2Conf = op2.getConf();

      if (compareString(op1Conf.getAlias(), op2Conf.getAlias()) &&
        compareExprNodeDesc(op1Conf.getFilterExpr(), op2Conf.getFilterExpr()) &&
        op1Conf.getRowLimit() == op2Conf.getRowLimit() &&
        op1Conf.isGatherStats() == op2Conf.isGatherStats()) {
        return true;
      } else {
        return false;
      }
    }
  }

  static class SelectOperatorComparator implements OperatorComparator<SelectOperator> {

    @Override
    public boolean equals(SelectOperator op1, SelectOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      SelectDesc op1Conf = op1.getConf();
      SelectDesc op2Conf = op2.getConf();

      if (compareString(op1Conf.getColListString(), op2Conf.getColListString()) &&
        compareObject(op1Conf.getOutputColumnNames(), op2Conf.getOutputColumnNames()) &&
        compareString(op1Conf.explainNoCompute(), op2Conf.explainNoCompute())) {
        return true;
      } else {
        return false;
      }
    }
  }

  static class VectorSelectOperatorComparator implements OperatorComparator<VectorSelectOperator> {

    @Override
    public boolean equals(VectorSelectOperator op1, VectorSelectOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      SelectDesc op1Conf = op1.getConf();
      SelectDesc op2Conf = op2.getConf();

      if (compareString(op1Conf.getColListString(), op2Conf.getColListString()) &&
        compareObject(op1Conf.getOutputColumnNames(), op2Conf.getOutputColumnNames()) &&
        compareString(op1Conf.explainNoCompute(), op2Conf.explainNoCompute())) {
        return true;
      } else {
        return false;
      }
    }
  }

  static class FilterOperatorComparator implements OperatorComparator<FilterOperator> {

    @Override
    public boolean equals(FilterOperator op1, FilterOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      FilterDesc op1Conf = op1.getConf();
      FilterDesc op2Conf = op2.getConf();

      if (compareString(op1Conf.getPredicateString(), op2Conf.getPredicateString()) &&
        (op1Conf.getIsSamplingPred() == op2Conf.getIsSamplingPred()) &&
        compareString(op1Conf.getSampleDescExpr(), op2Conf.getSampleDescExpr())) {
        return true;
      } else {
        return false;
      }
    }
  }

  static class GroupByOperatorComparator implements OperatorComparator<GroupByOperator> {

    @Override
    public boolean equals(GroupByOperator op1, GroupByOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      GroupByDesc op1Conf = op1.getConf();
      GroupByDesc op2Conf = op2.getConf();

      if (compareString(op1Conf.getModeString(), op2Conf.getModeString()) &&
        compareString(op1Conf.getKeyString(), op2Conf.getKeyString()) &&
        compareObject(op1Conf.getOutputColumnNames(), op2Conf.getOutputColumnNames()) &&
        op1Conf.pruneGroupingSetId() == op2Conf.pruneGroupingSetId() &&
        compareObject(op1Conf.getAggregatorStrings(), op2Conf.getAggregatorStrings()) &&
        op1Conf.getBucketGroup() == op2Conf.getBucketGroup()) {
        return true;
      } else {
        return false;
      }
    }
  }

  static class VectorGroupByOperatorComparator implements OperatorComparator<VectorGroupByOperator> {

    @Override
    public boolean equals(VectorGroupByOperator op1, VectorGroupByOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      GroupByDesc op1Conf = op1.getConf();
      GroupByDesc op2Conf = op2.getConf();

      if (compareString(op1Conf.getModeString(), op2Conf.getModeString()) &&
        compareString(op1Conf.getKeyString(), op2Conf.getKeyString()) &&
        compareObject(op1Conf.getOutputColumnNames(), op2Conf.getOutputColumnNames()) &&
        op1Conf.pruneGroupingSetId() == op2Conf.pruneGroupingSetId() &&
        compareObject(op1Conf.getAggregatorStrings(), op2Conf.getAggregatorStrings()) &&
        op1Conf.getBucketGroup() == op2Conf.getBucketGroup()) {
        return true;
      } else {
        return false;
      }
    }
  }


  static class ReduceSinkOperatorComparator implements OperatorComparator<ReduceSinkOperator> {

    @Override
    public boolean equals(ReduceSinkOperator op1, ReduceSinkOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      ReduceSinkDesc op1Conf = op1.getConf();
      ReduceSinkDesc op2Conf = op2.getConf();

      if (compareExprNodeDescList(op1Conf.getKeyCols(), op2Conf.getKeyCols()) &&
        compareExprNodeDescList(op1Conf.getValueCols(), op2Conf.getValueCols()) &&
        compareExprNodeDescList(op1Conf.getPartitionCols(), op2Conf.getPartitionCols()) &&
        op1Conf.getTag() == op2Conf.getTag() &&
        compareString(op1Conf.getOrder(), op2Conf.getOrder()) &&
        op1Conf.getTopN() == op2Conf.getTopN() &&
        op1Conf.isAutoParallel() == op2Conf.isAutoParallel()) {
        return true;
      } else {
        return false;
      }
    }
  }

  static class FileSinkOperatorComparator implements OperatorComparator<FileSinkOperator> {

    @Override
    public boolean equals(FileSinkOperator op1, FileSinkOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      FileSinkDesc op1Conf = op1.getConf();
      FileSinkDesc op2Conf = op2.getConf();

      if (compareObject(op1Conf.getDirName(), op2Conf.getDirName()) &&
        compareObject(op1Conf.getTableInfo(), op2Conf.getTableInfo()) &&
        op1Conf.getCompressed() == op2Conf.getCompressed() &&
        op1Conf.getDestTableId() == op2Conf.getDestTableId() &&
        op1Conf.isMultiFileSpray() == op2Conf.isMultiFileSpray() &&
        op1Conf.getTotalFiles() == op2Conf.getTotalFiles() &&
        op1Conf.getNumFiles() == op2Conf.getNumFiles() &&
        compareString(op1Conf.getStaticSpec(), op2Conf.getStaticSpec()) &&
        op1Conf.isGatherStats() == op2Conf.isGatherStats() &&
        compareString(op1Conf.getStatsAggPrefix(), op2Conf.getStatsAggPrefix())) {
        return true;
      } else {
        return false;
      }
    }
  }

  static class JoinOperatorComparator implements OperatorComparator<JoinOperator> {

    @Override
    public boolean equals(JoinOperator op1, JoinOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      JoinDesc desc1 = op1.getConf();
      JoinDesc desc2 = op2.getConf();

      if (compareObject(desc1.getKeysString(), desc2.getKeysString()) &&
        compareObject(desc1.getFiltersStringMap(), desc2.getFiltersStringMap()) &&
        compareObject(desc1.getOutputColumnNames(), desc2.getOutputColumnNames()) &&
        compareObject(desc1.getCondsList(), desc2.getCondsList()) &&
        desc1.getHandleSkewJoin() == desc2.getHandleSkewJoin() &&
        compareString(desc1.getNullSafeString(), desc2.getNullSafeString())) {
        return true;
      } else {
        return false;
      }
    }
  }

  static class MapJoinOperatorComparator implements OperatorComparator<MapJoinOperator> {

    @Override
    public boolean equals(MapJoinOperator op1, MapJoinOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      MapJoinDesc desc1 = op1.getConf();
      MapJoinDesc desc2 = op2.getConf();

      if (compareObject(desc1.getParentToInput(), desc2.getParentToInput()) &&
        compareString(desc1.getKeyCountsExplainDesc(), desc2.getKeyCountsExplainDesc()) &&
        compareObject(desc1.getKeysString(), desc2.getKeysString()) &&
        desc1.getPosBigTable() == desc2.getPosBigTable() &&
        desc1.isBucketMapJoin() == desc2.isBucketMapJoin() &&
        compareObject(desc1.getKeysString(), desc2.getKeysString()) &&
        compareObject(desc1.getFiltersStringMap(), desc2.getFiltersStringMap()) &&
        compareObject(desc1.getOutputColumnNames(), desc2.getOutputColumnNames()) &&
        compareObject(desc1.getCondsList(), desc2.getCondsList()) &&
        desc1.getHandleSkewJoin() == desc2.getHandleSkewJoin() &&
        compareString(desc1.getNullSafeString(), desc2.getNullSafeString())) {
        return true;
      } else {
        return false;
      }
    }
  }

  static class SMBMapJoinOperatorComparator implements OperatorComparator<SMBMapJoinOperator> {

    @Override
    public boolean equals(SMBMapJoinOperator op1, SMBMapJoinOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      SMBJoinDesc desc1 = op1.getConf();
      SMBJoinDesc desc2 = op2.getConf();

      if (compareObject(desc1.getParentToInput(), desc2.getParentToInput()) &&
        compareString(desc1.getKeyCountsExplainDesc(), desc2.getKeyCountsExplainDesc()) &&
        compareObject(desc1.getKeysString(), desc2.getKeysString()) &&
        desc1.getPosBigTable() == desc2.getPosBigTable() &&
        desc1.isBucketMapJoin() == desc2.isBucketMapJoin() &&
        compareObject(desc1.getKeysString(), desc2.getKeysString()) &&
        compareObject(desc1.getFiltersStringMap(), desc2.getFiltersStringMap()) &&
        compareObject(desc1.getOutputColumnNames(), desc2.getOutputColumnNames()) &&
        compareObject(desc1.getCondsList(), desc2.getCondsList()) &&
        desc1.getHandleSkewJoin() == desc2.getHandleSkewJoin() &&
        compareString(desc1.getNullSafeString(), desc2.getNullSafeString())) {
        return true;
      } else {
        return false;
      }
    }
  }

  static class LimitOperatorComparator implements OperatorComparator<LimitOperator> {

    @Override
    public boolean equals(LimitOperator op1, LimitOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      LimitDesc desc1 = op1.getConf();
      LimitDesc desc2 = op2.getConf();

      return desc1.getLimit() == desc2.getLimit();
    }
  }

  static class SparkHashTableSinkOperatorComparator implements OperatorComparator<SparkHashTableSinkOperator> {

    @Override
    public boolean equals(SparkHashTableSinkOperator op1, SparkHashTableSinkOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      SparkHashTableSinkDesc desc1 = op1.getConf();
      SparkHashTableSinkDesc desc2 = op2.getConf();

      if (compareObject(desc1.getFilterMapString(), desc2.getFilterMapString()) &&
        compareObject(desc1.getKeysString(), desc2.getKeysString()) &&
        desc1.getPosBigTable() == desc2.getPosBigTable() &&
        compareObject(desc1.getKeysString(), desc2.getKeysString()) &&
        compareObject(desc1.getFiltersStringMap(), desc2.getFiltersStringMap()) &&
        compareObject(desc1.getOutputColumnNames(), desc2.getOutputColumnNames()) &&
        compareObject(desc1.getCondsList(), desc2.getCondsList()) &&
        desc1.getHandleSkewJoin() == desc2.getHandleSkewJoin() &&
        compareString(desc1.getNullSafeString(), desc2.getNullSafeString())) {
        return true;
      } else {
        return false;
      }
    }
  }

  static class HashTableSinkOperatorComparator implements OperatorComparator<HashTableSinkOperator> {

    @Override
    public boolean equals(HashTableSinkOperator op1, HashTableSinkOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      HashTableSinkDesc desc1 = op1.getConf();
      HashTableSinkDesc desc2 = op2.getConf();

      if (compareObject(desc1.getFilterMapString(), desc2.getFilterMapString()) &&
        compareObject(desc1.getKeysString(), desc2.getKeysString()) &&
        desc1.getPosBigTable() == desc2.getPosBigTable() &&
        compareObject(desc1.getKeysString(), desc2.getKeysString()) &&
        compareObject(desc1.getFiltersStringMap(), desc2.getFiltersStringMap()) &&
        compareObject(desc1.getOutputColumnNames(), desc2.getOutputColumnNames()) &&
        compareObject(desc1.getCondsList(), desc2.getCondsList()) &&
        desc1.getHandleSkewJoin() == desc2.getHandleSkewJoin() &&
        compareString(desc1.getNullSafeString(), desc2.getNullSafeString())) {
        return true;
      } else {
        return false;
      }
    }
  }

  static class LateralViewJoinOperatorComparator implements OperatorComparator<LateralViewJoinOperator> {

    @Override
    public boolean equals(LateralViewJoinOperator op1, LateralViewJoinOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      LateralViewJoinDesc desc1 = op1.getConf();
      LateralViewJoinDesc desc2 = op2.getConf();

      return compareObject(desc1.getOutputInternalColNames(), desc2.getOutputInternalColNames());
    }
  }

  static class ScriptOperatorComparator implements OperatorComparator<ScriptOperator> {

    @Override
    public boolean equals(ScriptOperator op1, ScriptOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      ScriptDesc desc1 = op1.getConf();
      ScriptDesc desc2 = op2.getConf();

      if (compareString(desc1.getScriptCmd(), desc2.getScriptCmd()) &&
        compareObject(desc1.getScriptOutputInfo(), desc2.getScriptOutputInfo())) {
        return true;
      } else {
        return false;
      }
    }
  }

  static class UDTFOperatorComparator implements OperatorComparator<UDTFOperator> {

    @Override
    public boolean equals(UDTFOperator op1, UDTFOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      UDTFDesc desc1 = op1.getConf();
      UDTFDesc desc2 = op2.getConf();

      if (compareString(desc1.getUDTFName(), desc2.getUDTFName()) &&
        compareString(desc1.isOuterLateralView(), desc2.isOuterLateralView())) {
        return true;
      } else {
        return false;
      }
    }
  }

  static boolean compareString(String first, String second) {
    return compareObject(first, second);
  }

  /*
   * Compare Objects which implements its own meaningful equals methods.
   */
  static boolean compareObject(Object first, Object second) {
    return first == null ? second == null : first.equals(second);
  }

  static boolean compareExprNodeDesc(ExprNodeDesc first, ExprNodeDesc second) {
    return first == null ? second == null : first.isSame(second);
  }

  static boolean compareExprNodeDescList(List<ExprNodeDesc> first, List<ExprNodeDesc> second) {
    if (first == null && second == null) {
      return true;
    }
    if ((first == null && second != null) || (first != null && second == null)) {
      return false;
    }
    if (first.size() != second.size()) {
      return false;
    } else {
      for (int i = 0; i < first.size(); i++) {
        if (!first.get(i).isSame(second.get(i))) {
          return false;
        }
      }
    }
    return true;
  }
}
