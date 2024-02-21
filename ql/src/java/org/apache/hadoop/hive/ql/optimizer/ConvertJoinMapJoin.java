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

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MemoryMonitorInfo;
import org.apache.hadoop.hive.ql.exec.MuxOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.TezDummyStoreOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.physical.LlapClusterStateForCompile;
import org.apache.hadoop.hive.ql.parse.GenTezUtils;
import org.apache.hadoop.hive.ql.parse.OptimizeTezProcContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.CommonMergeJoinDesc;
import org.apache.hadoop.hive.ql.plan.DummyStoreDesc;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OpTraits;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.math.DoubleMath;

import static org.apache.hadoop.hive.ql.exec.OperatorUtils.hasMoreOperatorsThan;

/**
 * ConvertJoinMapJoin is an optimization that replaces a common join
 * (aka shuffle join) with a map join (aka broadcast or fragment replicate
 * join when possible. Map joins have restrictions on which joins can be
 * converted (e.g.: full outer joins cannot be handled as map joins) as well
 * as memory restrictions (one side of the join has to fit into memory).
 */
public class ConvertJoinMapJoin implements SemanticNodeProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(ConvertJoinMapJoin.class.getName());
  private static final int DEFAULT_MAX_EXECUTORS_PER_QUERY_CONTAINER_MODE = 3;
  public float hashTableLoadFactor;
  private long maxJoinMemory;
  private HashMapDataStructureType hashMapDataStructure;
  private boolean fastHashTableAvailable;

  @Override
  /*
   * (non-Javadoc) we should ideally not modify the tree we traverse. However,
   * since we need to walk the tree at any time when we modify the operator, we
   * might as well do it here.
   */
  public Object
      process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
          throws SemanticException {

    OptimizeTezProcContext context = (OptimizeTezProcContext) procCtx;

    hashTableLoadFactor = context.conf.getFloatVar(ConfVars.HIVE_HASHTABLE_LOAD_FACTOR);
    fastHashTableAvailable = context.conf.getBoolVar(ConfVars.HIVE_VECTORIZATION_MAPJOIN_NATIVE_FAST_HASHTABLE_ENABLED);

    JoinOperator joinOp = (JoinOperator) nd;
    // adjust noconditional task size threshold for LLAP
    LlapClusterStateForCompile llapInfo = null;
    if ("llap".equalsIgnoreCase(context.conf.getVar(ConfVars.HIVE_EXECUTION_MODE))) {
      llapInfo = LlapClusterStateForCompile.getClusterInfo(context.conf);
      llapInfo.initClusterInfo();
    }
    MemoryMonitorInfo memoryMonitorInfo = getMemoryMonitorInfo(context.conf, llapInfo);
    joinOp.getConf().setMemoryMonitorInfo(memoryMonitorInfo);
    maxJoinMemory = memoryMonitorInfo.getAdjustedNoConditionalTaskSize();

    LOG.info("maxJoinMemory: {}", maxJoinMemory);

    hashMapDataStructure = HashMapDataStructureType.of(joinOp.getConf());


    TezBucketJoinProcCtx tezBucketJoinProcCtx = new TezBucketJoinProcCtx(context.conf);
    boolean hiveConvertJoin = context.conf.getBoolVar(HiveConf.ConfVars.HIVE_CONVERT_JOIN) &
            !context.parseContext.getDisableMapJoin();
    if (!hiveConvertJoin) {
      // we are just converting to a common merge join operator. The shuffle
      // join in map-reduce case.
      Object retval = checkAndConvertSMBJoin(context, joinOp, tezBucketJoinProcCtx);
      if (retval == null) {
        return retval;
      } else {
        fallbackToReduceSideJoin(joinOp, context);
        return null;
      }
    }

    // if we have traits, and table info is present in the traits, we know the
    // exact number of buckets. Else choose the largest number of estimated
    // reducers from the parent operators.
    int numBuckets = -1;
    if (context.conf.getBoolVar(HiveConf.ConfVars.HIVE_CONVERT_JOIN_BUCKET_MAPJOIN_TEZ)) {
      numBuckets = estimateNumBuckets(joinOp, true);
    } else {
      numBuckets = 1;
    }
    LOG.info("Estimated number of buckets " + numBuckets);
    MapJoinConversion mapJoinConversion =
        getMapJoinConversion(joinOp, context, numBuckets, false, maxJoinMemory, true);
    if (mapJoinConversion == null) {
      Object retval = checkAndConvertSMBJoin(context, joinOp, tezBucketJoinProcCtx);
      if (retval == null) {
        return retval;
      } else {
        // only case is full outer join with SMB enabled which is not possible. Convert to regular
        // join.
        fallbackToReduceSideJoin(joinOp, context);
        return null;
      }
    }

    if (numBuckets > 1) {
      if (context.conf.getBoolVar(HiveConf.ConfVars.HIVE_CONVERT_JOIN_BUCKET_MAPJOIN_TEZ)) {
        // Check if we are in LLAP, if so it needs to be determined if we should use BMJ or DPHJ
        if (llapInfo != null) {
          if (selectJoinForLlap(context, joinOp, tezBucketJoinProcCtx, llapInfo, mapJoinConversion, numBuckets)) {
            return null;
          }
        } else if (convertJoinBucketMapJoin(joinOp, context, mapJoinConversion, tezBucketJoinProcCtx)) {
          return null;
        }
      }
    }

    // check if we can convert to map join no bucket scaling.
    LOG.info("Convert to non-bucketed map join");
    if (numBuckets != 1) {
      mapJoinConversion = getMapJoinConversion(joinOp, context, 1, false, maxJoinMemory, true);
    }
    if (mapJoinConversion == null) {
      // we are just converting to a common merge join operator. The shuffle
      // join in map-reduce case.
      fallbackToReduceSideJoin(joinOp, context);
      return null;
    }

    // Currently, this is a MJ path and we don's support FULL OUTER MapJoin yet.
    if (mapJoinConversion.getIsFullOuterJoin() &&
        !mapJoinConversion.getIsFullOuterEnabledForMapJoin()) {
      fallbackToReduceSideJoin(joinOp, context);
      return null;
    }

    MapJoinOperator mapJoinOp = convertJoinMapJoin(joinOp, context, mapJoinConversion, true);
    if (mapJoinOp == null) {
      fallbackToReduceSideJoin(joinOp, context);
      return null;
    }
    // map join operator by default has no bucket cols and num of reduce sinks
    // reduced by 1
    mapJoinOp.setOpTraits(new OpTraits(null, -1, null,
        joinOp.getOpTraits().getNumReduceSinks()));
    preserveOperatorInfos(mapJoinOp, joinOp, context);
    // propagate this change till the next RS
    for (Operator<? extends OperatorDesc> childOp : mapJoinOp.getChildOperators()) {
      setAllChildrenTraits(childOp, mapJoinOp.getOpTraits());
    }

    return null;
  }

  private enum HashMapDataStructureType {
    COMPOSITE_KEYED, LONG_KEYED;

    public static HashMapDataStructureType of(JoinDesc conf) {
      ExprNodeDesc[][] keys = conf.getJoinKeys();
      if (keys != null && keys[0].length == 1) {
        TypeInfo typeInfo = keys[0][0].getTypeInfo();
        if (typeInfo instanceof PrimitiveTypeInfo) {
          PrimitiveTypeInfo pti = ((PrimitiveTypeInfo) typeInfo);
          PrimitiveCategory pCat = pti.getPrimitiveCategory();
          switch (pCat) {
          case BOOLEAN:
          case BYTE:
          case SHORT:
          case INT:
          case LONG:
            return HashMapDataStructureType.LONG_KEYED;
          default:
            break;
          }
        }
      }
      return HashMapDataStructureType.COMPOSITE_KEYED;
    }
  }

  private boolean selectJoinForLlap(OptimizeTezProcContext context, JoinOperator joinOp,
                          TezBucketJoinProcCtx tezBucketJoinProcCtx,
                          LlapClusterStateForCompile llapInfo,
                          MapJoinConversion mapJoinConversion, int numBuckets) throws SemanticException {
    if (!context.conf.getBoolVar(HiveConf.ConfVars.HIVE_DYNAMIC_PARTITION_HASHJOIN)
            && numBuckets > 1) {
      // DPHJ is disabled, only attempt BMJ or mapjoin
      return convertJoinBucketMapJoin(joinOp, context, mapJoinConversion, tezBucketJoinProcCtx);
    }

    int numExecutorsPerNode = -1;
    if (llapInfo.hasClusterInfo()) {
      numExecutorsPerNode = llapInfo.getNumExecutorsPerNode();
    }
    if (numExecutorsPerNode == -1) {
      numExecutorsPerNode = context.conf.getIntVar(ConfVars.LLAP_DAEMON_NUM_EXECUTORS);
    }

    int numNodes = llapInfo.getKnownExecutorCount()/numExecutorsPerNode;

    LOG.debug("Number of nodes = " + numNodes + ". Number of Executors per node = " + numExecutorsPerNode);

    // Determine the size of small table inputs
    final int mapJoinConversionPos = mapJoinConversion.getBigTablePos();
    long totalSize = 0;
    for (int pos = 0; pos < joinOp.getParentOperators().size(); pos++) {
      if (pos == mapJoinConversionPos) {
        continue;
      }
      Operator<? extends OperatorDesc> parentOp = joinOp.getParentOperators().get(pos);
      totalSize += computeOnlineDataSize(parentOp.getStatistics());
    }

    // Size of bigtable
    long bigTableSize = computeOnlineDataSize(joinOp.getParentOperators().get(mapJoinConversionPos).getStatistics());

    // Network cost of DPHJ
    long networkCostDPHJ = totalSize + bigTableSize;

    LOG.info("Cost of dynamically partitioned hash join : total small table size = " + totalSize
    + " bigTableSize = " + bigTableSize + "networkCostDPHJ = " + networkCostDPHJ);

    // Network cost of map side join
    long networkCostMJ = numNodes * totalSize;
    LOG.info("Cost of Bucket Map Join : numNodes = " + numNodes + " total small table size = "
    + totalSize + " networkCostMJ = " + networkCostMJ);

    if (totalSize <= maxJoinMemory) {
      // mapjoin is applicable; don't try the below algos..
      return false;
    }

    if (networkCostDPHJ < networkCostMJ) {
      LOG.info("Dynamically partitioned Hash Join chosen");
      return convertJoinDynamicPartitionedHashJoin(joinOp, context);
    } else if (numBuckets > 1) {
      LOG.info("Bucket Map Join chosen");
      return convertJoinBucketMapJoin(joinOp, context, mapJoinConversion, tezBucketJoinProcCtx);
    }
    // fallback to mapjoin no bucket scaling
    LOG.info("Falling back to mapjoin no bucket scaling");
    return false;
  }

  public long computeOnlineDataSize(Statistics statistics) {
    if (fastHashTableAvailable) {
      return computeOnlineDataSizeFast(statistics);
    } else {
      return computeOnlineDataSizeOptimized(statistics);
    }
  }

  public long computeOnlineDataSizeFast(Statistics statistics) {
    switch (hashMapDataStructure) {
    case LONG_KEYED:
      return computeOnlineDataSizeFastLongKeyed(statistics);
    case COMPOSITE_KEYED:
      return computeOnlineDataSizeFastCompositeKeyed(statistics);
    default:
      throw new RuntimeException("invalid mode");
    }
  }

  public long computeOnlineDataSizeFastLongKeyed(Statistics statistics) {
    return computeOnlineDataSizeGeneric(statistics,
        -8, // the long key is stored in a slot
        2 * 8 // maintenance structure consists of 2 longs
    );
  }

  public long computeOnlineDataSizeFastCompositeKeyed(Statistics statistics) {
    return computeOnlineDataSizeGeneric(statistics,
        5 + 4, // list header ; value length stored as vint
        8 // maintenance structure consists of 1 long
    );
  }

  public long computeOnlineDataSizeOptimized(Statistics statistics) {
    // BytesBytesMultiHashMap
    return computeOnlineDataSizeGeneric(statistics,
        2 * 6, // 2 offsets are stored using:  LazyBinaryUtils.writeVLongToByteArray
        8 // maintenance structure consists of 1 long
    );
  }


  public long computeOnlineDataSizeGeneric(Statistics statistics, long overHeadPerRow, long overHeadPerSlot) {
    long onlineDataSize = 0;
    long numRows = statistics.getNumRows();
    if (numRows <= 0) {
      numRows = 1;
    }
    long worstCaseNeededSlots = 1L << DoubleMath.log2(numRows / hashTableLoadFactor, RoundingMode.UP);
    onlineDataSize += statistics.getDataSize() - hashTableDataSizeAdjustment(numRows, statistics.getColumnStats());
    onlineDataSize += overHeadPerRow * statistics.getNumRows();
    onlineDataSize += overHeadPerSlot * worstCaseNeededSlots;
    return onlineDataSize;
  }

  /**
   * In data calculation logic, we include some overhead due to java object refs, etc.
   * However, this overhead may be different when storing values in hashtable for mapjoin.
   * Hence, we calculate a size adjustment to the original data size for a given input.
   */
  private static long hashTableDataSizeAdjustment(long numRows, List<ColStatistics> colStats) {
    long result = 0;

    if (numRows <= 0 || colStats == null || colStats.isEmpty()) {
      return result;
    }

    for (ColStatistics cs : colStats) {
      if (cs != null) {
        String colTypeLowerCase = cs.getColumnType().toLowerCase();
        long nonNullCount = cs.getNumNulls() > 0 ? numRows - cs.getNumNulls() + 1 : numRows;
        double overhead = 0;
        if (colTypeLowerCase.equals(serdeConstants.STRING_TYPE_NAME)
            || colTypeLowerCase.startsWith(serdeConstants.VARCHAR_TYPE_NAME)
            || colTypeLowerCase.startsWith(serdeConstants.CHAR_TYPE_NAME)) {
          overhead = JavaDataModel.get().lengthForStringOfLength(0);
        } else if (colTypeLowerCase.equals(serdeConstants.BINARY_TYPE_NAME)) {
          overhead = JavaDataModel.get().lengthForByteArrayOfSize(0);
        } else if (colTypeLowerCase.equals(serdeConstants.TIMESTAMP_TYPE_NAME) ||
            colTypeLowerCase.equals(serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME) ||
            colTypeLowerCase.startsWith(serdeConstants.DECIMAL_TYPE_NAME) ||
            colTypeLowerCase.equals(serdeConstants.DATE_TYPE_NAME)) {
          overhead = JavaDataModel.get().object();
        }
        result = StatsUtils.safeAdd(StatsUtils.safeMult(nonNullCount, overhead), result);
      }
    }

    return result;
  }

  @VisibleForTesting
  public MemoryMonitorInfo getMemoryMonitorInfo(
                                                final HiveConf conf,
                                                LlapClusterStateForCompile llapInfo) {
    long maxSize = conf.getLongVar(HiveConf.ConfVars.HIVE_CONVERT_JOIN_NOCONDITIONAL_TASK_THRESHOLD);
    final double overSubscriptionFactor = conf.getFloatVar(ConfVars.LLAP_MAPJOIN_MEMORY_OVERSUBSCRIBE_FACTOR);
    final int maxSlotsPerQuery = getMaxSlotsPerQuery(conf, llapInfo);
    final long memoryCheckInterval = conf.getLongVar(ConfVars.LLAP_MAPJOIN_MEMORY_MONITOR_CHECK_INTERVAL);
    final float inflationFactor = conf.getFloatVar(ConfVars.HIVE_HASH_TABLE_INFLATION_FACTOR);
    final MemoryMonitorInfo memoryMonitorInfo;
    if (llapInfo != null) {
      final int executorsPerNode;
      if (!llapInfo.hasClusterInfo()) {
        LOG.warn("LLAP cluster information not available. Falling back to getting #executors from hiveconf..");
        executorsPerNode = conf.getIntVar(ConfVars.LLAP_DAEMON_NUM_EXECUTORS);
      } else {
        final int numExecutorsPerNodeFromCluster = llapInfo.getNumExecutorsPerNode();
        if (numExecutorsPerNodeFromCluster == -1) {
          LOG.warn("Cannot determine executor count from LLAP cluster information. Falling back to getting #executors" +
            " from hiveconf..");
          executorsPerNode = conf.getIntVar(ConfVars.LLAP_DAEMON_NUM_EXECUTORS);
        } else {
          executorsPerNode = numExecutorsPerNodeFromCluster;
        }
      }

      // bounded by max executors
      final int slotsPerQuery = Math.min(maxSlotsPerQuery, executorsPerNode);
      final long llapMaxSize = (long) (maxSize + (maxSize * overSubscriptionFactor * slotsPerQuery));
      // prevents under subscription
      final long adjustedMaxSize = Math.max(maxSize, llapMaxSize);
      memoryMonitorInfo = new MemoryMonitorInfo(true, executorsPerNode, maxSlotsPerQuery,
        overSubscriptionFactor, maxSize, adjustedMaxSize, memoryCheckInterval, inflationFactor);
    } else {
      // for non-LLAP mode most of these are not relevant. Only noConditionalTaskSize is used by shared scan optimizer.
      memoryMonitorInfo = new MemoryMonitorInfo(false, 1, maxSlotsPerQuery, overSubscriptionFactor, maxSize, maxSize,
        memoryCheckInterval, inflationFactor);
    }
    LOG.info("Memory monitor info set to : {}", memoryMonitorInfo);
    return memoryMonitorInfo;
  }

  private int getMaxSlotsPerQuery(HiveConf conf, LlapClusterStateForCompile llapInfo) {
    int maxExecutorsPerQuery = conf.getIntVar(ConfVars.LLAP_MEMORY_OVERSUBSCRIPTION_MAX_EXECUTORS_PER_QUERY);
    if (maxExecutorsPerQuery == -1) {
      if (llapInfo == null) {
        maxExecutorsPerQuery = DEFAULT_MAX_EXECUTORS_PER_QUERY_CONTAINER_MODE;
      } else {
        maxExecutorsPerQuery = Math.min(Math.max(1, llapInfo.getNumExecutorsPerNode() / 3), 8);
      }
    }
    return maxExecutorsPerQuery;
  }

  @SuppressWarnings("unchecked")
  private Object checkAndConvertSMBJoin(OptimizeTezProcContext context, JoinOperator joinOp,
    TezBucketJoinProcCtx tezBucketJoinProcCtx) throws SemanticException {
    // we cannot convert to bucket map join, we cannot convert to
    // map join either based on the size. Check if we can convert to SMB join.
    if (!(HiveConf.getBoolVar(context.conf, ConfVars.HIVE_AUTO_SORTMERGE_JOIN))
      || ((!HiveConf.getBoolVar(context.conf, ConfVars.HIVE_AUTO_SORTMERGE_JOIN_REDUCE))
          && joinOp.getOpTraits().getNumReduceSinks() >= 2)) {
      fallbackToReduceSideJoin(joinOp, context);
      return null;
    }
    Class<? extends BigTableSelectorForAutoSMJ> bigTableMatcherClass = null;
    try {
      String selector = HiveConf.getVar(context.parseContext.getConf(),
          HiveConf.ConfVars.HIVE_AUTO_SORTMERGE_JOIN_BIGTABLE_SELECTOR);
      bigTableMatcherClass =
          JavaUtils.loadClass(selector);
    } catch (ClassNotFoundException e) {
      throw new SemanticException(e.getMessage());
    }

    BigTableSelectorForAutoSMJ bigTableMatcher =
        ReflectionUtils.newInstance(bigTableMatcherClass, null);
    JoinDesc joinDesc = joinOp.getConf();
    JoinCondDesc[] joinCondns = joinDesc.getConds();
    Set<Integer> joinCandidates = MapJoinProcessor.getBigTableCandidates(joinCondns);
    if (joinCandidates.isEmpty()) {
      // This is a full outer join. This can never be a map-join
      // of any type. So return false.
      return false;
    }
    int mapJoinConversionPos =
        bigTableMatcher.getBigTablePosition(context.parseContext, joinOp, joinCandidates);
    if (mapJoinConversionPos < 0) {
      // contains aliases from sub-query
      // we are just converting to a common merge join operator. The shuffle
      // join in map-reduce case.
      fallbackToReduceSideJoin(joinOp, context);
      return null;
    }

    if (checkConvertJoinSMBJoin(joinOp, context, mapJoinConversionPos, tezBucketJoinProcCtx)) {
      convertJoinSMBJoin(joinOp, context, mapJoinConversionPos,
          tezBucketJoinProcCtx.getNumBuckets(), true);
    } else {
      // we are just converting to a common merge join operator. The shuffle
      // join in map-reduce case.
      fallbackToReduceSideJoin(joinOp, context);
    }
    return null;
  }

  // replaces the join operator with a new CommonJoinOperator, removes the
  // parent reduce sinks
  private void convertJoinSMBJoin(JoinOperator joinOp, OptimizeTezProcContext context,
      int mapJoinConversionPos, int numBuckets, boolean adjustParentsChildren)
      throws SemanticException {
    MapJoinDesc mapJoinDesc = null;
    if (adjustParentsChildren) {
      mapJoinDesc = MapJoinProcessor.getMapJoinDesc(context.conf,
            joinOp, joinOp.getConf().isLeftInputJoin(), joinOp.getConf().getBaseSrc(),
            joinOp.getConf().getMapAliases(), mapJoinConversionPos, true);
    } else {
      JoinDesc joinDesc = joinOp.getConf();
      // retain the original join desc in the map join.
      mapJoinDesc =
          new MapJoinDesc(
                  MapJoinProcessor.getKeys(joinOp.getConf().isLeftInputJoin(),
                  joinOp.getConf().getBaseSrc(), joinOp).getRight(),
                  null, joinDesc.getExprs(), null, null,
                  joinDesc.getOutputColumnNames(), mapJoinConversionPos, joinDesc.getConds(),
                  joinDesc.getFilters(), joinDesc.getNoOuterJoin(), null,
                  joinDesc.getMemoryMonitorInfo(), joinDesc.getInMemoryDataSize());
      mapJoinDesc.setNullSafes(joinDesc.getNullSafes());
      mapJoinDesc.setFilterMap(joinDesc.getFilterMap());
      mapJoinDesc.setResidualFilterExprs(joinDesc.getResidualFilterExprs());
      // keep column expression map, explain plan uses this to display
      mapJoinDesc.setColumnExprMap(joinDesc.getColumnExprMap());
      mapJoinDesc.setReversedExprs(joinDesc.getReversedExprs());
      mapJoinDesc.resetOrder();
    }

    CommonMergeJoinOperator mergeJoinOp =
        (CommonMergeJoinOperator) OperatorFactory.get(joinOp.getCompilationOpContext(),
            new CommonMergeJoinDesc(numBuckets, mapJoinConversionPos, mapJoinDesc),
            joinOp.getSchema());
    context.parseContext.getContext().getPlanMapper().link(joinOp, mergeJoinOp);
    int numReduceSinks = joinOp.getOpTraits().getNumReduceSinks();
    OpTraits opTraits = new OpTraits(joinOp.getOpTraits().getBucketColNames(), numBuckets,
        joinOp.getOpTraits().getSortCols(), numReduceSinks);
    mergeJoinOp.setOpTraits(opTraits);
    mergeJoinOp.getConf().setBucketingVersion(joinOp.getConf().getBucketingVersion());
    preserveOperatorInfos(mergeJoinOp, joinOp, context);

    for (Operator<? extends OperatorDesc> parentOp : joinOp.getParentOperators()) {
      int pos = parentOp.getChildOperators().indexOf(joinOp);
      parentOp.getChildOperators().remove(pos);
      parentOp.getChildOperators().add(pos, mergeJoinOp);
    }

    for (Operator<? extends OperatorDesc> childOp : joinOp.getChildOperators()) {
      int pos = childOp.getParentOperators().indexOf(joinOp);
      childOp.getParentOperators().remove(pos);
      childOp.getParentOperators().add(pos, mergeJoinOp);
    }

    List<Operator<? extends OperatorDesc>> childOperators = mergeJoinOp.getChildOperators();
    List<Operator<? extends OperatorDesc>> parentOperators = mergeJoinOp.getParentOperators();

    childOperators.clear();
    parentOperators.clear();
    childOperators.addAll(joinOp.getChildOperators());
    parentOperators.addAll(joinOp.getParentOperators());
    mergeJoinOp.getConf().setGenJoinKeys(false);

    if (adjustParentsChildren) {
      mergeJoinOp.getConf().setGenJoinKeys(true);
      List<Operator<? extends OperatorDesc>> newParentOpList = new ArrayList<Operator<? extends OperatorDesc>>();
      for (Operator<? extends OperatorDesc> parentOp : mergeJoinOp.getParentOperators()) {
        for (Operator<? extends OperatorDesc> grandParentOp : parentOp.getParentOperators()) {
          grandParentOp.getChildOperators().remove(parentOp);
          grandParentOp.getChildOperators().add(mergeJoinOp);
          newParentOpList.add(grandParentOp);
        }
      }
      mergeJoinOp.getParentOperators().clear();
      mergeJoinOp.getParentOperators().addAll(newParentOpList);
      List<Operator<? extends OperatorDesc>> parentOps =
          new ArrayList<Operator<? extends OperatorDesc>>(mergeJoinOp.getParentOperators());
      for (Operator<? extends OperatorDesc> parentOp : parentOps) {
        int parentIndex = mergeJoinOp.getParentOperators().indexOf(parentOp);
        if (parentIndex == mapJoinConversionPos) {
          continue;
        }

        // In case of SMB join, the parent group by has to be FINAL where it streams the
        // aggregate as and when its calculated. This is required, as the sort merge join
        // pulls the data from the parent operators and expect it to get the records
        // during join processing, not at the time of close.
        if (parentOp instanceof GroupByOperator) {
          GroupByOperator gpbyOp = (GroupByOperator )parentOp;
          if (gpbyOp.getConf().getMode() == GroupByDesc.Mode.HASH) {
            // No need to change for MERGE_PARTIAL etc.
            gpbyOp.getConf().setMode(GroupByDesc.Mode.FINAL);
          }
        }

        // insert the dummy store operator here
        DummyStoreOperator dummyStoreOp = new TezDummyStoreOperator(mergeJoinOp.getCompilationOpContext());
        dummyStoreOp.setConf(new DummyStoreDesc());
        dummyStoreOp.setParentOperators(new ArrayList<Operator<? extends OperatorDesc>>());
        dummyStoreOp.setChildOperators(new ArrayList<Operator<? extends OperatorDesc>>());
        dummyStoreOp.getChildOperators().add(mergeJoinOp);
        int index = parentOp.getChildOperators().indexOf(mergeJoinOp);
        parentOp.getChildOperators().remove(index);
        parentOp.getChildOperators().add(index, dummyStoreOp);
        dummyStoreOp.getParentOperators().add(parentOp);
        mergeJoinOp.getParentOperators().remove(parentIndex);
        mergeJoinOp.getParentOperators().add(parentIndex, dummyStoreOp);
      }
    }
    mergeJoinOp.cloneOriginalParentsList(mergeJoinOp.getParentOperators());
  }

  private void setAllChildrenTraits(Operator<? extends OperatorDesc> currentOp, OpTraits opTraits) {
    if (currentOp instanceof ReduceSinkOperator) {
      return;
    }
    currentOp.setOpTraits(new OpTraits(opTraits.getBucketColNames(),
        opTraits.getNumBuckets(), opTraits.getSortCols(), opTraits.getNumReduceSinks()));
    for (Operator<? extends OperatorDesc> childOp : currentOp.getChildOperators()) {
      if ((childOp instanceof ReduceSinkOperator) || (childOp instanceof GroupByOperator)) {
        break;
      }
      setAllChildrenTraits(childOp, opTraits);
    }
  }

  private boolean convertJoinBucketMapJoin(JoinOperator joinOp, OptimizeTezProcContext context,
      MapJoinConversion mapJoinConversion, TezBucketJoinProcCtx tezBucketJoinProcCtx) throws SemanticException {

    if (mapJoinConversion.getIsFullOuterJoin() &&
        !mapJoinConversion.getIsFullOuterEnabledForMapJoin()) {
      return false;
    }
    final int bigTablePosition = mapJoinConversion.getBigTablePos();
    if (!checkConvertJoinBucketMapJoin(joinOp, bigTablePosition, tezBucketJoinProcCtx)) {
      LOG.info("Check conversion to bucket map join failed.");
      return false;
    }

    // Incase the join has extra keys other than bucketed columns, partition keys need to be updated
    // on small table(s).
    ReduceSinkOperator bigTableRS = (ReduceSinkOperator)joinOp.getParentOperators().get(bigTablePosition);
    OpTraits opTraits = bigTableRS.getOpTraits();
    List<List<String>> listBucketCols = opTraits.getBucketColNames();
    List<ExprNodeDesc> bigTablePartitionCols = bigTableRS.getConf().getPartitionCols();
    boolean updatePartitionCols = false;
    List<Integer> positions = new ArrayList<>();

    if (listBucketCols.get(0).size() != bigTablePartitionCols.size()) {
      updatePartitionCols = true;
      // Prepare updated partition columns for small table(s).
      // Get the positions of bucketed columns

      int bigTableExprPos = 0;
      Map<String, ExprNodeDesc> colExprMap = bigTableRS.getColumnExprMap();
      for (ExprNodeDesc bigTableExpr : bigTablePartitionCols) {
        // It is guaranteed there is only 1 list within listBucketCols.
        for (String colName : listBucketCols.get(0)) {
          if (colExprMap.get(colName).isSame(bigTableExpr)) {
            positions.add(bigTableExprPos);
          }
        }
        bigTableExprPos = bigTableExprPos + 1;
      }
    }

    MapJoinOperator mapJoinOp = convertJoinMapJoin(joinOp, context, mapJoinConversion, true);
    if (mapJoinOp == null) {
      LOG.debug("Conversion to bucket map join failed.");
      return false;
    }
    MapJoinDesc joinDesc = mapJoinOp.getConf();
    joinDesc.setBucketMapJoin(true);

    // we can set the traits for this join operator
    opTraits = new OpTraits(joinOp.getOpTraits().getBucketColNames(),
        tezBucketJoinProcCtx.getNumBuckets(), null, joinOp.getOpTraits().getNumReduceSinks());
    mapJoinOp.setOpTraits(opTraits);
    preserveOperatorInfos(mapJoinOp, joinOp, context);
    setNumberOfBucketsOnChildren(mapJoinOp);

    // Once the conversion is done, we can set the partitioner to bucket cols on the small table
    Map<String, Integer> bigTableBucketNumMapping = new HashMap<String, Integer>();
    bigTableBucketNumMapping.put(joinDesc.getBigTableAlias(), tezBucketJoinProcCtx.getNumBuckets());
    joinDesc.setBigTableBucketNumMapping(bigTableBucketNumMapping);

    // Update the partition columns in small table to ensure correct routing of hash tables.
    if (updatePartitionCols) {
      // use the positions to only pick the partitionCols which are required
      // on the small table side.
      for (Operator<?> op : mapJoinOp.getParentOperators()) {
        if (!(op instanceof ReduceSinkOperator)) {
          continue;
        }

        ReduceSinkOperator rsOp = (ReduceSinkOperator) op;
        List<ExprNodeDesc> newPartitionCols = new ArrayList<>();
        List<ExprNodeDesc> partitionCols = rsOp.getConf().getPartitionCols();
        for (Integer position : positions) {
          newPartitionCols.add(partitionCols.get(position));
        }
        rsOp.getConf().setPartitionCols(newPartitionCols);
      }
    }

    // Update the memory monitor info for LLAP.
    MemoryMonitorInfo memoryMonitorInfo = joinDesc.getMemoryMonitorInfo();
    if (memoryMonitorInfo.isLlap()) {
      memoryMonitorInfo.setHashTableInflationFactor(1);
      memoryMonitorInfo.setMemoryOverSubscriptionFactor(0);
    }
    return true;
  }


  /**
   * Preserves additional information about the operator.
   *
   * When an operator is replaced by a new one; some of the information of the old have to be retained.
   */
  private void preserveOperatorInfos(Operator<?> newOp, Operator<?> oldOp, OptimizeTezProcContext context) {
    newOp.setStatistics(oldOp.getStatistics());
    // linking these two operator declares that they are representing the same thing
    // currently important because statistincs are actually gather for newOp; but the lookup is done using oldOp
    context.parseContext.getContext().getPlanMapper().link(oldOp, newOp);
  }

  /*
   * This method tries to convert a join to an SMB. This is done based on
   * traits. If the sorted by columns are the same as the join columns then, we
   * can convert the join to an SMB. Otherwise retain the bucket map join as it
   * is still more efficient than a regular join.
   */
  private boolean checkConvertJoinSMBJoin(JoinOperator joinOp, OptimizeTezProcContext context,
      int bigTablePosition, TezBucketJoinProcCtx tezBucketJoinProcCtx) throws SemanticException {

    ReduceSinkOperator bigTableRS =
        (ReduceSinkOperator) joinOp.getParentOperators().get(bigTablePosition);
    int numBuckets = bigTableRS.getParentOperators().get(0).getOpTraits().getNumBuckets();

    int size = -1;
    boolean shouldCheckExternalTables =
        context.conf.getBoolVar(HiveConf.ConfVars.HIVE_DISABLE_UNSAFE_EXTERNALTABLE_OPERATIONS);
    StringBuilder sb = new StringBuilder();
    for (Operator<?> parentOp : joinOp.getParentOperators()) {
      if (shouldCheckExternalTables && !canTableUseStats(parentOp, sb)) {
        LOG.debug("External table {} found in join and also could not provide statistics - disabling SMB join.", sb);
        return false;
      }
      // GBY operators buffers one record. These are processed when ReduceRecordSources flushes the operator tree
      // when end of record stream reached. If the tree has more than two GBY operators CommonMergeJoinOperator can
      // not process all buffered records.
      // HIVE-27788
      if (parentOp.getParentOperators() != null) {
        // Parent operator is RS and hasMoreOperatorsThan traverses until the next RS, so we start from grandparent
        for (Operator<?> grandParent : parentOp.getParentOperators()) {
          if (hasMoreOperatorsThan(grandParent, GroupByOperator.class, 1)) {
            LOG.info("We cannot convert to SMB join " +
                "because one of the join branches has more than one Group by operators in the same reducer.");
            return false;
          }
        }
      }

      // each side better have 0 or more RS. if either side is unbalanced, cannot convert.
      // This is a workaround for now. Right fix would be to refactor code in the
      // MapRecordProcessor and ReduceRecordProcessor with respect to the sources.
      Set<ReduceSinkOperator> set =
          OperatorUtils.findOperatorsUpstream(parentOp.getParentOperators(),
              ReduceSinkOperator.class);
      if (size < 0) {
        size = set.size();
        continue;
      }

      if (((size > 0) && (set.size() > 0)) || ((size == 0) && (set.size() == 0))) {
        continue;
      } else {
        return false;
      }
    }

    // the sort and bucket cols have to match on both sides for this
    // transformation of the join operation
    for (Operator<? extends OperatorDesc> parentOp : joinOp.getParentOperators()) {
      if (!(parentOp instanceof ReduceSinkOperator)) {
        // could be mux/demux operators. Currently not supported
        LOG.debug("Found correlation optimizer operators. Cannot convert to SMB at this time.");
        return false;
      }
      ReduceSinkOperator rsOp = (ReduceSinkOperator) parentOp;
      List<ExprNodeDesc> keyCols = rsOp.getConf().getKeyCols();

      // For SMB, the key column(s) in RS should be same as bucket column(s) and sort column(s)`
      List<String> sortCols = rsOp.getOpTraits().getSortCols().get(0);
      List<String> bucketCols = rsOp.getOpTraits().getBucketColNames().get(0);
      if (sortCols.size() != keyCols.size() || bucketCols.size() != keyCols.size()) {
        return false;
      }

      // Check columns.
      for (int i = 0; i < sortCols.size(); i++) {
        ExprNodeDesc sortCol = rsOp.getColumnExprMap().get(sortCols.get(i));
        ExprNodeDesc bucketCol = rsOp.getColumnExprMap().get(bucketCols.get(i));
        if (!(sortCol.isSame(keyCols.get(i)) && bucketCol.isSame(keyCols.get(i)))) {
          return false;
        }
      }
      // check Parent's traits are same as rs
      OpTraits parentTraits = rsOp.getParentOperators().get(0).getOpTraits();
      if (null == parentTraits) {
        // programming error - shouldn't be null
        return false;
      }

      if (!checkColEquality(parentTraits.getSortCols(), rsOp
          .getOpTraits().getSortCols(), rsOp.getColumnExprMap(), false)) {
        LOG.info("We cannot convert to SMB because the sort column names do not match.");
        return false;
      }

      if (!checkColEquality(parentTraits.getBucketColNames(), rsOp
          .getOpTraits().getBucketColNames(), rsOp.getColumnExprMap(), true)) {
        LOG.info("We cannot convert to SMB because bucket column names do not match.");
        return false;
      }
    }

    if (numBuckets < 0) {
      numBuckets = bigTableRS.getConf().getNumReducers();
    }
    tezBucketJoinProcCtx.setNumBuckets(numBuckets);

    // With bucketing using two different versions. Version 1 for exiting
    // tables and version 2 for new tables. All the inputs to the SMB must be
    // from same version. This only applies to tables read directly and not
    // intermediate outputs of joins/groupbys
    int bucketingVersion = -1;
    for (Operator<? extends OperatorDesc> parentOp : joinOp.getParentOperators()) {
      // Check if the parent is coming from a table scan, if so, what is the version of it.
      assert parentOp.getParentOperators() != null && parentOp.getParentOperators().size() == 1;
      Operator<?> op = parentOp;
      while (op != null && !(op instanceof TableScanOperator || op instanceof ReduceSinkOperator
          || op instanceof CommonJoinOperator)) {
        // If op has parents it is guaranteed to be 1.
        List<Operator<?>> parents = op.getParentOperators();
        Preconditions.checkState(parents.size() == 0 || parents.size() == 1);
        op = parents.size() == 1 ? parents.get(0) : null;
      }

      if (op instanceof TableScanOperator) {
        int localVersion = ((TableScanOperator) op).getConf().getTableMetadata().getBucketingVersion();
        if (bucketingVersion == -1) {
          bucketingVersion = localVersion;
        } else if (bucketingVersion != localVersion) {
          // versions dont match, return false.
          LOG.debug("SMB Join can't be performed due to bucketing version mismatch");
          return false;
        }
      }
    }

    LOG.info("We can convert the join to an SMB join.");
    return true;
  }

  private void setNumberOfBucketsOnChildren(Operator<? extends OperatorDesc> currentOp) {
    int numBuckets = currentOp.getOpTraits().getNumBuckets();
    for (Operator<? extends OperatorDesc>op : currentOp.getChildOperators()) {
      if (!(op instanceof ReduceSinkOperator) && !(op instanceof GroupByOperator)) {
        op.getOpTraits().setNumBuckets(numBuckets);
        setNumberOfBucketsOnChildren(op);
      }
    }
  }

  /*
   * If the parent reduce sink of the big table side has the same emit key cols as its parent, we
   * can create a bucket map join eliminating the reduce sink.
   */
  private boolean checkConvertJoinBucketMapJoin(JoinOperator joinOp,
      int bigTablePosition, TezBucketJoinProcCtx tezBucketJoinProcCtx)
          throws SemanticException {
    // bail on mux-operator because mux operator masks the emit keys of the
    // constituent reduce sinks
    if (!(joinOp.getParentOperators().get(0) instanceof ReduceSinkOperator)) {
      LOG.info("Operator is " + joinOp.getParentOperators().get(0).getName() +
          ". Cannot convert to bucket map join");
      return false;
    }

    ReduceSinkOperator rs = (ReduceSinkOperator) joinOp.getParentOperators().get(bigTablePosition);
    List<List<String>> parentColNames = rs.getOpTraits().getBucketColNames();
    Operator<? extends OperatorDesc> parentOfParent = rs.getParentOperators().get(0);
    List<List<String>> grandParentColNames = parentOfParent.getOpTraits().getBucketColNames();
    int numBuckets = parentOfParent.getOpTraits().getNumBuckets();
    // all keys matched.
    if (!checkColEquality(grandParentColNames, parentColNames, rs.getColumnExprMap(), true)) {
      LOG.info("No info available to check for bucket map join. Cannot convert");
      return false;
    }

    boolean shouldCheckExternalTables = tezBucketJoinProcCtx.getConf()
        .getBoolVar(HiveConf.ConfVars.HIVE_DISABLE_UNSAFE_EXTERNALTABLE_OPERATIONS);
    if (shouldCheckExternalTables) {
      StringBuilder sb = new StringBuilder();
      for (Operator<?> parentOp : joinOp.getParentOperators()) {
        if (!canTableUseStats(parentOp, sb)) {
          LOG.debug("External table {} found in join and also could not provide statistics - " +
              "disabling bucket map join.", sb);
          return false;
        }
      }
    }

    /*
     * this is the case when the big table is a sub-query and is probably already bucketed by the
     * join column in say a group by operation
     */
    if (numBuckets < 0) {
      numBuckets = rs.getConf().getNumReducers();
    }
    tezBucketJoinProcCtx.setNumBuckets(numBuckets);
    return true;
  }

  private boolean checkColEquality(List<List<String>> grandParentColNames,
      List<List<String>> parentColNames, Map<String, ExprNodeDesc> colExprMap,
      boolean strict) {

    if ((grandParentColNames == null) || (parentColNames == null)) {
      return false;
    }

    if (!parentColNames.isEmpty()) {
      for (List<String> listBucketCols : grandParentColNames) {
        // can happen if this operator does not carry forward the previous bucketing columns
        // for e.g. another join operator which does not carry one of the sides' key columns
        if (listBucketCols.isEmpty()) {
          continue;
        }
        int colCount = 0;
        // parent op is guaranteed to have a single list because it is a reduce sink
        for (String colName : parentColNames.get(0)) {
          if (listBucketCols.size() <= colCount) {
            // can happen with virtual columns. RS would add the column to its output columns
            // but it would not exist in the grandparent output columns or exprMap.
            return false;
          }
          // all columns need to be at least a subset of the parentOfParent's bucket cols
          ExprNodeDesc exprNodeDesc = colExprMap.get(colName);
          if (exprNodeDesc instanceof ExprNodeColumnDesc) {
            if (((ExprNodeColumnDesc) exprNodeDesc).getColumn()
                .equals(listBucketCols.get(colCount))) {
              colCount++;
            } else {
              break;
            }
          }

          if (colCount == parentColNames.get(0).size()) {
            return !strict || (colCount == listBucketCols.size());
          }
        }
      }
      return false;
    }
    return false;
  }

  private boolean hasOuterJoin(JoinOperator joinOp) throws SemanticException {
    boolean hasOuter = false;
    for (JoinCondDesc joinCondDesc : joinOp.getConf().getConds()) {
      switch (joinCondDesc.getType()) {
        case JoinDesc.INNER_JOIN:
        case JoinDesc.LEFT_SEMI_JOIN:
        case JoinDesc.ANTI_JOIN:
        case JoinDesc.UNIQUE_JOIN:
          hasOuter = false;
          break;

        case JoinDesc.FULL_OUTER_JOIN:
        case JoinDesc.LEFT_OUTER_JOIN:
        case JoinDesc.RIGHT_OUTER_JOIN:
          hasOuter = true;
          break;

        default:
          throw new SemanticException("Unknown join type " + joinCondDesc.getType());
      }
    }
    return hasOuter;
  }

  private boolean isCrossProduct(JoinOperator joinOp) {
    ExprNodeDesc[][] joinExprs = joinOp.getConf().getJoinKeys();
    if (joinExprs != null) {
      for (ExprNodeDesc[] expr : joinExprs) {
        if (expr != null && expr.length != 0) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Return result for getMapJoinConversion method.
   */
  public static class MapJoinConversion {

    private final int bigTablePos;

    private final boolean isFullOuterJoin;
    private final boolean isFullOuterEnabledForDynamicPartitionHashJoin;
    private final boolean isFullOuterEnabledForMapJoin;

    public MapJoinConversion(int bigTablePos, boolean isFullOuterJoin,
        boolean isFullOuterEnabledForDynamicPartitionHashJoin, boolean isFullOuterEnabledForMapJoin) {
      this.bigTablePos = bigTablePos;

      this.isFullOuterJoin = isFullOuterJoin;
      this.isFullOuterEnabledForDynamicPartitionHashJoin = isFullOuterEnabledForDynamicPartitionHashJoin;
      this.isFullOuterEnabledForMapJoin = isFullOuterEnabledForMapJoin;
    }

    public int getBigTablePos() {
      return bigTablePos;
    }

    /*
     * Do we have a single FULL OUTER JOIN here?
     */
    public boolean getIsFullOuterJoin() {
      return isFullOuterJoin;
    }

    public boolean getIsFullOuterEnabledForDynamicPartitionHashJoin() {
      return isFullOuterEnabledForDynamicPartitionHashJoin;
    }

    public boolean getIsFullOuterEnabledForMapJoin() {
      return isFullOuterEnabledForMapJoin;
    }
  }

  /**
   * Obtain big table position for join.
   *
   * @param joinOp join operator
   * @param context optimization context
   * @param buckets bucket count for Bucket Map Join conversion consideration or reduce count
   * for Dynamic Hash Join conversion consideration
   * @param skipJoinTypeChecks whether to skip join type checking
   * @param maxSize size threshold for Map Join conversion
   * @param checkMapJoinThresholds whether to check thresholds to convert to Map Join
   * @return returns big table position or -1 if it cannot be determined
   * @throws SemanticException
   */
  public MapJoinConversion getMapJoinConversion(JoinOperator joinOp, OptimizeTezProcContext context,
      int buckets, boolean skipJoinTypeChecks, long maxSize, boolean checkMapJoinThresholds)
              throws SemanticException {
    JoinDesc joinDesc = joinOp.getConf();
    JoinCondDesc[] conds = joinDesc.getConds();
    if (!skipJoinTypeChecks) {
      /*
       * HIVE-9038: Join tests fail in tez when we have more than 1 join on the same key and there is
       * an outer join down the join tree that requires filterTag. We disable this conversion to map
       * join here now. We need to emulate the behavior of HashTableSinkOperator as in MR or create a
       * new operation to be able to support this. This seems like a corner case enough to special
       * case this for now.
       */
      if (conds.length > 1) {
        if (hasOuterJoin(joinOp)) {
          return null;
        }
      }
    }

    // Assume.
    boolean isFullOuterEnabledForDynamicPartitionHashJoin = false;
    boolean isFullOuterEnabledForMapJoin = false;

    boolean isFullOuterJoin =
        MapJoinProcessor.precheckFullOuter(context.conf, joinOp);
    if (isFullOuterJoin) {

      boolean isFullOuterEnabled = MapJoinProcessor.isFullOuterMapEnabled(context.conf, joinOp);
      if (isFullOuterEnabled) {

        // FUTURE: Currently, we only support DPHJ.
        isFullOuterEnabledForDynamicPartitionHashJoin =
            MapJoinProcessor.isFullOuterEnabledForDynamicPartitionHashJoin(context.conf, joinOp);
      }
    }

    Set<Integer> bigTableCandidateSet =
        MapJoinProcessor.getBigTableCandidates(conds, /* isSupportFullOuter */ true);

    if (bigTableCandidateSet.isEmpty()) {
      return null;
    }

    int bigTablePosition = -1;
    // big input cumulative row count
    long bigInputCumulativeCardinality = -1L;
    // stats of the big input
    Statistics bigInputStat = null;

    // bigTableFound means we've encountered a table that's bigger than the
    // max. This table is either the the big table or we cannot convert.
    boolean foundInputNotFittingInMemory = false;

    // total size of the inputs
    long totalSize = 0;

    // convert to DPHJ
    boolean convertDPHJ = false;

    for (int pos = 0; pos < joinOp.getParentOperators().size(); pos++) {
      Operator<? extends OperatorDesc> parentOp = joinOp.getParentOperators().get(pos);

      Statistics currInputStat = parentOp.getStatistics();
      if (currInputStat == null) {
        LOG.warn("Couldn't get statistics from: " + parentOp);
        return null;
      }

      long inputSize = computeOnlineDataSize(currInputStat);
      LOG.info("Join input#{}; onlineDataSize: {}; Statistics: {}", pos, inputSize, currInputStat);

      boolean currentInputNotFittingInMemory = false;
      if ((bigInputStat == null)
          || (inputSize > computeOnlineDataSize(bigInputStat))) {

        if (foundInputNotFittingInMemory) {
          // cannot convert to map join; we've already chosen a big table
          // on size and there's another one that's bigger.
          return null;
        }

        if (inputSize/buckets > maxSize) {
          if (!bigTableCandidateSet.contains(pos)) {
            // can't use the current table as the big table, but it's too
            // big for the map side.
            return null;
          }

          currentInputNotFittingInMemory = true;
          foundInputNotFittingInMemory = true;
        }
      }

      long currentInputCumulativeCardinality;
      if (foundInputNotFittingInMemory) {
        currentInputCumulativeCardinality = -1L;
      } else {
        Long cardinality = computeCumulativeCardinality(parentOp);
        if (cardinality == null) {
          // We could not get stats, we cannot convert
          return null;
        }
        currentInputCumulativeCardinality = cardinality;
      }

      // This input is the big table if it is contained in the big candidates set, and either:
      // 1) we have not chosen a big table yet, or
      // 2) it has been chosen as the big table above, or
      // 3) the cumulative cardinality for this input is higher, or
      // 4) the cumulative cardinality is equal, but the size is bigger,
      boolean selectedBigTable = bigTableCandidateSet.contains(pos) &&
              (bigInputStat == null || currentInputNotFittingInMemory ||
                      (!foundInputNotFittingInMemory && (currentInputCumulativeCardinality > bigInputCumulativeCardinality ||
                  (currentInputCumulativeCardinality == bigInputCumulativeCardinality
                      && inputSize > computeOnlineDataSize(bigInputStat)))));

      if (bigInputStat != null && selectedBigTable) {
        // We are replacing the current big table with a new one, thus
        // we need to count the current one as a map table then.
        totalSize += computeOnlineDataSize(bigInputStat);
        // Check if number of distinct keys is greater than given max number of entries
        // for HashMap
        if (checkMapJoinThresholds && !checkNumberOfEntriesForHashTable(joinOp, bigTablePosition, context)) {
          convertDPHJ = true;
        }
      } else if (!selectedBigTable) {
        // This is not the first table and we are not using it as big table,
        // in fact, we're adding this table as a map table
        totalSize += inputSize;
        // Check if number of distinct keys is greater than given max number of entries
        // for HashMap
        if (checkMapJoinThresholds && !checkNumberOfEntriesForHashTable(joinOp, pos, context)) {
          convertDPHJ = true;
        }
      }

      if (totalSize/buckets > maxSize) {
        // sum of small tables size in this join exceeds configured limit
        // hence cannot convert.
        return null;
      }

      if (selectedBigTable) {
        bigTablePosition = pos;
        bigInputCumulativeCardinality = currentInputCumulativeCardinality;
        bigInputStat = currInputStat;
      }

    }

    if (bigTablePosition == -1) {
      LOG.debug("No big table selected, no MapJoin");
      return null;
    }

    // Check if size of data to shuffle (larger table) is less than given max size
    if (checkMapJoinThresholds && convertDPHJ
            && checkShuffleSizeForLargeTable(joinOp, bigTablePosition, context)) {
      LOG.debug("Conditions to convert to MapJoin are not met");
      return null;
    }

    // only allow cross product in map joins if build side is 'small'
    boolean cartesianProductEdgeEnabled =
      HiveConf.getBoolVar(context.conf, HiveConf.ConfVars.TEZ_CARTESIAN_PRODUCT_EDGE_ENABLED);
    if (cartesianProductEdgeEnabled && !hasOuterJoin(joinOp) && isCrossProduct(joinOp)) {
      for (int i = 0 ; i < joinOp.getParentOperators().size(); i ++) {
        if (i != bigTablePosition) {
          Statistics parentStats = joinOp.getParentOperators().get(i).getStatistics();
          if (parentStats.getNumRows() >
            HiveConf.getIntVar(context.conf, HiveConf.ConfVars.XPROD_SMALL_TABLE_ROWS_THRESHOLD)) {
            // if any of smaller side is estimated to generate more than
            // threshold rows we would disable mapjoin
            return null;
          }
        }
      }
    }

    // We store the total memory that this MapJoin is going to use,
    // which is calculated as totalSize/buckets, with totalSize
    // equal to sum of small tables size.
    joinOp.getConf().setInMemoryDataSize(totalSize / buckets);

    return
        new MapJoinConversion(
            bigTablePosition,
            isFullOuterJoin,
            isFullOuterEnabledForDynamicPartitionHashJoin,
            isFullOuterEnabledForMapJoin);
  }

  // This is akin to CBO cumulative cardinality model
  private static Long computeCumulativeCardinality(Operator<? extends OperatorDesc> op) {
    long cumulativeCardinality = 0L;
    if (op instanceof CommonJoinOperator) {
      // Choose max
      for (Operator<? extends OperatorDesc> inputOp : op.getParentOperators()) {
        Long inputCardinality = computeCumulativeCardinality(inputOp);
        if (inputCardinality == null) {
          return null;
        }
        if (inputCardinality > cumulativeCardinality) {
          cumulativeCardinality = inputCardinality;
        }
      }
    } else {
      // Choose cumulative
      for (Operator<? extends OperatorDesc> inputOp : op.getParentOperators()) {
        Long inputCardinality = computeCumulativeCardinality(inputOp);
        if (inputCardinality == null) {
          return null;
        }
        cumulativeCardinality += inputCardinality;
      }
    }
    Statistics currInputStat = op.getStatistics();
    if (currInputStat == null) {
      LOG.warn("Couldn't get statistics from: " + op);
      return null;
    }
    cumulativeCardinality += currInputStat.getNumRows();
    return cumulativeCardinality;
  }

  /*
   * Once we have decided on the map join, the tree would transform from
   *
   *        |                   |
   *       Join               MapJoin
   *       / \                /   \
   *     RS   RS   --->     RS    TS (big table)
   *    /      \           /
   *   TS       TS        TS (small table)
   *
   * for tez.
   */
  public MapJoinOperator convertJoinMapJoin(JoinOperator joinOp, OptimizeTezProcContext context,
      MapJoinConversion mapJoinConversion, boolean removeReduceSink) throws SemanticException {

    // bail on mux operator because currently the mux operator masks the emit keys
    // of the constituent reduce sinks.
    for (Operator<? extends OperatorDesc> parentOp : joinOp.getParentOperators()) {
      if (parentOp instanceof MuxOperator) {
        return null;
      }
    }

    // can safely convert the join to a map join.
    final int bigTablePosition = mapJoinConversion.getBigTablePos();
    MapJoinOperator mapJoinOp =
        MapJoinProcessor.convertJoinOpMapJoinOp(context.conf, joinOp,
            joinOp.getConf().isLeftInputJoin(), joinOp.getConf().getBaseSrc(),
            joinOp.getConf().getMapAliases(), bigTablePosition, true, removeReduceSink);
    if (mapJoinOp == null) {
      return null;
    }
    MapJoinDesc mapJoinDesc = mapJoinOp.getConf();
    mapJoinDesc.setHybridHashJoin(HiveConf.getBoolVar(context.conf,
        HiveConf.ConfVars.HIVE_USE_HYBRIDGRACE_HASHJOIN));
    List<ExprNodeDesc> joinExprs = mapJoinDesc.getKeys().values().iterator().next();
    if (joinExprs.size() == 0) {  // In case of cross join, we disable hybrid grace hash join
      mapJoinDesc.setHybridHashJoin(false);
    }

    Operator<? extends OperatorDesc> parentBigTableOp =
        mapJoinOp.getParentOperators().get(bigTablePosition);
    if (parentBigTableOp instanceof ReduceSinkOperator) {
      Operator<?> parentSelectOpOfBigTableOp = parentBigTableOp.getParentOperators().get(0);
      if (removeReduceSink) {
        for (Operator<?> p : parentBigTableOp.getParentOperators()) {
          // we might have generated a dynamic partition operator chain. Since
          // we're removing the reduce sink we need do remove that too.
          Set<Operator<?>> dynamicPartitionOperators = new HashSet<Operator<?>>();
          Map<Operator<?>, AppMasterEventOperator> opEventPairs = new HashMap<>();
          for (Operator<?> c : p.getChildOperators()) {
            AppMasterEventOperator event = findDynamicPartitionBroadcast(c);
            if (event != null) {
              dynamicPartitionOperators.add(c);
              opEventPairs.put(c, event);
            }
          }
          for (Operator<?> c : dynamicPartitionOperators) {
            if (context.pruningOpsRemovedByPriorOpt.isEmpty() ||
                !context.pruningOpsRemovedByPriorOpt.contains(opEventPairs.get(c))) {
              p.removeChild(c);
              // at this point we've found the fork in the op pipeline that has the pruning as a child plan.
              LOG.info("Disabling dynamic pruning for: "
                  + ((DynamicPruningEventDesc) opEventPairs.get(c).getConf()).getTableScan().getName()
                  + ". Need to be removed together with reduce sink");
            }
          }
          for (Operator<?> op : dynamicPartitionOperators) {
            context.pruningOpsRemovedByPriorOpt.add(opEventPairs.get(op));
          }
        }

        mapJoinOp.getParentOperators().remove(bigTablePosition);
        if (!(mapJoinOp.getParentOperators().contains(parentBigTableOp.getParentOperators().get(0)))) {
          mapJoinOp.getParentOperators().add(bigTablePosition,
              parentBigTableOp.getParentOperators().get(0));
        }
        parentBigTableOp.getParentOperators().get(0).removeChild(parentBigTableOp);
      }

      for (Operator<? extends OperatorDesc>op : mapJoinOp.getParentOperators()) {
        if (!(op.getChildOperators().contains(mapJoinOp))) {
          op.getChildOperators().add(mapJoinOp);
        }
        op.getChildOperators().remove(joinOp);
      }

      // Remove semijoin Op if there is any.
      // The semijoin branch can potentially create a task level cycle
      // with the hashjoin except when it is dynamically partitioned hash
      // join which takes place in a separate task.
      if (context.parseContext.getRsToSemiJoinBranchInfo().size() > 0
              && removeReduceSink) {
        removeCycleCreatingSemiJoinOps(mapJoinOp, parentSelectOpOfBigTableOp,
                context.parseContext);
      }
    }

    return mapJoinOp;
  }

  // Remove any semijoin branch associated with hashjoin's parent's operator
  // pipeline which can cause a cycle after hashjoin optimization.
  private void removeCycleCreatingSemiJoinOps(MapJoinOperator mapjoinOp,
                                              Operator<?> parentSelectOpOfBigTable,
                                              ParseContext parseContext) throws SemanticException {
    Map<ReduceSinkOperator, TableScanOperator> semiJoinMap =
            new HashMap<ReduceSinkOperator, TableScanOperator>();
    for (Operator<?> op : parentSelectOpOfBigTable.getChildOperators()) {
      if (!(op instanceof SelectOperator)) {
        continue;
      }

      while (op.getChildOperators().size() > 0) {
        op = op.getChildOperators().get(0);
      }

      // If not ReduceSink Op, skip
      if (!(op instanceof ReduceSinkOperator)) {
        continue;
      }

      ReduceSinkOperator rs = (ReduceSinkOperator) op;
      TableScanOperator ts = parseContext.getRsToSemiJoinBranchInfo().get(rs).getTsOp();
      if (ts == null) {
        // skip, no semijoin branch
        continue;
      }

      // Found a semijoin branch.
      // There can be more than one semijoin branch coming from the parent
      // GBY Operator of the RS Operator.
      Operator<?> parentGB = op.getParentOperators().get(0);
      for (Operator<?> childRS : parentGB.getChildOperators()) {
        // Get the RS and TS for this branch
        rs = (ReduceSinkOperator) childRS;
        ts = parseContext.getRsToSemiJoinBranchInfo().get(rs).getTsOp();
        assert ts != null;
        for (Operator<?> parent : mapjoinOp.getParentOperators()) {
          if (!(parent instanceof ReduceSinkOperator)) {
            continue;
          }

          Set<TableScanOperator> tsOps = OperatorUtils.findOperatorsUpstream(parent,
                  TableScanOperator.class);
          boolean found = false;
          for (TableScanOperator parentTS : tsOps) {
            // If the parent is same as the ts, then we have a cycle.
            if (ts == parentTS) {
              semiJoinMap.put(rs, ts);
              found = true;
              break;
            }
          }
          if (found) {
            break;
          }
        }
      }
    }
    if (semiJoinMap.size() > 0) {
      for (ReduceSinkOperator rs : semiJoinMap.keySet()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found semijoin optimization from the big table side of a map join, which will cause a task cycle. "
              + "Removing semijoin "
              + OperatorUtils.getOpNamePretty(rs) + " - " + OperatorUtils.getOpNamePretty(semiJoinMap.get(rs)));
        }
        GenTezUtils.removeBranch(rs);
        GenTezUtils.removeSemiJoinOperator(parseContext, rs,
                semiJoinMap.get(rs));
      }
    }
  }

  private AppMasterEventOperator findDynamicPartitionBroadcast(Operator<?> parent) {

    for (Operator<?> op : parent.getChildOperators()) {
      while (op != null) {
        if (op instanceof AppMasterEventOperator && op.getConf() instanceof DynamicPruningEventDesc) {
          // found dynamic partition pruning operator
          return (AppMasterEventOperator)op;
        }
        if (op instanceof ReduceSinkOperator || op instanceof FileSinkOperator) {
          // crossing reduce sink or file sink means the pruning isn't for this parent.
          break;
        }

        if (op.getChildOperators().size() != 1) {
          // dynamic partition pruning pipeline doesn't have multiple children
          break;
        }

        op = op.getChildOperators().get(0);
      }
    }

    return null;
  }

  /**
   * Estimate the number of buckets in the join, using the parent operators' OpTraits and/or
   * parent operators' number of reducers
   * @param joinOp
   * @param useOpTraits  Whether OpTraits should be used for the estimate.
   * @return
   */
  private static int estimateNumBuckets(JoinOperator joinOp, boolean useOpTraits) {
    int numBuckets = -1;
    int estimatedBuckets = -1;

    for (Operator<? extends OperatorDesc>parentOp : joinOp.getParentOperators()) {
      if (parentOp.getOpTraits().getNumBuckets() > 0) {
        numBuckets = (numBuckets < parentOp.getOpTraits().getNumBuckets()) ?
            parentOp.getOpTraits().getNumBuckets() : numBuckets;
      }

      if (!useOpTraits && parentOp instanceof ReduceSinkOperator) {
        ReduceSinkOperator rs = (ReduceSinkOperator) parentOp;
        estimatedBuckets = (estimatedBuckets < rs.getConf().getNumReducers()) ?
            rs.getConf().getNumReducers() : estimatedBuckets;
      }
    }

    if (!useOpTraits) {
      // Ignore the value we got from OpTraits.
      // The logic below will fall back to the estimate from numReducers
      numBuckets = -1;
    }

    if (numBuckets <= 0) {
      numBuckets = estimatedBuckets;
      if (numBuckets <= 0) {
        numBuckets = 1;
      }
    }

    return numBuckets;
  }

  private boolean convertJoinDynamicPartitionedHashJoin(JoinOperator joinOp, OptimizeTezProcContext context)
    throws SemanticException {
    // Attempt dynamic partitioned hash join
    // Since we don't have big table index yet, must start with estimate of numReducers
    int numReducers = estimateNumBuckets(joinOp, false);
    LOG.info("Try dynamic partitioned hash join with estimated " + numReducers + " reducers");
    MapJoinConversion mapJoinConversion =
        getMapJoinConversion(joinOp, context, numReducers, false, maxJoinMemory, false);
    if (mapJoinConversion != null) {
      if (mapJoinConversion.getIsFullOuterJoin() &&
          !mapJoinConversion.getIsFullOuterEnabledForDynamicPartitionHashJoin()) {
        return false;
      }
      final int bigTablePos = mapJoinConversion.getBigTablePos();

      // Now that we have the big table index, get real numReducers value based on big table RS
      ReduceSinkOperator bigTableParentRS =
          (ReduceSinkOperator) (joinOp.getParentOperators().get(bigTablePos));
      numReducers = bigTableParentRS.getConf().getNumReducers();
      LOG.debug("Real big table reducers = " + numReducers);

      MapJoinOperator mapJoinOp = convertJoinMapJoin(joinOp, context, mapJoinConversion, false);
      if (mapJoinOp != null) {
        LOG.info("Selected dynamic partitioned hash join");
        MapJoinDesc mapJoinDesc = mapJoinOp.getConf();
        mapJoinDesc.setDynamicPartitionHashJoin(true);

        // Set OpTraits for dynamically partitioned hash join:
        // bucketColNames: Re-use previous joinOp's bucketColNames. Parent operators should be
        //   reduce sink, which should have bucket columns based on the join keys.
        // numBuckets: set to number of reducers
        // sortCols: This is an unsorted join - no sort cols
        OpTraits opTraits = new OpTraits(
            joinOp.getOpTraits().getBucketColNames(),
            numReducers,
            null,
            joinOp.getOpTraits().getNumReduceSinks());
        mapJoinOp.setOpTraits(opTraits);
        preserveOperatorInfos(mapJoinOp, joinOp, context);
        // propagate this change till the next RS
        for (Operator<? extends OperatorDesc> childOp : mapJoinOp.getChildOperators()) {
          setAllChildrenTraits(childOp, mapJoinOp.getOpTraits());
        }
        return true;
      }
    }

    return false;
  }

  private void fallbackToReduceSideJoin(JoinOperator joinOp, OptimizeTezProcContext context)
      throws SemanticException {
    if (context.conf.getBoolVar(HiveConf.ConfVars.HIVE_CONVERT_JOIN) &&
        context.conf.getBoolVar(HiveConf.ConfVars.HIVE_DYNAMIC_PARTITION_HASHJOIN)) {
      if (convertJoinDynamicPartitionedHashJoin(joinOp, context)) {
        return;
      }
    }

    // we are just converting to a common merge join operator. The shuffle
    // join in map-reduce case.
    fallbackToMergeJoin(joinOp, context);
  }

  private void fallbackToMergeJoin(JoinOperator joinOp, OptimizeTezProcContext context)
      throws SemanticException {
    MapJoinConversion mapJoinConversion =
        getMapJoinConversion(
            joinOp, context, estimateNumBuckets(joinOp, false), true, Long.MAX_VALUE, false);
    final int pos;
    if (mapJoinConversion == null || mapJoinConversion.getBigTablePos() == -1) {
      LOG.info("Could not get a valid join position. Defaulting to position 0");
      pos = 0;
    } else {
      pos = mapJoinConversion.getBigTablePos();
    }
    LOG.info("Fallback to common merge join operator");
    convertJoinSMBJoin(joinOp, context, pos, 0, false);
  }

  /* Returns true if it passes the test, false otherwise. */
  private boolean checkNumberOfEntriesForHashTable(JoinOperator joinOp, int position,
          OptimizeTezProcContext context) {
    long max = HiveConf.getLongVar(context.parseContext.getConf(),
            HiveConf.ConfVars.HIVE_CONVERT_JOIN_MAX_ENTRIES_HASHTABLE);
    if (max < 1) {
      // Max is disabled, we can safely return true
      return true;
    }
    // Calculate number of different entries and evaluate
    ReduceSinkOperator rsOp = (ReduceSinkOperator) joinOp.getParentOperators().get(position);
    List<String> keys = StatsUtils.getQualifedReducerKeyNames(rsOp.getConf().getOutputKeyColumnNames());
    Statistics inputStats = rsOp.getStatistics();
    List<ColStatistics> columnStats = new ArrayList<>();
    for (String key : keys) {
      ColStatistics cs = inputStats.getColumnStatisticsFromColName(key);
      if (cs == null) {
        return true;
      }
      columnStats.add(cs);
    }
    long numRows = inputStats.getNumRows();
    long estimation = estimateNDV(numRows, columnStats);
    LOG.debug("Estimated NDV for input {}: {}; Max NDV for MapJoin conversion: {}",
            position, estimation, max);
    if (estimation > max) {
      // Estimation larger than max
      LOG.debug("Number of different entries for HashTable is greater than the max; "
          + "we do not convert to MapJoin");
      return false;
    }
    // We can proceed with the conversion
    return true;
  }

  /* Returns true if it passes the test, false otherwise. */
  private boolean checkShuffleSizeForLargeTable(JoinOperator joinOp, int position,
          OptimizeTezProcContext context) {
    long max = HiveConf.getLongVar(context.parseContext.getConf(),
            HiveConf.ConfVars.HIVE_CONVERT_JOIN_MAX_SHUFFLE_SIZE);
    if (max < 1) {
      // Max is disabled, we can safely return false
      return false;
    }
    // Evaluate
    ReduceSinkOperator rsOp = (ReduceSinkOperator) joinOp.getParentOperators().get(position);
    Statistics inputStats = rsOp.getStatistics();
    long inputSize = computeOnlineDataSize(inputStats);
    LOG.debug("Estimated size for input {}: {}; Max size for DPHJ conversion: {}",
        position, inputSize, max);
    if (inputSize > max) {
      LOG.debug("Size of input is greater than the max; "
          + "we do not convert to DPHJ");
      return false;
    }
    return true;
  }

  private static long estimateNDV(long numRows, List<ColStatistics> columnStats) {
    // If there is a single column, return the number of distinct values
    if (columnStats.size() == 1) {
      return columnStats.get(0).getCountDistint();
    }

    // The expected number of distinct values when choosing p values
    // with replacement from n integers is n . (1 - ((n - 1) / n) ^ p).
    //
    // If we have several uniformly distributed attributes A1 ... Am
    // with N1 ... Nm distinct values, they behave as one uniformly
    // distributed attribute with N1 * ... * Nm distinct values.
    long n = 1L;
    for (ColStatistics cs : columnStats) {
      final long ndv = cs.getCountDistint();
      if (ndv > 1) {
        n = StatsUtils.safeMult(n, ndv);
      }
    }
    final double nn = n;
    final double a = (nn - 1d) / nn;
    if (a == 1d) {
      // A under-flows if nn is large.
      return numRows;
    }
    final double v = nn * (1d - Math.pow(a, numRows));
    // Cap at fact-row-count, because numerical artifacts can cause it
    // to go a few % over.
    return Math.min(Math.round(v), numRows);
  }

  private static boolean canTableUseStats(Operator op, StringBuilder sb) {
    Operator ancestor = OperatorUtils.findSingleOperatorUpstream(op, TableScanOperator.class);
    if (ancestor != null) {
      TableScanOperator ts = (TableScanOperator) ancestor;
      Boolean canUseStats = StatsUtils.checkCanProvideStats(new Table(ts.getConf().getTableMetadata().getTTable()));
      if (!canUseStats) {
        sb.append(ts.getConf().getTableMetadata().getFullyQualifiedName());
        return false;
      }
    }
    return true;
  }
}
