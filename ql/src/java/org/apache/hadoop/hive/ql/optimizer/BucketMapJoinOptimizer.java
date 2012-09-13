/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 *this transformation does bucket map join optimization.
 */
public class BucketMapJoinOptimizer implements Transform {

  private static final Log LOG = LogFactory.getLog(GroupByOptimizer.class
      .getName());

  public BucketMapJoinOptimizer() {
  }

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    BucketMapjoinOptProcCtx bucketMapJoinOptimizeCtx =
      new BucketMapjoinOptProcCtx(pctx.getConf());

    // process map joins with no reducers pattern
    opRules.put(new RuleRegExp("R1",
      MapJoinOperator.getOperatorName() + "%"),
      getBucketMapjoinProc(pctx));
    opRules.put(new RuleRegExp("R2",
      ReduceSinkOperator.getOperatorName() + "%.*" + MapJoinOperator.getOperatorName()),
      getBucketMapjoinRejectProc(pctx));
    opRules.put(new RuleRegExp(new String("R3"),
      UnionOperator.getOperatorName() + "%.*" + MapJoinOperator.getOperatorName() + "%"),
      getBucketMapjoinRejectProc(pctx));
    opRules.put(new RuleRegExp(new String("R4"),
      MapJoinOperator.getOperatorName() + "%.*" + MapJoinOperator.getOperatorName() + "%"),
      getBucketMapjoinRejectProc(pctx));

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules,
        bucketMapJoinOptimizeCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pctx;
  }

  private NodeProcessor getBucketMapjoinRejectProc(ParseContext pctx) {
    return new NodeProcessor () {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs)
          throws SemanticException {
        MapJoinOperator mapJoinOp = (MapJoinOperator) nd;
        BucketMapjoinOptProcCtx context = (BucketMapjoinOptProcCtx) procCtx;
        context.listOfRejectedMapjoins.add(mapJoinOp);
        return null;
      }
    };
  }

  private NodeProcessor getBucketMapjoinProc(ParseContext pctx) {
    return new BucketMapjoinOptProc(pctx);
  }

  private NodeProcessor getDefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs)
          throws SemanticException {
        return null;
      }
    };
  }

  class BucketMapjoinOptProc implements NodeProcessor {

    protected ParseContext pGraphContext;

    public BucketMapjoinOptProc(ParseContext pGraphContext) {
      super();
      this.pGraphContext = pGraphContext;
    }

    private boolean convertBucketMapJoin(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      MapJoinOperator mapJoinOp = (MapJoinOperator) nd;
      BucketMapjoinOptProcCtx context = (BucketMapjoinOptProcCtx) procCtx;
      HiveConf conf = context.getConf();

      if(context.getListOfRejectedMapjoins().contains(mapJoinOp)) {
        return false;
      }

      QBJoinTree joinCxt = this.pGraphContext.getMapJoinContext().get(mapJoinOp);
      if(joinCxt == null) {
        return false;
      }

      List<String> joinAliases = new ArrayList<String>();
      String[] srcs = joinCxt.getBaseSrc();
      String[] left = joinCxt.getLeftAliases();
      List<String> mapAlias = joinCxt.getMapAliases();
      String baseBigAlias = null;
      for(String s : left) {
        if(s != null && !joinAliases.contains(s)) {
          joinAliases.add(s);
          if(!mapAlias.contains(s)) {
            baseBigAlias = s;
          }
        }
      }
      for(String s : srcs) {
        if(s != null && !joinAliases.contains(s)) {
          joinAliases.add(s);
          if(!mapAlias.contains(s)) {
            baseBigAlias = s;
          }
        }
      }

      MapJoinDesc mjDesc = mapJoinOp.getConf();
      LinkedHashMap<String, List<Integer>> aliasToPartitionBucketNumberMapping =
          new LinkedHashMap<String, List<Integer>>();
      LinkedHashMap<String, List<List<String>>> aliasToPartitionBucketFileNamesMapping =
          new LinkedHashMap<String, List<List<String>>>();

      Map<String, Operator<? extends OperatorDesc>> topOps =
        this.pGraphContext.getTopOps();
      Map<TableScanOperator, Table> topToTable = this.pGraphContext.getTopToTable();

      // (partition to bucket file names) and (partition to bucket number) for
      // the big table;
      LinkedHashMap<Partition, List<String>> bigTblPartsToBucketFileNames = new LinkedHashMap<Partition, List<String>>();
      LinkedHashMap<Partition, Integer> bigTblPartsToBucketNumber = new LinkedHashMap<Partition, Integer>();

      Integer[] orders = null; // accessing order of join cols to bucket cols, should be same
      boolean bigTablePartitioned = true;
      for (int index = 0; index < joinAliases.size(); index++) {
        String alias = joinAliases.get(index);
        TableScanOperator tso = (TableScanOperator) topOps.get(alias);
        if (tso == null) {
          return false;
        }
        List<String> keys = toColumns(mjDesc.getKeys().get((byte) index));
        if (keys == null || keys.isEmpty()) {
          return false;
        }
        if (orders == null) {
          orders = new Integer[keys.size()];
        }

        Table tbl = topToTable.get(tso);
        if(tbl.isPartitioned()) {
          PrunedPartitionList prunedParts;
          try {
            prunedParts = pGraphContext.getOpToPartList().get(tso);
            if (prunedParts == null) {
              prunedParts = PartitionPruner.prune(tbl, pGraphContext.getOpToPartPruner().get(tso), pGraphContext.getConf(), alias,
                pGraphContext.getPrunedPartitions());
              pGraphContext.getOpToPartList().put(tso, prunedParts);
            }
          } catch (HiveException e) {
            // Has to use full name to make sure it does not conflict with
            // org.apache.commons.lang.StringUtils
            LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
            throw new SemanticException(e.getMessage(), e);
          }
          List<Partition> partitions = prunedParts.getNotDeniedPartns();
          // construct a mapping of (Partition->bucket file names) and (Partition -> bucket number)
          if (partitions.isEmpty()) {
            if (!alias.equals(baseBigAlias)) {
              aliasToPartitionBucketNumberMapping.put(alias, Arrays.<Integer>asList());
              aliasToPartitionBucketFileNamesMapping.put(alias, new ArrayList<List<String>>());
            }
          } else {
            List<Integer> buckets = new ArrayList<Integer>();
            List<List<String>> files = new ArrayList<List<String>>();
            for (Partition p : partitions) {
              if (!checkBucketColumns(p.getBucketCols(), keys, orders)) {
                return false;
              }
              List<String> fileNames = getOnePartitionBucketFileNames(p.getDataLocation());
              // The number of files for the table should be same as number of buckets.
              int bucketCount = p.getBucketCount();
              if (fileNames.size() != bucketCount) {
                String msg = "The number of buckets for table " +
                  tbl.getTableName() + " partition " + p.getName() + " is " +
                  p.getBucketCount() + ", whereas the number of files is " + fileNames.size();
                throw new SemanticException(
                  ErrorMsg.BUCKETED_TABLE_METADATA_INCORRECT.getMsg(msg));
              }
              if (alias.equals(baseBigAlias)) {
                bigTblPartsToBucketFileNames.put(p, fileNames);
                bigTblPartsToBucketNumber.put(p, bucketCount);
              } else {
                files.add(fileNames);
                buckets.add(bucketCount);
              }
            }
            if (!alias.equals(baseBigAlias)) {
              aliasToPartitionBucketNumberMapping.put(alias, buckets);
              aliasToPartitionBucketFileNamesMapping.put(alias, files);
            }
          }
        } else {
          if (!checkBucketColumns(tbl.getBucketCols(), keys, orders)) {
            return false;
          }
          List<String> fileNames = getOnePartitionBucketFileNames(tbl.getDataLocation());
          Integer num = new Integer(tbl.getNumBuckets());
          // The number of files for the table should be same as number of buckets.
          if (fileNames.size() != num) {
            String msg = "The number of buckets for table " +
              tbl.getTableName() + " is " + tbl.getNumBuckets() +
              ", whereas the number of files is " + fileNames.size();
            throw new SemanticException(
              ErrorMsg.BUCKETED_TABLE_METADATA_INCORRECT.getMsg(msg));
          }
          if (alias.equals(baseBigAlias)) {
            bigTblPartsToBucketFileNames.put(null, fileNames);
            bigTblPartsToBucketNumber.put(null, tbl.getNumBuckets());
            bigTablePartitioned = false;
          } else {
            aliasToPartitionBucketNumberMapping.put(alias, Arrays.asList(num));
            aliasToPartitionBucketFileNamesMapping.put(alias, Arrays.asList(fileNames));
          }
        }
      }

      // All tables or partitions are bucketed, and their bucket number is
      // stored in 'bucketNumbers', we need to check if the number of buckets in
      // the big table can be divided by no of buckets in small tables.
      for (Integer bucketNumber : bigTblPartsToBucketNumber.values()) {
        if (!checkBucketNumberAgainstBigTable(aliasToPartitionBucketNumberMapping, bucketNumber)) {
          return false;
        }
      }

      MapJoinDesc desc = mapJoinOp.getConf();

      Map<String, Map<String, List<String>>> aliasBucketFileNameMapping =
        new LinkedHashMap<String, Map<String, List<String>>>();

      //sort bucket names for the big table
      for(List<String> partBucketNames : bigTblPartsToBucketFileNames.values()) {
        Collections.sort(partBucketNames);
      }

      // go through all small tables and get the mapping from bucket file name
      // in the big table to bucket file names in small tables.
      for (int j = 0; j < joinAliases.size(); j++) {
        String alias = joinAliases.get(j);
        if (alias.equals(baseBigAlias)) {
          continue;
        }
        for (List<String> names : aliasToPartitionBucketFileNamesMapping.get(alias)) {
          Collections.sort(names);
        }
        List<Integer> smallTblBucketNums = aliasToPartitionBucketNumberMapping.get(alias);
        List<List<String>> smallTblFilesList = aliasToPartitionBucketFileNamesMapping.get(alias);

        Map<String, List<String>> mapping = new LinkedHashMap<String, List<String>>();
        aliasBucketFileNameMapping.put(alias, mapping);

        // for each bucket file in big table, get the corresponding bucket file
        // name in the small table.
        //more than 1 partition in the big table, do the mapping for each partition
        Iterator<Entry<Partition, List<String>>> bigTblPartToBucketNames =
            bigTblPartsToBucketFileNames.entrySet().iterator();
        Iterator<Entry<Partition, Integer>> bigTblPartToBucketNum = bigTblPartsToBucketNumber
            .entrySet().iterator();
        while (bigTblPartToBucketNames.hasNext()) {
          assert bigTblPartToBucketNum.hasNext();
          int bigTblBucketNum = bigTblPartToBucketNum.next().getValue();
          List<String> bigTblBucketNameList = bigTblPartToBucketNames.next().getValue();
          fillMapping(smallTblBucketNums, smallTblFilesList,
              mapping, bigTblBucketNum, bigTblBucketNameList, desc.getBigTableBucketNumMapping());
        }
      }
      desc.setAliasBucketFileNameMapping(aliasBucketFileNameMapping);
      desc.setBigTableAlias(baseBigAlias);
      if (bigTablePartitioned) {
        desc.setBigTablePartSpecToFileMapping(convert(bigTblPartsToBucketFileNames));
      }

      return true;
    }


    @Override
    @SuppressWarnings("unchecked")
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      boolean convert = convertBucketMapJoin(nd, stack, procCtx, nodeOutputs);
      BucketMapjoinOptProcCtx context = (BucketMapjoinOptProcCtx) procCtx;
      HiveConf conf = context.getConf();

      // Throw an error if the user asked for bucketed mapjoin to be enforced and
      // bucketed mapjoin cannot be performed
      if (!convert && conf.getBoolVar(HiveConf.ConfVars.HIVEENFORCEBUCKETMAPJOIN)) {
        throw new SemanticException(ErrorMsg.BUCKET_MAPJOIN_NOT_POSSIBLE.getMsg());
      }

      return null;
    }

    private List<String> toColumns(List<ExprNodeDesc> keys) {
      List<String> columns = new ArrayList<String>();
      for (ExprNodeDesc key : keys) {
        if (!(key instanceof ExprNodeColumnDesc)) {
          return null;
        }
        columns.add(((ExprNodeColumnDesc) key).getColumn());
      }
      return columns;
    }

    // convert partition to partition spec string
    private Map<String, List<String>> convert(Map<Partition, List<String>> mapping) {
      Map<String, List<String>> converted = new HashMap<String, List<String>>();
      for (Map.Entry<Partition, List<String>> entry : mapping.entrySet()) {
        converted.put(entry.getKey().getName(), entry.getValue());
      }
      return converted;
    }

    // called for each partition of big table and populates mapping for each file in the partition
    private void fillMapping(
        List<Integer> smallTblBucketNums,
        List<List<String>> smallTblFilesList,
        Map<String, List<String>> mapping,
        int bigTblBucketNum, List<String> bigTblBucketNameList,
        Map<String, Integer> bucketFileNameMapping) {

      for (int bindex = 0; bindex < bigTblBucketNameList.size(); bindex++) {
        ArrayList<String> resultFileNames = new ArrayList<String>();
        for (int sindex = 0 ; sindex < smallTblBucketNums.size(); sindex++) {
          int smallTblBucketNum = smallTblBucketNums.get(sindex);
          List<String> smallTblFileNames = smallTblFilesList.get(sindex);
          if (bigTblBucketNum >= smallTblBucketNum) {
            // if the big table has more buckets than the current small table,
            // use "MOD" to get small table bucket names. For example, if the big
            // table has 4 buckets and the small table has 2 buckets, then the
            // mapping should be 0->0, 1->1, 2->0, 3->1.
            int toAddSmallIndex = bindex % smallTblBucketNum;
            resultFileNames.add(smallTblFileNames.get(toAddSmallIndex));
          } else {
            int jump = smallTblBucketNum / bigTblBucketNum;
            for (int i = bindex; i < smallTblFileNames.size(); i = i + jump) {
              resultFileNames.add(smallTblFileNames.get(i));
            }
          }
        }
        String inputBigTBLBucket = bigTblBucketNameList.get(bindex);
        mapping.put(inputBigTBLBucket, resultFileNames);
        bucketFileNameMapping.put(inputBigTBLBucket, bindex);
      }
    }

    private boolean checkBucketNumberAgainstBigTable(
        Map<String, List<Integer>> aliasToBucketNumber, int bucketNumberInPart) {
      for (List<Integer> bucketNums : aliasToBucketNumber.values()) {
        for (int nxt : bucketNums) {
          boolean ok = (nxt >= bucketNumberInPart) ? nxt % bucketNumberInPart == 0
              : bucketNumberInPart % nxt == 0;
          if (!ok) {
            return false;
          }
        }
      }
      return true;
    }

    private List<String> getOnePartitionBucketFileNames(URI location)
        throws SemanticException {
      List<String> fileNames = new ArrayList<String>();
      try {
        FileSystem fs = FileSystem.get(location, this.pGraphContext.getConf());
        FileStatus[] files = fs.listStatus(new Path(location.toString()));
        if (files != null) {
          for (FileStatus file : files) {
            fileNames.add(file.getPath().toString());
          }
        }
      } catch (IOException e) {
        throw new SemanticException(e);
      }
      return fileNames;
    }

    private boolean checkBucketColumns(List<String> bucketColumns, List<String> keys,
        Integer[] orders) {
      if (keys == null || bucketColumns == null || bucketColumns.isEmpty()) {
        return false;
      }
      for (int i = 0; i < keys.size(); i++) {
        int index = bucketColumns.indexOf(keys.get(i));
        if (orders[i] != null && orders[i] != index) {
          return false;
        }
        orders[i] = index;
      }
      // Check if the join columns contains all bucket columns.
      // If a table is bucketized on column B, but the join key is A and B,
      // it is easy to see joining on different buckets yield empty results.
      return keys.containsAll(bucketColumns);
    }
  }

  class BucketMapjoinOptProcCtx implements NodeProcessorCtx {
    private final HiveConf conf;

    // we only convert map joins that follows a root table scan in the same
    // mapper. That means there is no reducer between the root table scan and
    // mapjoin.
    Set<MapJoinOperator> listOfRejectedMapjoins = new HashSet<MapJoinOperator>();

    public BucketMapjoinOptProcCtx(HiveConf conf) {
      this.conf = conf;
    }

    public HiveConf getConf() {
      return conf;
    }

    public Set<MapJoinOperator> getListOfRejectedMapjoins() {
      return listOfRejectedMapjoins;
    }
  }
}
