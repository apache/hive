/*
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TableAccessAnalyzer;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * this transformation does bucket map join optimization.
 */
abstract public class AbstractBucketJoinProc implements SemanticNodeProcessor {

  protected ParseContext pGraphContext;

  public AbstractBucketJoinProc(ParseContext pGraphContext) {
    this.pGraphContext = pGraphContext;
  }

  public AbstractBucketJoinProc() {
  }

  @Override
  abstract public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
      Object... nodeOutputs) throws SemanticException;

  public static List<String> getBucketFilePathsOfPartition(
      Path location, ParseContext pGraphContext) throws SemanticException {
    List<String> fileNames = new ArrayList<String>();
    try {
      FileSystem fs = location.getFileSystem(pGraphContext.getConf());
      FileStatus[] files = fs.listStatus(new Path(location.toString()), FileUtils.HIDDEN_FILES_PATH_FILTER);
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

  // This function checks whether all bucketing columns are also in join keys and are in same order
  private boolean checkBucketColumns(List<String> bucketColumns,
      List<String> joinKeys,
      Integer[] joinKeyOrders) {
    if (joinKeys == null || bucketColumns == null || bucketColumns.isEmpty()) {
      return false;
    }
    for (int i = 0; i < joinKeys.size(); i++) {
      int index = bucketColumns.indexOf(joinKeys.get(i));
      if (joinKeyOrders[i] != null && joinKeyOrders[i] != index) {
        return false;
      }
      joinKeyOrders[i] = index;
    }

    // Check if the join columns contains all bucket columns.
    // If a table is bucketized on column B, but the join key is A and B,
    // it is easy to see joining on different buckets yield empty results.
    return joinKeys.containsAll(bucketColumns);
  }

  private boolean checkNumberOfBucketsAgainstBigTable(
      Map<String, List<Integer>> tblAliasToNumberOfBucketsInEachPartition,
      int numberOfBucketsInPartitionOfBigTable) {
    for (List<Integer> bucketNums : tblAliasToNumberOfBucketsInEachPartition.values()) {
      for (int nxt : bucketNums) {
        boolean ok = (nxt >= numberOfBucketsInPartitionOfBigTable) ? nxt
            % numberOfBucketsInPartitionOfBigTable == 0
            : numberOfBucketsInPartitionOfBigTable % nxt == 0;
        if (!ok) {
          return false;
        }
      }
    }
    return true;
  }

  protected boolean canConvertMapJoinToBucketMapJoin(
      MapJoinOperator mapJoinOp,
      BucketJoinProcCtx context) throws SemanticException {

    if (!this.pGraphContext.getMapJoinOps().contains(mapJoinOp)) {
      return false;
    }

    List<String> joinAliases = new ArrayList<String>();
    String[] srcs = mapJoinOp.getConf().getBaseSrc();
    String[] left = mapJoinOp.getConf().getLeftAliases();
    List<String> mapAlias = mapJoinOp.getConf().getMapAliases();
    String baseBigAlias = null;

    for (String s : left) {
      if (s != null) {
        String subQueryAlias = QB.getAppendedAliasFromId(mapJoinOp.getConf().getId(), s);
        if (!joinAliases.contains(subQueryAlias)) {
          joinAliases.add(subQueryAlias);
          if (!mapAlias.contains(s)) {
            baseBigAlias = subQueryAlias;
          }
        }
      }
    }

    for (String s : srcs) {
      if (s != null) {
        String subQueryAlias = QB.getAppendedAliasFromId(mapJoinOp.getConf().getId(), s);
        if (!joinAliases.contains(subQueryAlias)) {
          joinAliases.add(subQueryAlias);
          if (!mapAlias.contains(s)) {
            baseBigAlias = subQueryAlias;
          }
        }
      }
    }

    Map<Byte, List<ExprNodeDesc>> keysMap = mapJoinOp.getConf().getKeys();

    return checkConvertBucketMapJoin(
        context,
        mapJoinOp.getConf().getAliasToOpInfo(),
        keysMap,
        baseBigAlias,
        joinAliases);
  }

  /*
   * Can this mapjoin be converted to a bucketed mapjoin ?
   * The following checks are performed:
   * a. The join columns contains all the bucket columns.
   * b. The join keys are not transformed in the sub-query.
   * c. All partitions contain the expected number of files (number of buckets).
   * d. The number of buckets in the big table can be divided by no of buckets in small tables.
   */
  protected boolean checkConvertBucketMapJoin(
      BucketJoinProcCtx context,
      Map<String, Operator<? extends OperatorDesc>> aliasToOpInfo,
      Map<Byte, List<ExprNodeDesc>> keysMap,
      String baseBigAlias,
      List<String> joinAliases) throws SemanticException {

    Map<String, List<Integer>> tblAliasToNumberOfBucketsInEachPartition =
        new LinkedHashMap<String, List<Integer>>();
    Map<String, List<List<String>>> tblAliasToBucketedFilePathsInEachPartition =
        new LinkedHashMap<String, List<List<String>>>();

    Map<String, TableScanOperator> topOps = pGraphContext.getTopOps();

    Map<String, String> aliasToNewAliasMap = new HashMap<String, String>();

    // (partition to bucket file names) and (partition to bucket number) for
    // the big table;
    Map<Partition, List<String>> bigTblPartsToBucketFileNames = new LinkedHashMap<Partition, List<String>>();
    Map<Partition, Integer> bigTblPartsToBucketNumber = new LinkedHashMap<Partition, Integer>();

    Integer[] joinKeyOrder = null; // accessing order of join cols to bucket cols, should be same
    boolean bigTablePartitioned = true;
    for (int index = 0; index < joinAliases.size(); index++) {
      String alias = joinAliases.get(index);
      Operator<? extends OperatorDesc> topOp = aliasToOpInfo.get(alias);
      // The alias may not be present in case of a sub-query
      if (topOp == null) {
        return false;
      }
      List<String> keys = toColumns(keysMap.get((byte) index));
      if (keys == null || keys.isEmpty()) {
        return false;
      }
      int oldKeySize = keys.size();
      TableScanOperator tso = TableAccessAnalyzer.genRootTableScan(topOp, keys);
      if (tso == null) {
        // We cannot get to root TableScan operator, likely because there is a join or group-by
        // between topOp and root TableScan operator. We don't handle that case, and simply return
        return false;
      }

      // For nested sub-queries, the alias mapping is not maintained in QB currently.
      if (topOps.containsValue(tso)) {
        for (Map.Entry<String, TableScanOperator> topOpEntry : topOps.entrySet()) {
          if (topOpEntry.getValue() == tso) {
            String newAlias = topOpEntry.getKey();
            if (!newAlias.equals(alias)) {
              joinAliases.set(index, newAlias);
              if (baseBigAlias.equals(alias)) {
                baseBigAlias = newAlias;
              }
              aliasToNewAliasMap.put(alias, newAlias);
              alias = newAlias;
            }
            break;
          }
        }
      }
      else {
        // Ideally, this should never happen, and this should be an assert.
        return false;
      }

      // The join keys cannot be transformed in the sub-query currently.
      // TableAccessAnalyzer.genRootTableScan will only return the base table scan
      // if the join keys are constants or a column. Even a simple cast of the join keys
      // will result in a null table scan operator. In case of constant join keys, they would
      // be removed, and the size before and after the genRootTableScan will be different.
      if (keys.size() != oldKeySize) {
        return false;
      }

      if (joinKeyOrder == null) {
        joinKeyOrder = new Integer[keys.size()];
      }

      Table tbl = tso.getConf().getTableMetadata();
      if (AcidUtils.isInsertOnlyTable(tbl.getParameters())) {
        Utilities.FILE_OP_LOGGER.debug("No bucketed join on MM table " + tbl.getTableName());
        return false;
      }
      if (tbl.isPartitioned()) {
        PrunedPartitionList prunedParts = pGraphContext.getPrunedPartitions(alias, tso);
        List<Partition> partitions = prunedParts.getNotDeniedPartns();
        // construct a mapping of (Partition->bucket file names) and (Partition -> bucket number)
        if (partitions.isEmpty()) {
          if (!alias.equals(baseBigAlias)) {
            tblAliasToNumberOfBucketsInEachPartition.put(alias, Arrays.<Integer> asList());
            tblAliasToBucketedFilePathsInEachPartition.put(alias, new ArrayList<List<String>>());
          }
        } else {
          List<Integer> buckets = new ArrayList<Integer>();
          List<List<String>> files = new ArrayList<List<String>>();
          for (Partition p : partitions) {
            if (!checkBucketColumns(p.getBucketCols(), keys, joinKeyOrder)) {
              return false;
            }
            List<String> fileNames =
                getBucketFilePathsOfPartition(p.getDataLocation(), pGraphContext);
            // The number of files for the table should be same as number of buckets.
            int bucketCount = p.getBucketCount();

            if (fileNames.size() != 0 && fileNames.size() != bucketCount) {
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
            tblAliasToNumberOfBucketsInEachPartition.put(alias, buckets);
            tblAliasToBucketedFilePathsInEachPartition.put(alias, files);
          }
        }
      } else {
        if (!checkBucketColumns(tbl.getBucketCols(), keys, joinKeyOrder)) {
          return false;
        }
        List<String> fileNames =
            getBucketFilePathsOfPartition(tbl.getDataLocation(), pGraphContext);
        int num = tbl.getNumBuckets();

        // The number of files for the table should be same as number of buckets.
        if (fileNames.size() != 0 && fileNames.size() != num) {
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
          tblAliasToNumberOfBucketsInEachPartition.put(alias, Arrays.asList(Integer.valueOf(num)));
          tblAliasToBucketedFilePathsInEachPartition.put(alias, Arrays.asList(fileNames));
        }
      }
    }

    // All tables or partitions are bucketed, and their bucket number is
    // stored in 'bucketNumbers', we need to check if the number of buckets in
    // the big table can be divided by no of buckets in small tables.
    for (Integer numBucketsInPartitionOfBigTable : bigTblPartsToBucketNumber.values()) {
      if (!checkNumberOfBucketsAgainstBigTable(
          tblAliasToNumberOfBucketsInEachPartition, numBucketsInPartitionOfBigTable)) {
        return false;
      }
    }

    context.setTblAliasToNumberOfBucketsInEachPartition(tblAliasToNumberOfBucketsInEachPartition);
    context.setTblAliasToBucketedFilePathsInEachPartition(
        tblAliasToBucketedFilePathsInEachPartition);
    context.setBigTblPartsToBucketFileNames(bigTblPartsToBucketFileNames);
    context.setBigTblPartsToBucketNumber(bigTblPartsToBucketNumber);
    context.setJoinAliases(joinAliases);
    context.setBaseBigAlias(baseBigAlias);
    context.setBigTablePartitioned(bigTablePartitioned);
    if (!aliasToNewAliasMap.isEmpty()) {
      context.setAliasToNewAliasMap(aliasToNewAliasMap);
    }

    return true;
  }

  /*
   * Convert mapjoin to a bucketed mapjoin.
   * The operator tree is not changed, but the mapjoin descriptor in the big table is
   * enhanced to keep the big table bucket -> small table buckets mapping.
   */
  protected void convertMapJoinToBucketMapJoin(
      MapJoinOperator mapJoinOp,
      BucketJoinProcCtx context) throws SemanticException {
    MapJoinDesc desc = mapJoinOp.getConf();

    Map<String, Map<String, List<String>>> aliasBucketFileNameMapping =
        new LinkedHashMap<String, Map<String, List<String>>>();

    Map<String, List<Integer>> tblAliasToNumberOfBucketsInEachPartition =
        context.getTblAliasToNumberOfBucketsInEachPartition();

    Map<String, List<List<String>>> tblAliasToBucketedFilePathsInEachPartition =
        context.getTblAliasToBucketedFilePathsInEachPartition();

    Map<Partition, List<String>> bigTblPartsToBucketFileNames =
        context.getBigTblPartsToBucketFileNames();

    Map<Partition, Integer> bigTblPartsToBucketNumber =
        context.getBigTblPartsToBucketNumber();

    List<String> joinAliases = context.getJoinAliases();
    String baseBigAlias = context.getBaseBigAlias();

    // sort bucket names for the big table
    for (List<String> partBucketNames : bigTblPartsToBucketFileNames.values()) {
      Collections.sort(partBucketNames);
    }

    // go through all small tables and get the mapping from bucket file name
    // in the big table to bucket file names in small tables.
    for (int j = 0; j < joinAliases.size(); j++) {
      String alias = joinAliases.get(j);
      if (alias.equals(baseBigAlias)) {
        continue;
      }
      for (List<String> names : tblAliasToBucketedFilePathsInEachPartition.get(alias)) {
        Collections.sort(names);
      }
      List<Integer> smallTblBucketNums = tblAliasToNumberOfBucketsInEachPartition.get(alias);
      List<List<String>> smallTblFilesList = tblAliasToBucketedFilePathsInEachPartition.get(alias);

      Map<String, List<String>> mappingBigTableBucketFileNameToSmallTableBucketFileNames =
          new LinkedHashMap<String, List<String>>();
      aliasBucketFileNameMapping.put(alias,
          mappingBigTableBucketFileNameToSmallTableBucketFileNames);

      // for each bucket file in big table, get the corresponding bucket file
      // name in the small table.
      // more than 1 partition in the big table, do the mapping for each partition
      Iterator<Entry<Partition, List<String>>> bigTblPartToBucketNames =
          bigTblPartsToBucketFileNames.entrySet().iterator();
      Iterator<Entry<Partition, Integer>> bigTblPartToBucketNum = bigTblPartsToBucketNumber
          .entrySet().iterator();
      while (bigTblPartToBucketNames.hasNext()) {
        assert bigTblPartToBucketNum.hasNext();
        int bigTblBucketNum = bigTblPartToBucketNum.next().getValue();
        List<String> bigTblBucketNameList = bigTblPartToBucketNames.next().getValue();
        fillMappingBigTableBucketFileNameToSmallTableBucketFileNames(smallTblBucketNums,
            smallTblFilesList,
            mappingBigTableBucketFileNameToSmallTableBucketFileNames, bigTblBucketNum,
            bigTblBucketNameList,
            desc.getBigTableBucketNumMapping());
      }
    }
    desc.setAliasBucketFileNameMapping(aliasBucketFileNameMapping);
    desc.setBigTableAlias(baseBigAlias);
    boolean bigTablePartitioned = context.isBigTablePartitioned();
    if (bigTablePartitioned) {
      desc.setBigTablePartSpecToFileMapping(convert(bigTblPartsToBucketFileNames));
    }

    Map<Integer, Set<String>> posToAliasMap = mapJoinOp.getPosToAliasMap();
    Map<String, String> aliasToNewAliasMap = context.getAliasToNewAliasMap();
    if (aliasToNewAliasMap != null && posToAliasMap != null) {
      for (Map.Entry<String, String> entry: aliasToNewAliasMap.entrySet()) {
        for (Set<String> aliases: posToAliasMap.values()) {
          if (aliases.remove(entry.getKey())) {
            aliases.add(entry.getValue());
          }
        }
      }
    }

    // successfully convert to bucket map join
    desc.setBucketMapJoin(true);
  }

  // convert partition to partition spec string
  private Map<String, List<String>> convert(Map<Partition, List<String>> mapping) {
    Map<String, List<String>> converted = new HashMap<String, List<String>>();
    for (Map.Entry<Partition, List<String>> entry : mapping.entrySet()) {
      converted.put(entry.getKey().getName(), entry.getValue());
    }
    return converted;
  }

  public static List<String> toColumns(List<ExprNodeDesc> keys) {
    List<String> columns = new ArrayList<String>();
    for (ExprNodeDesc key : keys) {
      if (key instanceof ExprNodeColumnDesc) {
        columns.add(((ExprNodeColumnDesc) key).getColumn());
      } else if ((key instanceof ExprNodeConstantDesc)) {
        ExprNodeConstantDesc constant = (ExprNodeConstantDesc) key;
        String colName = constant.getFoldedFromCol();
        if (colName == null){
          return null;
        } else {
          columns.add(colName);
        }
      } else {
        return null;
      }
    }
    return columns;
  }

  // called for each partition of big table and populates mapping for each file in the partition
  private void fillMappingBigTableBucketFileNameToSmallTableBucketFileNames(
      List<Integer> smallTblBucketNums,
      List<List<String>> smallTblFilesList,
      Map<String, List<String>> bigTableBucketFileNameToSmallTableBucketFileNames,
      int bigTblBucketNum, List<String> bigTblBucketNameList,
      Map<String, Integer> bucketFileNameMapping) {

    for (int bindex = 0; bindex < bigTblBucketNameList.size(); bindex++) {
      ArrayList<String> resultFileNames = new ArrayList<String>();
      for (int sindex = 0; sindex < smallTblBucketNums.size(); sindex++) {
        int smallTblBucketNum = smallTblBucketNums.get(sindex);
        List<String> smallTblFileNames = smallTblFilesList.get(sindex);
        if (smallTblFileNames.size() > 0) {
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
      }
      String inputBigTBLBucket = bigTblBucketNameList.get(bindex);
      bigTableBucketFileNameToSmallTableBucketFileNames.put(inputBigTBLBucket, resultFileNames);
      bucketFileNameMapping.put(inputBigTBLBucket, bindex);
    }
  }
}
