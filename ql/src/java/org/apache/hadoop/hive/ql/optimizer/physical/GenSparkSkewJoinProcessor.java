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

import com.google.common.base.Preconditions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SparkHashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.spark.GenSparkUtils;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverSkewJoin;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.HashTableDummyDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SparkEdgeProperty;
import org.apache.hadoop.hive.ql.plan.SparkHashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copied from GenMRSkewJoinProcessor. It's used for spark task
 *
 */
public class GenSparkSkewJoinProcessor {
  private static final Log LOG = LogFactory.getLog(GenSparkSkewJoinProcessor.class.getName());

  private GenSparkSkewJoinProcessor() {
    // prevent instantiation
  }

  @SuppressWarnings("unchecked")
  public static void processSkewJoin(JoinOperator joinOp, Task<? extends Serializable> currTask,
      ReduceWork reduceWork, ParseContext parseCtx) throws SemanticException {

    SparkWork currentWork = ((SparkTask) currTask).getWork();
    if (currentWork.getChildren(reduceWork).size() > 0) {
      LOG.warn("Skip runtime skew join as the ReduceWork has child work and hasn't been split.");
      return;
    }

    List<Task<? extends Serializable>> children = currTask.getChildTasks();

    Task<? extends Serializable> child =
        children != null && children.size() == 1 ? children.get(0) : null;

    Path baseTmpDir = parseCtx.getContext().getMRTmpPath();

    JoinDesc joinDescriptor = joinOp.getConf();
    Map<Byte, List<ExprNodeDesc>> joinValues = joinDescriptor.getExprs();
    int numAliases = joinValues.size();

    Map<Byte, Path> bigKeysDirMap = new HashMap<Byte, Path>();
    Map<Byte, Map<Byte, Path>> smallKeysDirMap = new HashMap<Byte, Map<Byte, Path>>();
    Map<Byte, Path> skewJoinJobResultsDir = new HashMap<Byte, Path>();
    Byte[] tags = joinDescriptor.getTagOrder();
    // for each joining table, set dir for big key and small keys properly
    for (int i = 0; i < numAliases; i++) {
      Byte alias = tags[i];
      bigKeysDirMap.put(alias, GenMRSkewJoinProcessor.getBigKeysDir(baseTmpDir, alias));
      Map<Byte, Path> smallKeysMap = new HashMap<Byte, Path>();
      smallKeysDirMap.put(alias, smallKeysMap);
      for (Byte src2 : tags) {
        if (!src2.equals(alias)) {
          smallKeysMap.put(src2, GenMRSkewJoinProcessor.getSmallKeysDir(baseTmpDir, alias, src2));
        }
      }
      skewJoinJobResultsDir.put(alias,
          GenMRSkewJoinProcessor.getBigKeysSkewJoinResultDir(baseTmpDir, alias));
    }

    joinDescriptor.setHandleSkewJoin(true);
    joinDescriptor.setBigKeysDirMap(bigKeysDirMap);
    joinDescriptor.setSmallKeysDirMap(smallKeysDirMap);
    joinDescriptor.setSkewKeyDefinition(HiveConf.getIntVar(parseCtx.getConf(),
        HiveConf.ConfVars.HIVESKEWJOINKEY));

    // create proper table/column desc for spilled tables
    TableDesc keyTblDesc = (TableDesc) reduceWork.getKeyDesc().clone();
    List<String> joinKeys = Utilities
        .getColumnNames(keyTblDesc.getProperties());
    List<String> joinKeyTypes = Utilities.getColumnTypes(keyTblDesc
        .getProperties());

    Map<Byte, TableDesc> tableDescList = new HashMap<Byte, TableDesc>();
    Map<Byte, RowSchema> rowSchemaList = new HashMap<Byte, RowSchema>();
    Map<Byte, List<ExprNodeDesc>> newJoinValues = new HashMap<Byte, List<ExprNodeDesc>>();
    Map<Byte, List<ExprNodeDesc>> newJoinKeys = new HashMap<Byte, List<ExprNodeDesc>>();
    // used for create mapJoinDesc, should be in order
    List<TableDesc> newJoinValueTblDesc = new ArrayList<TableDesc>();

    for (int i = 0; i < tags.length; i++) {
      newJoinValueTblDesc.add(null);
    }

    for (int i = 0; i < numAliases; i++) {
      Byte alias = tags[i];
      List<ExprNodeDesc> valueCols = joinValues.get(alias);
      String colNames = "";
      String colTypes = "";
      int columnSize = valueCols.size();
      List<ExprNodeDesc> newValueExpr = new ArrayList<ExprNodeDesc>();
      List<ExprNodeDesc> newKeyExpr = new ArrayList<ExprNodeDesc>();
      ArrayList<ColumnInfo> columnInfos = new ArrayList<ColumnInfo>();

      boolean first = true;
      for (int k = 0; k < columnSize; k++) {
        TypeInfo type = valueCols.get(k).getTypeInfo();
        String newColName = i + "_VALUE_" + k; // any name, it does not matter.
        ColumnInfo columnInfo = new ColumnInfo(newColName, type, alias.toString(), false);
        columnInfos.add(columnInfo);
        newValueExpr.add(new ExprNodeColumnDesc(
            columnInfo.getType(), columnInfo.getInternalName(),
            columnInfo.getTabAlias(), false));
        if (!first) {
          colNames = colNames + ",";
          colTypes = colTypes + ",";
        }
        first = false;
        colNames = colNames + newColName;
        colTypes = colTypes + valueCols.get(k).getTypeString();
      }

      // we are putting join keys at last part of the spilled table
      for (int k = 0; k < joinKeys.size(); k++) {
        if (!first) {
          colNames = colNames + ",";
          colTypes = colTypes + ",";
        }
        first = false;
        colNames = colNames + joinKeys.get(k);
        colTypes = colTypes + joinKeyTypes.get(k);
        ColumnInfo columnInfo = new ColumnInfo(joinKeys.get(k), TypeInfoFactory
            .getPrimitiveTypeInfo(joinKeyTypes.get(k)), alias.toString(), false);
        columnInfos.add(columnInfo);
        newKeyExpr.add(new ExprNodeColumnDesc(
            columnInfo.getType(), columnInfo.getInternalName(),
            columnInfo.getTabAlias(), false));
      }

      newJoinValues.put(alias, newValueExpr);
      newJoinKeys.put(alias, newKeyExpr);
      tableDescList.put(alias, Utilities.getTableDesc(colNames, colTypes));
      rowSchemaList.put(alias, new RowSchema(columnInfos));

      // construct value table Desc
      String valueColNames = "";
      String valueColTypes = "";
      first = true;
      for (int k = 0; k < columnSize; k++) {
        String newColName = i + "_VALUE_" + k; // any name, it does not matter.
        if (!first) {
          valueColNames = valueColNames + ",";
          valueColTypes = valueColTypes + ",";
        }
        valueColNames = valueColNames + newColName;
        valueColTypes = valueColTypes + valueCols.get(k).getTypeString();
        first = false;
      }
      newJoinValueTblDesc.set((byte) i, Utilities.getTableDesc(
          valueColNames, valueColTypes));
    }

    joinDescriptor.setSkewKeysValuesTables(tableDescList);
    joinDescriptor.setKeyTableDesc(keyTblDesc);

    // create N-1 map join tasks
    HashMap<Path, Task<? extends Serializable>> bigKeysDirToTaskMap =
        new HashMap<Path, Task<? extends Serializable>>();
    List<Serializable> listWorks = new ArrayList<Serializable>();
    List<Task<? extends Serializable>> listTasks = new ArrayList<Task<? extends Serializable>>();
    for (int i = 0; i < numAliases - 1; i++) {
      Byte src = tags[i];
      HiveConf hiveConf = new HiveConf(parseCtx.getConf(),
          GenSparkSkewJoinProcessor.class);
      SparkWork sparkWork = new SparkWork(parseCtx.getConf().getVar(HiveConf.ConfVars.HIVEQUERYID));
      Task<? extends Serializable> skewJoinMapJoinTask = TaskFactory.get(sparkWork, hiveConf);
      skewJoinMapJoinTask.setFetchSource(currTask.isFetchSource());

      // create N TableScans
      Operator<? extends OperatorDesc>[] parentOps = new TableScanOperator[tags.length];
      for (int k = 0; k < tags.length; k++) {
        Operator<? extends OperatorDesc> ts =
            GenMapRedUtils.createTemporaryTableScanOperator(rowSchemaList.get((byte) k));
        ((TableScanOperator) ts).setTableDesc(tableDescList.get((byte) k));
        parentOps[k] = ts;
      }

      // create the MapJoinOperator
      String dumpFilePrefix = "mapfile" + PlanUtils.getCountForMapJoinDumpFilePrefix();
      MapJoinDesc mapJoinDescriptor = new MapJoinDesc(newJoinKeys, keyTblDesc,
          newJoinValues, newJoinValueTblDesc, newJoinValueTblDesc, joinDescriptor
          .getOutputColumnNames(), i, joinDescriptor.getConds(),
          joinDescriptor.getFilters(), joinDescriptor.getNoOuterJoin(), dumpFilePrefix);
      mapJoinDescriptor.setTagOrder(tags);
      mapJoinDescriptor.setHandleSkewJoin(false);
      mapJoinDescriptor.setNullSafes(joinDescriptor.getNullSafes());
      // temporarily, mark it as child of all the TS
      MapJoinOperator mapJoinOp = (MapJoinOperator) OperatorFactory
          .getAndMakeChild(mapJoinDescriptor, null, parentOps);

      // clone the original join operator, and replace it with the MJ
      // this makes sure MJ has the same downstream operator plan as the original join
      List<Operator<?>> reducerList = new ArrayList<Operator<?>>();
      reducerList.add(reduceWork.getReducer());
      Operator<? extends OperatorDesc> reducer = Utilities.cloneOperatorTree(
          parseCtx.getConf(), reducerList).get(0);
      Preconditions.checkArgument(reducer instanceof JoinOperator,
          "Reducer should be join operator, but actually is " + reducer.getName());
      JoinOperator cloneJoinOp = (JoinOperator) reducer;
      List<Operator<? extends OperatorDesc>> childOps = cloneJoinOp
          .getChildOperators();
      for (Operator<? extends OperatorDesc> childOp : childOps) {
        childOp.replaceParent(cloneJoinOp, mapJoinOp);
      }
      mapJoinOp.setChildOperators(childOps);

      // set memory usage for the MJ operator
      setMemUsage(mapJoinOp, skewJoinMapJoinTask, parseCtx);

      // create N MapWorks and add them to the SparkWork
      MapWork bigMapWork = null;
      Map<Byte, Path> smallTblDirs = smallKeysDirMap.get(src);
      for (int j = 0; j < tags.length; j++) {
        MapWork mapWork = PlanUtils.getMapRedWork().getMapWork();
        sparkWork.add(mapWork);
        // This code has been only added for testing
        boolean mapperCannotSpanPartns =
            parseCtx.getConf().getBoolVar(
                HiveConf.ConfVars.HIVE_MAPPER_CANNOT_SPAN_MULTIPLE_PARTITIONS);
        mapWork.setMapperCannotSpanPartns(mapperCannotSpanPartns);
        Operator<? extends OperatorDesc> tableScan = parentOps[j];
        String alias = tags[j].toString();
        ArrayList<String> aliases = new ArrayList<String>();
        aliases.add(alias);
        Path path;
        if (j == i) {
          path = bigKeysDirMap.get(tags[j]);
          bigKeysDirToTaskMap.put(path, skewJoinMapJoinTask);
          bigMapWork = mapWork;
        } else {
          path = smallTblDirs.get(tags[j]);
        }
        mapWork.getPathToAliases().put(path.toString(), aliases);
        mapWork.getAliasToWork().put(alias, tableScan);
        PartitionDesc partitionDesc = new PartitionDesc(tableDescList.get(tags[j]), null);
        mapWork.getPathToPartitionInfo().put(path.toString(), partitionDesc);
        mapWork.getAliasToPartnInfo().put(alias, partitionDesc);
        mapWork.setNumMapTasks(HiveConf.getIntVar(hiveConf,
            HiveConf.ConfVars.HIVESKEWJOINMAPJOINNUMMAPTASK));
        mapWork.setMinSplitSize(HiveConf.getLongVar(hiveConf,
            HiveConf.ConfVars.HIVESKEWJOINMAPJOINMINSPLIT));
        mapWork.setInputformat(HiveInputFormat.class.getName());
        mapWork.setName("Map " + GenSparkUtils.getUtils().getNextSeqNumber());
      }
      // connect all small dir map work to the big dir map work
      Preconditions.checkArgument(bigMapWork != null, "Haven't identified big dir MapWork");
      for (BaseWork work : sparkWork.getRoots()) {
        Preconditions.checkArgument(work instanceof MapWork,
            "All root work should be MapWork, but got " + work.getClass().getSimpleName());
        if (work != bigMapWork) {
          sparkWork.connect(work, bigMapWork,
              new SparkEdgeProperty(SparkEdgeProperty.SHUFFLE_NONE));
        }
      }

      // insert SparkHashTableSink and Dummy operators
      for (int j = 0; j < tags.length; j++) {
        if (j != i) {
          insertSHTS(tags[j], (TableScanOperator) parentOps[j], bigMapWork);
        }
      }

      listWorks.add(skewJoinMapJoinTask.getWork());
      listTasks.add(skewJoinMapJoinTask);
    }
    if (children != null) {
      for (Task<? extends Serializable> tsk : listTasks) {
        for (Task<? extends Serializable> oldChild : children) {
          tsk.addDependentTask(oldChild);
        }
      }
    }
    if (child != null) {
      currTask.removeDependentTask(child);
      listTasks.add(child);
      listWorks.add(child.getWork());
    }
    ConditionalResolverSkewJoin.ConditionalResolverSkewJoinCtx context =
        new ConditionalResolverSkewJoin.ConditionalResolverSkewJoinCtx(bigKeysDirToTaskMap, child);

    ConditionalWork cndWork = new ConditionalWork(listWorks);
    ConditionalTask cndTsk = (ConditionalTask) TaskFactory.get(cndWork, parseCtx.getConf());
    cndTsk.setListTasks(listTasks);
    cndTsk.setResolver(new ConditionalResolverSkewJoin());
    cndTsk.setResolverCtx(context);
    currTask.setChildTasks(new ArrayList<Task<? extends Serializable>>());
    currTask.addDependentTask(cndTsk);
  }

  /**
   * Insert SparkHashTableSink and HashTableDummy between small dir TS and MJ.
   */
  @SuppressWarnings("unchecked")
  private static void insertSHTS(byte tag, TableScanOperator tableScan, MapWork bigMapWork) {
    Preconditions.checkArgument(tableScan.getChildOperators().size() == 1
        && tableScan.getChildOperators().get(0) instanceof MapJoinOperator);
    HashTableDummyDesc desc = new HashTableDummyDesc();
    HashTableDummyOperator dummyOp = (HashTableDummyOperator) OperatorFactory.get(desc);
    dummyOp.getConf().setTbl(tableScan.getTableDesc());
    MapJoinOperator mapJoinOp = (MapJoinOperator) tableScan.getChildOperators().get(0);
    mapJoinOp.replaceParent(tableScan, dummyOp);
    List<Operator<? extends OperatorDesc>> mapJoinChildren =
        new ArrayList<Operator<? extends OperatorDesc>>();
    mapJoinChildren.add(mapJoinOp);
    dummyOp.setChildOperators(mapJoinChildren);
    bigMapWork.addDummyOp(dummyOp);
    MapJoinDesc mjDesc = mapJoinOp.getConf();
    // mapjoin should not be affected by join reordering
    mjDesc.resetOrder();
    SparkHashTableSinkDesc hashTableSinkDesc = new SparkHashTableSinkDesc(mjDesc);
    SparkHashTableSinkOperator hashTableSinkOp =
        (SparkHashTableSinkOperator) OperatorFactory.get(hashTableSinkDesc);
    int[] valueIndex = mjDesc.getValueIndex(tag);
    if (valueIndex != null) {
      List<ExprNodeDesc> newValues = new ArrayList<ExprNodeDesc>();
      List<ExprNodeDesc> values = hashTableSinkDesc.getExprs().get(tag);
      for (int index = 0; index < values.size(); index++) {
        if (valueIndex[index] < 0) {
          newValues.add(values.get(index));
        }
      }
      hashTableSinkDesc.getExprs().put(tag, newValues);
    }
    tableScan.replaceChild(mapJoinOp, hashTableSinkOp);
    List<Operator<? extends OperatorDesc>> tableScanParents =
        new ArrayList<Operator<? extends OperatorDesc>>();
    tableScanParents.add(tableScan);
    hashTableSinkOp.setParentOperators(tableScanParents);
    hashTableSinkOp.setTag(tag);
  }

  private static void setMemUsage(MapJoinOperator mapJoinOp, Task<? extends Serializable> task,
      ParseContext parseContext) {
    MapJoinResolver.LocalMapJoinProcCtx context =
        new MapJoinResolver.LocalMapJoinProcCtx(task, parseContext);
    try {
      new LocalMapJoinProcFactory.LocalMapJoinProcessor().hasGroupBy(mapJoinOp,
          context);
    } catch (Exception e) {
      LOG.warn("Error setting memory usage.", e);
      return;
    }
    MapJoinDesc mapJoinDesc = mapJoinOp.getConf();
    HiveConf conf = context.getParseCtx().getConf();
    float hashtableMemoryUsage;
    if (context.isFollowedByGroupBy()) {
      hashtableMemoryUsage = conf.getFloatVar(
          HiveConf.ConfVars.HIVEHASHTABLEFOLLOWBYGBYMAXMEMORYUSAGE);
    } else {
      hashtableMemoryUsage = conf.getFloatVar(
          HiveConf.ConfVars.HIVEHASHTABLEMAXMEMORYUSAGE);
    }
    mapJoinDesc.setHashTableMemoryUsage(hashtableMemoryUsage);
  }
}
