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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverSkewJoin;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverSkewJoin.ConditionalResolverSkewJoinCtx;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * GenMRSkewJoinProcessor.
 *
 */
public final class GenMRSkewJoinProcessor {

  private GenMRSkewJoinProcessor() {
    // prevent instantiation
  }

  /**
   * Create tasks for processing skew joins. The idea is (HIVE-964) to use
   * separated jobs and map-joins to handle skew joins.
   * <p>
   * <ul>
   * <li>
   * Number of mr jobs to handle skew keys is the number of table minus 1 (we
   * can stream the last table, so big keys in the last table will not be a
   * problem).
   * <li>
   * At runtime in Join, we output big keys in one table into one corresponding
   * directories, and all same keys in other tables into different dirs(one for
   * each table). The directories will look like:
   * <ul>
   * <li>
   * dir-T1-bigkeys(containing big keys in T1), dir-T2-keys(containing keys
   * which is big in T1),dir-T3-keys(containing keys which is big in T1), ...
   * <li>
   * dir-T1-keys(containing keys which is big in T2), dir-T2-bigkeys(containing
   * big keys in T2),dir-T3-keys(containing keys which is big in T2), ...
   * <li>
   * dir-T1-keys(containing keys which is big in T3), dir-T2-keys(containing big
   * keys in T3),dir-T3-bigkeys(containing keys which is big in T3), ... .....
   * </ul>
   * </ul>
   * For each table, we launch one mapjoin job, taking the directory containing
   * big keys in this table and corresponding dirs in other tables as input.
   * (Actally one job for one row in the above.)
   *
   * <p>
   * For more discussions, please check
   * https://issues.apache.org/jira/browse/HIVE-964.
   *
   */
  @SuppressWarnings("unchecked")
  public static void processSkewJoin(JoinOperator joinOp,
      Task<? extends Serializable> currTask, ParseContext parseCtx)
      throws SemanticException {

    // We are trying to adding map joins to handle skew keys, and map join right
    // now does not work with outer joins
    if (!GenMRSkewJoinProcessor.skewJoinEnabled(parseCtx.getConf(), joinOp)) {
      return;
    }

    List<Task<? extends Serializable>> children = currTask.getChildTasks();

    Path baseTmpDir = parseCtx.getContext().getMRTmpPath();

    JoinDesc joinDescriptor = joinOp.getConf();
    Map<Byte, List<ExprNodeDesc>> joinValues = joinDescriptor.getExprs();
    int numAliases = joinValues.size();

    Map<Byte, Path> bigKeysDirMap = new HashMap<Byte, Path>();
    Map<Byte, Map<Byte, Path>> smallKeysDirMap = new HashMap<Byte, Map<Byte, Path>>();
    Map<Byte, Path> skewJoinJobResultsDir = new HashMap<Byte, Path>();
    Byte[] tags = joinDescriptor.getTagOrder();
    for (int i = 0; i < numAliases; i++) {
      Byte alias = tags[i];
      bigKeysDirMap.put(alias, getBigKeysDir(baseTmpDir, alias));
      Map<Byte, Path> smallKeysMap = new HashMap<Byte, Path>();
      smallKeysDirMap.put(alias, smallKeysMap);
      for (Byte src2 : tags) {
        if (!src2.equals(alias)) {
          smallKeysMap.put(src2, getSmallKeysDir(baseTmpDir, alias, src2));
        }
      }
      skewJoinJobResultsDir.put(alias, getBigKeysSkewJoinResultDir(baseTmpDir,
          alias));
    }

    joinDescriptor.setHandleSkewJoin(true);
    joinDescriptor.setBigKeysDirMap(bigKeysDirMap);
    joinDescriptor.setSmallKeysDirMap(smallKeysDirMap);
    joinDescriptor.setSkewKeyDefinition(HiveConf.getIntVar(parseCtx.getConf(),
        HiveConf.ConfVars.HIVESKEWJOINKEY));

    HashMap<Path, Task<? extends Serializable>> bigKeysDirToTaskMap =
      new HashMap<Path, Task<? extends Serializable>>();
    List<Serializable> listWorks = new ArrayList<Serializable>();
    List<Task<? extends Serializable>> listTasks = new ArrayList<Task<? extends Serializable>>();
    MapredWork currPlan = (MapredWork) currTask.getWork();

    TableDesc keyTblDesc = (TableDesc) currPlan.getReduceWork().getKeyDesc().clone();
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

    for (Byte tag : tags) {
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
        newValueExpr.add(new ExprNodeColumnDesc(columnInfo));
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
        newKeyExpr.add(new ExprNodeColumnDesc(columnInfo));
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
      newJoinValueTblDesc.set(Byte.valueOf((byte) i), Utilities.getTableDesc(
          valueColNames, valueColTypes));
    }

    joinDescriptor.setSkewKeysValuesTables(tableDescList);
    joinDescriptor.setKeyTableDesc(keyTblDesc);

    for (int i = 0; i < numAliases - 1; i++) {
      Byte src = tags[i];
      MapWork newPlan = PlanUtils.getMapRedWork().getMapWork();

      // This code has been only added for testing
      boolean mapperCannotSpanPartns =
        parseCtx.getConf().getBoolVar(
          HiveConf.ConfVars.HIVE_MAPPER_CANNOT_SPAN_MULTIPLE_PARTITIONS);
      newPlan.setMapperCannotSpanPartns(mapperCannotSpanPartns);

      MapredWork clonePlan = SerializationUtilities.clonePlan(currPlan);

      Operator<? extends OperatorDesc>[] parentOps = new TableScanOperator[tags.length];
      for (int k = 0; k < tags.length; k++) {
        Operator<? extends OperatorDesc> ts =
            GenMapRedUtils.createTemporaryTableScanOperator(
                joinOp.getCompilationOpContext(), rowSchemaList.get((byte)k));
        ((TableScanOperator)ts).setTableDesc(tableDescList.get((byte)k));
        parentOps[k] = ts;
      }
      Operator<? extends OperatorDesc> tblScan_op = parentOps[i];

      ArrayList<String> aliases = new ArrayList<String>();
      String alias = src.toString().intern();
      aliases.add(alias);
      Path bigKeyDirPath = bigKeysDirMap.get(src);
      newPlan.addPathToAlias(bigKeyDirPath, aliases);

      newPlan.getAliasToWork().put(alias, tblScan_op);
      PartitionDesc part = new PartitionDesc(tableDescList.get(src), null);

      newPlan.addPathToPartitionInfo(bigKeyDirPath, part);
      newPlan.getAliasToPartnInfo().put(alias, part);

      Operator<? extends OperatorDesc> reducer = clonePlan.getReduceWork().getReducer();
      assert reducer instanceof JoinOperator;
      JoinOperator cloneJoinOp = (JoinOperator) reducer;

      String dumpFilePrefix = "mapfile"+PlanUtils.getCountForMapJoinDumpFilePrefix();
      MapJoinDesc mapJoinDescriptor = new MapJoinDesc(newJoinKeys, keyTblDesc,
          newJoinValues, newJoinValueTblDesc, newJoinValueTblDesc,joinDescriptor
          .getOutputColumnNames(), i, joinDescriptor.getConds(),
          joinDescriptor.getFilters(), joinDescriptor.getNoOuterJoin(), dumpFilePrefix);
      mapJoinDescriptor.setTagOrder(tags);
      mapJoinDescriptor.setHandleSkewJoin(false);
      mapJoinDescriptor.setNullSafes(joinDescriptor.getNullSafes());

      MapredLocalWork localPlan = new MapredLocalWork(
          new LinkedHashMap<String, Operator<? extends OperatorDesc>>(),
          new LinkedHashMap<String, FetchWork>());
      Map<Byte, Path> smallTblDirs = smallKeysDirMap.get(src);

      for (int j = 0; j < numAliases; j++) {
        if (j == i) {
          continue;
        }
        Byte small_alias = tags[j];
        Operator<? extends OperatorDesc> tblScan_op2 = parentOps[j];
        localPlan.getAliasToWork().put(small_alias.toString(), tblScan_op2);
        Path tblDir = smallTblDirs.get(small_alias);
        localPlan.getAliasToFetchWork().put(small_alias.toString(),
            new FetchWork(tblDir, tableDescList.get(small_alias)));
      }

      newPlan.setMapRedLocalWork(localPlan);

      // construct a map join and set it as the child operator of tblScan_op
      MapJoinOperator mapJoinOp = (MapJoinOperator) OperatorFactory.getAndMakeChild(
          joinOp.getCompilationOpContext(), mapJoinDescriptor, (RowSchema) null, parentOps);
      // change the children of the original join operator to point to the map
      // join operator
      List<Operator<? extends OperatorDesc>> childOps = cloneJoinOp
          .getChildOperators();
      for (Operator<? extends OperatorDesc> childOp : childOps) {
        childOp.replaceParent(cloneJoinOp, mapJoinOp);
      }
      mapJoinOp.setChildOperators(childOps);

      HiveConf jc = new HiveConf(parseCtx.getConf(),
          GenMRSkewJoinProcessor.class);

      newPlan.setNumMapTasks(HiveConf
          .getIntVar(jc, HiveConf.ConfVars.HIVESKEWJOINMAPJOINNUMMAPTASK));
      newPlan
          .setMinSplitSize(HiveConf.getLongVar(jc, HiveConf.ConfVars.HIVESKEWJOINMAPJOINMINSPLIT));
      newPlan.setInputformat(HiveInputFormat.class.getName());

      MapredWork w = new MapredWork();
      w.setMapWork(newPlan);

      Task<? extends Serializable> skewJoinMapJoinTask = TaskFactory.get(w, jc);
      skewJoinMapJoinTask.setFetchSource(currTask.isFetchSource());

      bigKeysDirToTaskMap.put(bigKeyDirPath, skewJoinMapJoinTask);
      listWorks.add(skewJoinMapJoinTask.getWork());
      listTasks.add(skewJoinMapJoinTask);
    }
    if (children != null) {
      for (Task<? extends Serializable> tsk : listTasks) {
        for (Task<? extends Serializable> oldChild : children) {
          tsk.addDependentTask(oldChild);
        }
      }
      currTask.setChildTasks(new ArrayList<Task<? extends Serializable>>());
      for (Task<? extends Serializable> oldChild : children) {
        oldChild.getParentTasks().remove(currTask);
      }
      listTasks.addAll(children);
    }
    ConditionalResolverSkewJoinCtx context =
        new ConditionalResolverSkewJoinCtx(bigKeysDirToTaskMap, children);

    ConditionalWork cndWork = new ConditionalWork(listWorks);
    ConditionalTask cndTsk = (ConditionalTask) TaskFactory.get(cndWork, parseCtx.getConf());
    cndTsk.setListTasks(listTasks);
    cndTsk.setResolver(new ConditionalResolverSkewJoin());
    cndTsk.setResolverCtx(context);
    currTask.setChildTasks(new ArrayList<Task<? extends Serializable>>());
    currTask.addDependentTask(cndTsk);

    return;
  }

  public static boolean skewJoinEnabled(HiveConf conf, JoinOperator joinOp) {

    if (conf != null && !conf.getBoolVar(HiveConf.ConfVars.HIVESKEWJOIN)) {
      return false;
    }

    if (!joinOp.getConf().isNoOuterJoin()) {
      return false;
    }

    byte pos = 0;
    for (Byte tag : joinOp.getConf().getTagOrder()) {
      if (tag != pos) {
        return false;
      }
      pos++;
    }

    return true;
  }

  private static final String skewJoinPrefix = "hive_skew_join";
  private static final String UNDERLINE = "_";
  private static final String BIGKEYS = "bigkeys";
  private static final String SMALLKEYS = "smallkeys";
  private static final String RESULTS = "results";

  static Path getBigKeysDir(Path baseDir, Byte srcTbl) {
    return StringInternUtils.internUriStringsInPath(
        new Path(baseDir, skewJoinPrefix + UNDERLINE + BIGKEYS + UNDERLINE + srcTbl));
  }

  static Path getBigKeysSkewJoinResultDir(Path baseDir, Byte srcTbl) {
    return StringInternUtils.internUriStringsInPath(
        new Path(baseDir, skewJoinPrefix + UNDERLINE + BIGKEYS
        + UNDERLINE + RESULTS + UNDERLINE + srcTbl));
  }

  static Path getSmallKeysDir(Path baseDir, Byte srcTblBigTbl,
      Byte srcTblSmallTbl) {
    return StringInternUtils.internUriStringsInPath(
        new Path(baseDir, skewJoinPrefix + UNDERLINE + SMALLKEYS
        + UNDERLINE + srcTblBigTbl + UNDERLINE + srcTblSmallTbl));
  }

}
