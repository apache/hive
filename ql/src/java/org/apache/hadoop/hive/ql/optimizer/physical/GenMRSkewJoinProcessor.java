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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverSkewJoin;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
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
  public static void processSkewJoin(JoinOperator joinOp,
      Task<? extends Serializable> currTask, ParseContext parseCtx)
      throws SemanticException {

    // We are trying to adding map joins to handle skew keys, and map join right
    // now does not work with outer joins
    if (!GenMRSkewJoinProcessor.skewJoinEnabled(parseCtx.getConf(), joinOp)) {
      return;
    }

    String baseTmpDir = parseCtx.getContext().getMRTmpFileURI();

    JoinDesc joinDescriptor = joinOp.getConf();
    Map<Byte, List<ExprNodeDesc>> joinValues = joinDescriptor.getExprs();
    int numAliases = joinValues.size();

    Map<Byte, String> bigKeysDirMap = new HashMap<Byte, String>();
    Map<Byte, Map<Byte, String>> smallKeysDirMap = new HashMap<Byte, Map<Byte, String>>();
    Map<Byte, String> skewJoinJobResultsDir = new HashMap<Byte, String>();
    Byte[] tags = joinDescriptor.getTagOrder();
    for (int i = 0; i < numAliases; i++) {
      Byte alias = tags[i];
      String bigKeysDir = getBigKeysDir(baseTmpDir, alias);
      bigKeysDirMap.put(alias, bigKeysDir);
      Map<Byte, String> smallKeysMap = new HashMap<Byte, String>();
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

    HashMap<String, Task<? extends Serializable>> bigKeysDirToTaskMap =
      new HashMap<String, Task<? extends Serializable>>();
    List<Serializable> listWorks = new ArrayList<Serializable>();
    List<Task<? extends Serializable>> listTasks = new ArrayList<Task<? extends Serializable>>();
    MapredWork currPlan = (MapredWork) currTask.getWork();

    TableDesc keyTblDesc = (TableDesc) currPlan.getKeyDesc().clone();
    List<String> joinKeys = Utilities
        .getColumnNames(keyTblDesc.getProperties());
    List<String> joinKeyTypes = Utilities.getColumnTypes(keyTblDesc
        .getProperties());

    Map<Byte, TableDesc> tableDescList = new HashMap<Byte, TableDesc>();
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

      boolean first = true;
      for (int k = 0; k < columnSize; k++) {
        TypeInfo type = valueCols.get(k).getTypeInfo();
        String newColName = i + "_VALUE_" + k; // any name, it does not matter.
        newValueExpr
            .add(new ExprNodeColumnDesc(type, newColName, "" + i, false));
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
        newKeyExpr.add(new ExprNodeColumnDesc(TypeInfoFactory
            .getPrimitiveTypeInfo(joinKeyTypes.get(k)), joinKeys.get(k),
            "" + i, false));
      }

      newJoinValues.put(alias, newValueExpr);
      newJoinKeys.put(alias, newKeyExpr);
      tableDescList.put(alias, Utilities.getTableDesc(colNames, colTypes));

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
      MapredWork newPlan = PlanUtils.getMapRedWork();
      MapredWork clonePlan = null;
      try {
        String xmlPlan = currPlan.toXML();
        StringBuilder sb = new StringBuilder(xmlPlan);
        ByteArrayInputStream bis;
        bis = new ByteArrayInputStream(sb.toString().getBytes("UTF-8"));
        clonePlan = Utilities.deserializeMapRedWork(bis, parseCtx.getConf());
      } catch (UnsupportedEncodingException e) {
        throw new SemanticException(e);
      }

      Operator<? extends Serializable>[] parentOps = new TableScanOperator[tags.length];
      for (int k = 0; k < tags.length; k++) {
        Operator<? extends Serializable> ts = OperatorFactory.get(
            TableScanDesc.class, (RowSchema) null);
        parentOps[k] = ts;
      }
      Operator<? extends Serializable> tblScan_op = parentOps[i];

      ArrayList<String> aliases = new ArrayList<String>();
      String alias = src.toString();
      aliases.add(alias);
      String bigKeyDirPath = bigKeysDirMap.get(src);
      newPlan.getPathToAliases().put(bigKeyDirPath, aliases);
      newPlan.getAliasToWork().put(alias, tblScan_op);
      PartitionDesc part = new PartitionDesc(tableDescList.get(src), null);
      newPlan.getPathToPartitionInfo().put(bigKeyDirPath, part);
      newPlan.getAliasToPartnInfo().put(alias, part);

      Operator<? extends Serializable> reducer = clonePlan.getReducer();
      assert reducer instanceof JoinOperator;
      JoinOperator cloneJoinOp = (JoinOperator) reducer;

      MapJoinDesc mapJoinDescriptor = new MapJoinDesc(newJoinKeys, keyTblDesc,
          newJoinValues, newJoinValueTblDesc, joinDescriptor
          .getOutputColumnNames(), i, joinDescriptor.getConds());
      mapJoinDescriptor.setNoOuterJoin(joinDescriptor.isNoOuterJoin());
      mapJoinDescriptor.setTagOrder(tags);
      mapJoinDescriptor.setHandleSkewJoin(false);

      MapredLocalWork localPlan = new MapredLocalWork(
          new LinkedHashMap<String, Operator<? extends Serializable>>(),
          new LinkedHashMap<String, FetchWork>());
      Map<Byte, String> smallTblDirs = smallKeysDirMap.get(src);

      for (int j = 0; j < numAliases; j++) {
        if (j == i) {
          continue;
        }
        Byte small_alias = tags[j];
        Operator<? extends Serializable> tblScan_op2 = parentOps[j];
        localPlan.getAliasToWork().put(small_alias.toString(), tblScan_op2);
        Path tblDir = new Path(smallTblDirs.get(small_alias));
        localPlan.getAliasToFetchWork().put(small_alias.toString(),
            new FetchWork(tblDir.toString(), tableDescList.get(small_alias)));
      }

      newPlan.setMapLocalWork(localPlan);

      // construct a map join and set it as the child operator of tblScan_op
      MapJoinOperator mapJoinOp = (MapJoinOperator) OperatorFactory
          .getAndMakeChild(mapJoinDescriptor, (RowSchema) null, parentOps);
      // change the children of the original join operator to point to the map
      // join operator
      List<Operator<? extends Serializable>> childOps = cloneJoinOp
          .getChildOperators();
      for (Operator<? extends Serializable> childOp : childOps) {
        childOp.replaceParent(cloneJoinOp, mapJoinOp);
      }
      mapJoinOp.setChildOperators(childOps);

      HiveConf jc = new HiveConf(parseCtx.getConf(),
          GenMRSkewJoinProcessor.class);

      newPlan.setNumMapTasks(HiveConf
          .getIntVar(jc, HiveConf.ConfVars.HIVESKEWJOINMAPJOINNUMMAPTASK));
      newPlan
          .setMinSplitSize(HiveConf.getIntVar(jc, HiveConf.ConfVars.HIVESKEWJOINMAPJOINMINSPLIT));
      newPlan.setInputformat(HiveInputFormat.class.getName());
      Task<? extends Serializable> skewJoinMapJoinTask = TaskFactory.get(
          newPlan, jc);
      bigKeysDirToTaskMap.put(bigKeyDirPath, skewJoinMapJoinTask);
      listWorks.add(skewJoinMapJoinTask.getWork());
      listTasks.add(skewJoinMapJoinTask);
    }

    ConditionalWork cndWork = new ConditionalWork(listWorks);
    ConditionalTask cndTsk = (ConditionalTask) TaskFactory.get(cndWork,
        parseCtx.getConf());
    cndTsk.setListTasks(listTasks);
    cndTsk.setResolver(new ConditionalResolverSkewJoin());
    cndTsk
        .setResolverCtx(new ConditionalResolverSkewJoin.ConditionalResolverSkewJoinCtx(
        bigKeysDirToTaskMap));
    List<Task<? extends Serializable>> oldChildTasks = currTask.getChildTasks();
    currTask.setChildTasks(new ArrayList<Task<? extends Serializable>>());
    currTask.addDependentTask(cndTsk);

    if (oldChildTasks != null) {
      for (Task<? extends Serializable> tsk : cndTsk.getListTasks()) {
        for (Task<? extends Serializable> oldChild : oldChildTasks) {
          tsk.addDependentTask(oldChild);
        }
      }
    }
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

  private static String skewJoinPrefix = "hive_skew_join";
  private static String UNDERLINE = "_";
  private static String BIGKEYS = "bigkeys";
  private static String SMALLKEYS = "smallkeys";
  private static String RESULTS = "results";

  static String getBigKeysDir(String baseDir, Byte srcTbl) {
    return baseDir + File.separator + skewJoinPrefix + UNDERLINE + BIGKEYS
        + UNDERLINE + srcTbl;
  }

  static String getBigKeysSkewJoinResultDir(String baseDir, Byte srcTbl) {
    return baseDir + File.separator + skewJoinPrefix + UNDERLINE + BIGKEYS
        + UNDERLINE + RESULTS + UNDERLINE + srcTbl;
  }

  static String getSmallKeysDir(String baseDir, Byte srcTblBigTbl,
      Byte srcTblSmallTbl) {
    return baseDir + File.separator + skewJoinPrefix + UNDERLINE + SMALLKEYS
        + UNDERLINE + srcTblBigTbl + UNDERLINE + srcTblSmallTbl;
  }

}
