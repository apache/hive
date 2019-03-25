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
package org.apache.hadoop.hive.ql.exec.repl.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.ReplConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.ReplStateLogWork;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.ReplTxnWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ImportTableDesc;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.ql.util.HiveStrictManagedMigration;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.ql.parse.repl.load.UpdatedMetaDataTracker;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.io.Serializable;

import static org.apache.hadoop.hive.ql.util.HiveStrictManagedMigration.TableMigrationOption.MANAGED;


public class ReplUtils {

  public static final String LAST_REPL_ID_KEY = "hive.repl.last.repl.id";
  public static final String REPL_CHECKPOINT_KEY = "hive.repl.ckpt.key";
  public static final String REPL_FIRST_INC_PENDING_FLAG = "hive.repl.first.inc.pending";

  // write id allocated in the current execution context which will be passed through config to be used by different
  // tasks.
  public static final String REPL_CURRENT_TBL_WRITE_ID = "hive.repl.current.table.write.id";

  // Configuration to be received via WITH clause of REPL LOAD to clean tables from any previously failed
  // bootstrap load.
  public static final String REPL_CLEAN_TABLES_FROM_BOOTSTRAP_CONFIG = "hive.repl.clean.tables.from.bootstrap";

  public static final String FUNCTIONS_ROOT_DIR_NAME = "_functions";
  public static final String CONSTRAINTS_ROOT_DIR_NAME = "_constraints";

  // Root directory for dumping bootstrapped tables along with incremental events dump.
  public static final String INC_BOOTSTRAP_ROOT_DIR_NAME = "_bootstrap";

  // Migrating to transactional tables in bootstrap load phase.
  // It is enough to copy all the original files under base_1 dir and so write-id is hardcoded to 1.
  public static final Long REPL_BOOTSTRAP_MIGRATION_BASE_WRITE_ID = 1L;

  // we keep the statement id as 0 so that the base directory is created with 0 and is easy to find out during
  // duplicate check. Note : Stmt id is not used for base directory now, but to avoid misuse later, its maintained.
  public static final int REPL_BOOTSTRAP_MIGRATION_BASE_STMT_ID = 0;

  /**
   * Bootstrap REPL LOAD operation type on the examined object based on ckpt state.
   */
  public enum ReplLoadOpType {
    LOAD_NEW, LOAD_SKIP, LOAD_REPLACE
  }

  public static Map<Integer, List<ExprNodeGenericFuncDesc>> genPartSpecs(
          Table table, List<Map<String, String>> partitions) throws SemanticException {
    Map<Integer, List<ExprNodeGenericFuncDesc>> partSpecs = new HashMap<>();
    int partPrefixLength = 0;
    if (partitions.size() > 0) {
      partPrefixLength = partitions.get(0).size();
      // pick the length of the first ptn, we expect all ptns listed to have the same number of
      // key-vals.
    }
    List<ExprNodeGenericFuncDesc> partitionDesc = new ArrayList<>();
    for (Map<String, String> ptn : partitions) {
      // convert each key-value-map to appropriate expression.
      ExprNodeGenericFuncDesc expr = null;
      for (Map.Entry<String, String> kvp : ptn.entrySet()) {
        String key = kvp.getKey();
        Object val = kvp.getValue();
        String type = table.getPartColByName(key).getType();
        PrimitiveTypeInfo pti = TypeInfoFactory.getPrimitiveTypeInfo(type);
        ExprNodeColumnDesc column = new ExprNodeColumnDesc(pti, key, null, true);
        ExprNodeGenericFuncDesc op = DDLSemanticAnalyzer.makeBinaryPredicate(
                "=", column, new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, val));
        expr = (expr == null) ? op : DDLSemanticAnalyzer.makeBinaryPredicate("and", expr, op);
      }
      if (expr != null) {
        partitionDesc.add(expr);
      }
    }
    if (partitionDesc.size() > 0) {
      partSpecs.put(partPrefixLength, partitionDesc);
    }
    return partSpecs;
  }

  public static Task<?> getTableReplLogTask(ImportTableDesc tableDesc, ReplLogger replLogger, HiveConf conf)
          throws SemanticException {
    TableType tableType = tableDesc.isExternal() ? TableType.EXTERNAL_TABLE : tableDesc.tableType();
    ReplStateLogWork replLogWork = new ReplStateLogWork(replLogger, tableDesc.getTableName(), tableType);
    return TaskFactory.get(replLogWork, conf);
  }

  public static Task<?> getTableCheckpointTask(ImportTableDesc tableDesc, HashMap<String, String> partSpec,
                                               String dumpRoot, HiveConf conf) throws SemanticException {
    HashMap<String, String> mapProp = new HashMap<>();
    mapProp.put(REPL_CHECKPOINT_KEY, dumpRoot);

    AlterTableDesc alterTblDesc =  new AlterTableDesc(AlterTableDesc.AlterTableTypes.ADDPROPS);
    alterTblDesc.setProps(mapProp);
    alterTblDesc.setOldName(
            StatsUtils.getFullyQualifiedTableName(tableDesc.getDatabaseName(), tableDesc.getTableName()));
    if (partSpec != null) {
      alterTblDesc.setPartSpec(partSpec);
    }
    return TaskFactory.get(new DDLWork(new HashSet<>(), new HashSet<>(), alterTblDesc), conf);
  }

  public static boolean replCkptStatus(String dbName, Map<String, String> props, String dumpRoot)
          throws InvalidOperationException {
    // If ckpt property not set or empty means, bootstrap is not run on this object.
    if ((props != null) && props.containsKey(REPL_CHECKPOINT_KEY) && !props.get(REPL_CHECKPOINT_KEY).isEmpty()) {
      if (props.get(REPL_CHECKPOINT_KEY).equals(dumpRoot)) {
        return true;
      }
      throw new InvalidOperationException(ErrorMsg.REPL_BOOTSTRAP_LOAD_PATH_NOT_VALID.format(dumpRoot,
              props.get(REPL_CHECKPOINT_KEY)));
    }
    return false;
  }

  public static List<Task<? extends Serializable>> addOpenTxnTaskForMigration(String actualDbName,
                                                                  String actualTblName, HiveConf conf,
                                                                  UpdatedMetaDataTracker updatedMetaDataTracker,
                                                                  Task<? extends Serializable> childTask,
                                                                  org.apache.hadoop.hive.metastore.api.Table tableObj)
          throws IOException, TException {
    List<Task<? extends Serializable>> taskList = new ArrayList<>();
    taskList.add(childTask);
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_STRICT_MANAGED_TABLES) && updatedMetaDataTracker != null &&
            !AcidUtils.isTransactionalTable(tableObj) &&
            TableType.valueOf(tableObj.getTableType()) == TableType.MANAGED_TABLE) {
      //TODO : isPathOwnByHive is hard coded to true, need to get it from repl dump metadata.
      HiveStrictManagedMigration.TableMigrationOption migrationOption =
              HiveStrictManagedMigration.determineMigrationTypeAutomatically(tableObj, TableType.MANAGED_TABLE,
                      null, conf, null, true);
      if (migrationOption == MANAGED) {
        //if conversion to managed table.
        Task<? extends Serializable> replTxnTask = TaskFactory.get(new ReplTxnWork(actualDbName, actualTblName,
                        ReplTxnWork.OperationType.REPL_MIGRATION_OPEN_TXN), conf);
        replTxnTask.addDependentTask(childTask);
        updatedMetaDataTracker.setNeedCommitTxn(true);
        taskList.add(replTxnTask);
      }
    }
    return taskList;
  }

  // Path filters to filter only events (directories) excluding "_bootstrap"
  public static PathFilter getEventsDirectoryFilter(final FileSystem fs) {
    return p -> {
      try {
        return fs.isDirectory(p) && !p.getName().equalsIgnoreCase(ReplUtils.INC_BOOTSTRAP_ROOT_DIR_NAME);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
  }

  public static boolean isFirstIncPending(Map<String, String> parameters) {
    if (parameters == null) {
      return false;
    }
    String firstIncPendFlag = parameters.get(ReplUtils.REPL_FIRST_INC_PENDING_FLAG);
    // If flag is not set, then we assume first incremental load is done as the database/table may be created by user
    // and not through replication.
    return firstIncPendFlag != null && !firstIncPendFlag.isEmpty() && "true".equalsIgnoreCase(firstIncPendFlag);
  }

  public static EnvironmentContext setReplDataLocationChangedFlag(EnvironmentContext envContext) {
    if (envContext == null) {
      envContext = new EnvironmentContext();
    }
    envContext.putToProperties(ReplConst.REPL_DATA_LOCATION_CHANGED, ReplConst.TRUE);
    return envContext;
  }
}
