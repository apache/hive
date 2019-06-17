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

package org.apache.hadoop.hive.ql.exec;

import java.io.DataOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.DefaultHiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.Msck;
import org.apache.hadoop.hive.metastore.MsckInfo;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.plan.CacheMetadataDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.InsertCommitHookDesc;
import org.apache.hadoop.hive.ql.plan.MsckDesc;
import org.apache.hadoop.hive.ql.plan.ReplRemoveFirstIncLoadPendFlagDesc;
import org.apache.hadoop.hive.ql.plan.ShowConfDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hive.common.util.ReflectionUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DDLTask implementation.
 *
 **/
public class DDLTask extends Task<DDLWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger("hive.ql.exec.DDLTask");

  private static final int separator = Utilities.tabCode;
  private static final int terminator = Utilities.newLineCode;

  @Override
  public boolean requireLock() {
    return this.work != null && this.work.getNeedLock();
  }

  public DDLTask() {
    super();
  }

  @Override
  public int execute(DriverContext driverContext) {
    if (driverContext.getCtx().getExplainAnalyze() == AnalyzeState.RUNNING) {
      return 0;
    }

    // Create the db
    Hive db;
    try {
      db = Hive.get(conf);

      MsckDesc msckDesc = work.getMsckDesc();
      if (msckDesc != null) {
        return msck(db, msckDesc);
      }

      ShowConfDesc showConf = work.getShowConfDesc();
      if (showConf != null) {
        return showConf(db, showConf);
      }

      CacheMetadataDesc cacheMetadataDesc = work.getCacheMetadataDesc();
      if (cacheMetadataDesc != null) {
        return cacheMetadata(db, cacheMetadataDesc);
      }
      InsertCommitHookDesc insertCommitHookDesc = work.getInsertCommitHookDesc();
      if (insertCommitHookDesc != null) {
        return insertCommitWork(db, insertCommitHookDesc);
      }

      if (work.getReplSetFirstIncLoadFlagDesc() != null) {
        return remFirstIncPendFlag(db, work.getReplSetFirstIncLoadFlagDesc());
      }
    } catch (Throwable e) {
      failed(e);
      return 1;
    }
    assert false;
    return 0;
  }

  private int insertCommitWork(Hive db, InsertCommitHookDesc insertCommitHookDesc) throws MetaException {
    boolean failed = true;
    HiveMetaHook hook = insertCommitHookDesc.getTable().getStorageHandler().getMetaHook();
    if (hook == null || !(hook instanceof DefaultHiveMetaHook)) {
      return 0;
    }
    DefaultHiveMetaHook hiveMetaHook = (DefaultHiveMetaHook) hook;
    try {
      hiveMetaHook.commitInsertTable(insertCommitHookDesc.getTable().getTTable(),
              insertCommitHookDesc.isOverwrite()
      );
      failed = false;
    } finally {
      if (failed) {
        hiveMetaHook.rollbackInsertTable(insertCommitHookDesc.getTable().getTTable(),
                insertCommitHookDesc.isOverwrite()
        );
      }
    }
    return 0;
  }

  private int cacheMetadata(Hive db, CacheMetadataDesc desc) throws HiveException {
    db.cacheFileMetadata(desc.getDbName(), desc.getTableName(),
        desc.getPartName(), desc.isAllParts());
    return 0;
  }

  private void failed(Throwable e) {
    while (e.getCause() != null && e.getClass() == RuntimeException.class) {
      e = e.getCause();
    }
    setException(e);
    LOG.error("Failed", e);
  }

  private int showConf(Hive db, ShowConfDesc showConf) throws Exception {
    ConfVars conf = HiveConf.getConfVars(showConf.getConfName());
    if (conf == null) {
      throw new HiveException("invalid configuration name " + showConf.getConfName());
    }
    String description = conf.getDescription();
    String defaultValue = conf.getDefaultValue();
    DataOutputStream output = getOutputStream(showConf.getResFile());
    try {
      if (defaultValue != null) {
        output.write(defaultValue.getBytes());
      }
      output.write(separator);
      output.write(conf.typeString().getBytes());
      output.write(separator);
      if (description != null) {
        output.write(description.replaceAll(" *\n *", " ").getBytes());
      }
      output.write(terminator);
    } finally {
      output.close();
    }
    return 0;
  }

  private DataOutputStream getOutputStream(Path outputFile) throws HiveException {
    try {
      FileSystem fs = outputFile.getFileSystem(conf);
      return fs.create(outputFile);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * MetastoreCheck, see if the data in the metastore matches what is on the
   * dfs. Current version checks for tables and partitions that are either
   * missing on disk on in the metastore.
   *
   * @param db
   *          The database in question.
   * @param msckDesc
   *          Information about the tables and partitions we want to check for.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   */
  private int msck(Hive db, MsckDesc msckDesc) {
    Msck msck;
    try {
      msck = new Msck( false, false);
      msck.init(db.getConf());
      String[] names = Utilities.getDbTableName(msckDesc.getTableName());
      MsckInfo msckInfo = new MsckInfo(SessionState.get().getCurrentCatalog(), names[0],
        names[1], msckDesc.getPartSpecs(), msckDesc.getResFile(),
        msckDesc.isRepairPartitions(), msckDesc.isAddPartitions(), msckDesc.isDropPartitions(), -1);
      return msck.repair(msckInfo);
    } catch (MetaException e) {
      LOG.error("Unable to create msck instance.", e);
      return 1;
    } catch (SemanticException e) {
      LOG.error("Msck failed.", e);
      return 1;
    }
  }

  /**
   * There are many places where "duplicate" Read/WriteEnity objects are added.  The way this was
   * initially implemented, the duplicate just replaced the previous object.
   * (work.getOutputs() is a Set and WriteEntity#equals() relies on name)
   * This may be benign for ReadEntity and perhaps was benign for WriteEntity before WriteType was
   * added. Now that WriteEntity has a WriteType it replaces it with one with possibly different
   * {@link org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType}.  It's hard to imagine
   * how this is desirable.
   *
   * As of HIVE-14993, WriteEntity with different WriteType must be considered different.
   * So WriteEntity created in DDLTask cause extra output in golden files, but only because
   * DDLTask sets a different WriteType for the same Entity.
   *
   * In the spirit of bug-for-bug compatibility, this method ensures we only add new
   * WriteEntity if it's really new.
   *
   * @return {@code true} if item was added
   */
  static boolean addIfAbsentByName(WriteEntity newWriteEntity, Set<WriteEntity> outputs) {
    for(WriteEntity writeEntity : outputs) {
      if(writeEntity.getName().equalsIgnoreCase(newWriteEntity.getName())) {
        LOG.debug("Ignoring request to add {} because {} is present",
          newWriteEntity.toStringDetail(), writeEntity.toStringDetail());
        return false;
      }
    }
    outputs.add(newWriteEntity);
    return true;
  }

  /**
   * Check if the given serde is valid.
   */
  public static void validateSerDe(String serdeName, HiveConf conf) throws HiveException {
    try {

      Deserializer d = ReflectionUtil.newInstance(conf.getClassByName(serdeName).
          asSubclass(Deserializer.class), conf);
      if (d != null) {
        LOG.debug("Found class for {}", serdeName);
      }
    } catch (Exception e) {
      throw new HiveException("Cannot validate serde: " + serdeName, e);
    }
  }

  @Override
  public StageType getType() {
    return StageType.DDL;
  }

  @Override
  public String getName() {
    return "DDL";
  }

  private int remFirstIncPendFlag(Hive hive, ReplRemoveFirstIncLoadPendFlagDesc desc) throws HiveException, TException {
    String dbNameOrPattern = desc.getDatabaseName();
    Map<String, String> parameters;

    // Flag is set only in database for db level load.
    for (String dbName : Utils.matchesDb(hive, dbNameOrPattern)) {
      Database database = hive.getMSC().getDatabase(dbName);
      parameters = database.getParameters();
      String incPendPara = parameters != null ? parameters.get(ReplUtils.REPL_FIRST_INC_PENDING_FLAG) : null;
      if (incPendPara != null) {
        parameters.remove(ReplUtils.REPL_FIRST_INC_PENDING_FLAG);
        hive.getMSC().alterDatabase(dbName, database);
      }
    }
    return 0;
  }

  /*
  uses the authorizer from SessionState will need some more work to get this to run in parallel,
  however this should not be a bottle neck so might not need to parallelize this.
   */
  @Override
  public boolean canExecuteInParallel() {
    return false;
  }
}
