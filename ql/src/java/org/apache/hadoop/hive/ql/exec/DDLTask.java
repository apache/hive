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

package org.apache.hadoop.hive.ql.exec;

import static org.apache.commons.lang.StringUtils.join;
import static org.apache.hadoop.util.StringUtils.stringifyException;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.collect.Iterables;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.ProtectMode;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.TxnInfo;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ArchiveUtils.PartSpecInfo;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.merge.MergeFileTask;
import org.apache.hadoop.hive.ql.io.merge.MergeFileWork;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.ql.io.rcfile.truncate.ColumnTruncateTask;
import org.apache.hadoop.hive.ql.io.rcfile.truncate.ColumnTruncateWork;
import org.apache.hadoop.hive.ql.lockmgr.DbLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockMode;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject.HiveLockObjectData;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.CheckResult;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveMetaStoreChecker;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PartitionIterable;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatter;
import org.apache.hadoop.hive.ql.parse.AlterTablePartMergeFilesDesc;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.AlterDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.AlterIndexDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableAlterPartDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes;
import org.apache.hadoop.hive.ql.plan.AlterTableExchangePartition;
import org.apache.hadoop.hive.ql.plan.AlterTableSimpleDesc;
import org.apache.hadoop.hive.ql.plan.CreateDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.CreateIndexDesc;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.plan.CreateTableLikeDesc;
import org.apache.hadoop.hive.ql.plan.CreateViewDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DescDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.DescFunctionDesc;
import org.apache.hadoop.hive.ql.plan.DescTableDesc;
import org.apache.hadoop.hive.ql.plan.DropDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.DropIndexDesc;
import org.apache.hadoop.hive.ql.plan.DropTableDesc;
import org.apache.hadoop.hive.ql.plan.FileMergeDesc;
import org.apache.hadoop.hive.ql.plan.GrantDesc;
import org.apache.hadoop.hive.ql.plan.GrantRevokeRoleDDL;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.LockDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.LockTableDesc;
import org.apache.hadoop.hive.ql.plan.MsckDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.OrcFileMergeDesc;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.plan.RCFileMergeDesc;
import org.apache.hadoop.hive.ql.plan.RenamePartitionDesc;
import org.apache.hadoop.hive.ql.plan.RevokeDesc;
import org.apache.hadoop.hive.ql.plan.RoleDDLDesc;
import org.apache.hadoop.hive.ql.plan.ShowColumnsDesc;
import org.apache.hadoop.hive.ql.plan.ShowCompactionsDesc;
import org.apache.hadoop.hive.ql.plan.ShowConfDesc;
import org.apache.hadoop.hive.ql.plan.ShowCreateTableDesc;
import org.apache.hadoop.hive.ql.plan.ShowDatabasesDesc;
import org.apache.hadoop.hive.ql.plan.ShowFunctionsDesc;
import org.apache.hadoop.hive.ql.plan.ShowGrantDesc;
import org.apache.hadoop.hive.ql.plan.ShowIndexesDesc;
import org.apache.hadoop.hive.ql.plan.ShowLocksDesc;
import org.apache.hadoop.hive.ql.plan.ShowPartitionsDesc;
import org.apache.hadoop.hive.ql.plan.ShowTableStatusDesc;
import org.apache.hadoop.hive.ql.plan.ShowTablesDesc;
import org.apache.hadoop.hive.ql.plan.ShowTblPropertiesDesc;
import org.apache.hadoop.hive.ql.plan.ShowTxnsDesc;
import org.apache.hadoop.hive.ql.plan.SwitchDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.TruncateTableDesc;
import org.apache.hadoop.hive.ql.plan.UnlockDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.UnlockTableDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveV1Authorizer;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatus;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.common.util.AnnotationUtils;
import org.apache.hive.common.util.ReflectionUtil;
import org.stringtemplate.v4.ST;

/**
 * DDLTask implementation.
 *
 **/
public class DDLTask extends Task<DDLWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog("hive.ql.exec.DDLTask");

  private static final int separator = Utilities.tabCode;
  private static final int terminator = Utilities.newLineCode;

  // These are suffixes attached to intermediate directory names used in the
  // archiving / un-archiving process.
  private static String INTERMEDIATE_ARCHIVED_DIR_SUFFIX;
  private static String INTERMEDIATE_ORIGINAL_DIR_SUFFIX;
  private static String INTERMEDIATE_EXTRACTED_DIR_SUFFIX;

  private MetaDataFormatter formatter;

  @Override
  public boolean requireLock() {
    return this.work != null && this.work.getNeedLock();
  }

  public DDLTask() {
    super();
  }

  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan, DriverContext ctx) {
    super.initialize(conf, queryPlan, ctx);

    // Pick the formatter to use to display the results.  Either the
    // normal human readable output or a json object.
    formatter = MetaDataFormatUtils.getFormatter(conf);
    INTERMEDIATE_ARCHIVED_DIR_SUFFIX =
        HiveConf.getVar(conf, ConfVars.METASTORE_INT_ARCHIVED);
    INTERMEDIATE_ORIGINAL_DIR_SUFFIX =
        HiveConf.getVar(conf, ConfVars.METASTORE_INT_ORIGINAL);
    INTERMEDIATE_EXTRACTED_DIR_SUFFIX =
        HiveConf.getVar(conf, ConfVars.METASTORE_INT_EXTRACTED);
  }

  @Override
  public int execute(DriverContext driverContext) {

    // Create the db
    Hive db;
    try {
      db = Hive.get(conf);

      CreateDatabaseDesc createDatabaseDesc = work.getCreateDatabaseDesc();
      if (null != createDatabaseDesc) {
        return createDatabase(db, createDatabaseDesc);
      }

      DropDatabaseDesc dropDatabaseDesc = work.getDropDatabaseDesc();
      if (dropDatabaseDesc != null) {
        return dropDatabase(db, dropDatabaseDesc);
      }

      LockDatabaseDesc lockDatabaseDesc = work.getLockDatabaseDesc();
      if (lockDatabaseDesc != null) {
        return lockDatabase(lockDatabaseDesc);
      }

      UnlockDatabaseDesc unlockDatabaseDesc = work.getUnlockDatabaseDesc();
      if (unlockDatabaseDesc != null) {
        return unlockDatabase(unlockDatabaseDesc);
      }

      SwitchDatabaseDesc switchDatabaseDesc = work.getSwitchDatabaseDesc();
      if (switchDatabaseDesc != null) {
        return switchDatabase(db, switchDatabaseDesc);
      }

      DescDatabaseDesc descDatabaseDesc = work.getDescDatabaseDesc();
      if (descDatabaseDesc != null) {
        return descDatabase(descDatabaseDesc);
      }

      AlterDatabaseDesc alterDatabaseDesc = work.getAlterDatabaseDesc();
      if (alterDatabaseDesc != null) {
        return alterDatabase(alterDatabaseDesc);
      }

      CreateTableDesc crtTbl = work.getCreateTblDesc();
      if (crtTbl != null) {
        return createTable(db, crtTbl);
      }

      CreateIndexDesc crtIndex = work.getCreateIndexDesc();
      if (crtIndex != null) {
        return createIndex(db, crtIndex);
      }

      AlterIndexDesc alterIndex = work.getAlterIndexDesc();
      if (alterIndex != null) {
        return alterIndex(db, alterIndex);
      }

      DropIndexDesc dropIdx = work.getDropIdxDesc();
      if (dropIdx != null) {
        return dropIndex(db, dropIdx);
      }

      CreateTableLikeDesc crtTblLike = work.getCreateTblLikeDesc();
      if (crtTblLike != null) {
        return createTableLike(db, crtTblLike);
      }

      DropTableDesc dropTbl = work.getDropTblDesc();
      if (dropTbl != null) {
        dropTableOrPartitions(db, dropTbl);
        return 0;
      }

      AlterTableDesc alterTbl = work.getAlterTblDesc();
      if (alterTbl != null) {
        return alterTable(db, alterTbl);
      }

      CreateViewDesc crtView = work.getCreateViewDesc();
      if (crtView != null) {
        return createView(db, crtView);
      }

      AddPartitionDesc addPartitionDesc = work.getAddPartitionDesc();
      if (addPartitionDesc != null) {
        return addPartitions(db, addPartitionDesc);
      }

      RenamePartitionDesc renamePartitionDesc = work.getRenamePartitionDesc();
      if (renamePartitionDesc != null) {
        return renamePartition(db, renamePartitionDesc);
      }

      AlterTableSimpleDesc simpleDesc = work.getAlterTblSimpleDesc();
      if (simpleDesc != null) {
        if (simpleDesc.getType() == AlterTableTypes.TOUCH) {
          return touch(db, simpleDesc);
        } else if (simpleDesc.getType() == AlterTableTypes.ARCHIVE) {
          return archive(db, simpleDesc, driverContext);
        } else if (simpleDesc.getType() == AlterTableTypes.UNARCHIVE) {
          return unarchive(db, simpleDesc);
        } else if (simpleDesc.getType() == AlterTableTypes.COMPACT) {
          return compact(db, simpleDesc);
        }
      }

      MsckDesc msckDesc = work.getMsckDesc();
      if (msckDesc != null) {
        return msck(db, msckDesc);
      }

      DescTableDesc descTbl = work.getDescTblDesc();
      if (descTbl != null) {
        return describeTable(db, descTbl);
      }

      DescFunctionDesc descFunc = work.getDescFunctionDesc();
      if (descFunc != null) {
        return describeFunction(descFunc);
      }

      ShowDatabasesDesc showDatabases = work.getShowDatabasesDesc();
      if (showDatabases != null) {
        return showDatabases(db, showDatabases);
      }

      ShowTablesDesc showTbls = work.getShowTblsDesc();
      if (showTbls != null) {
        return showTables(db, showTbls);
      }

      ShowColumnsDesc showCols = work.getShowColumnsDesc();
      if (showCols != null) {
        return showColumns(db, showCols);
      }

      ShowTableStatusDesc showTblStatus = work.getShowTblStatusDesc();
      if (showTblStatus != null) {
        return showTableStatus(db, showTblStatus);
      }

      ShowTblPropertiesDesc showTblProperties = work.getShowTblPropertiesDesc();
      if (showTblProperties != null) {
        return showTableProperties(db, showTblProperties);
      }

      ShowFunctionsDesc showFuncs = work.getShowFuncsDesc();
      if (showFuncs != null) {
        return showFunctions(showFuncs);
      }

      ShowLocksDesc showLocks = work.getShowLocksDesc();
      if (showLocks != null) {
        return showLocks(showLocks);
      }

      ShowCompactionsDesc compactionsDesc = work.getShowCompactionsDesc();
      if (compactionsDesc != null) {
        return showCompactions(compactionsDesc);
      }

      ShowTxnsDesc txnsDesc = work.getShowTxnsDesc();
      if (txnsDesc != null) {
        return showTxns(txnsDesc);
      }

       LockTableDesc lockTbl = work.getLockTblDesc();
      if (lockTbl != null) {
        return lockTable(lockTbl);
      }

      UnlockTableDesc unlockTbl = work.getUnlockTblDesc();
      if (unlockTbl != null) {
        return unlockTable(unlockTbl);
      }

      ShowPartitionsDesc showParts = work.getShowPartsDesc();
      if (showParts != null) {
        return showPartitions(db, showParts);
      }

      ShowCreateTableDesc showCreateTbl = work.getShowCreateTblDesc();
      if (showCreateTbl != null) {
        return showCreateTable(db, showCreateTbl);
      }

      ShowConfDesc showConf = work.getShowConfDesc();
      if (showConf != null) {
        return showConf(db, showConf);
      }

      RoleDDLDesc roleDDLDesc = work.getRoleDDLDesc();
      if (roleDDLDesc != null) {
        return roleDDL(roleDDLDesc);
      }

      GrantDesc grantDesc = work.getGrantDesc();
      if (grantDesc != null) {
        return grantOrRevokePrivileges(grantDesc.getPrincipals(), grantDesc
            .getPrivileges(), grantDesc.getPrivilegeSubjectDesc(), grantDesc.getGrantor(),
            grantDesc.getGrantorType(), grantDesc.isGrantOption(), true);
      }

      RevokeDesc revokeDesc = work.getRevokeDesc();
      if (revokeDesc != null) {
        return grantOrRevokePrivileges(revokeDesc.getPrincipals(), revokeDesc
            .getPrivileges(), revokeDesc.getPrivilegeSubjectDesc(), null, null,
            revokeDesc.isGrantOption(), false);
      }

      ShowGrantDesc showGrantDesc = work.getShowGrantDesc();
      if (showGrantDesc != null) {
        return showGrants(showGrantDesc);
      }

      GrantRevokeRoleDDL grantOrRevokeRoleDDL = work.getGrantRevokeRoleDDL();
      if (grantOrRevokeRoleDDL != null) {
        return grantOrRevokeRole(grantOrRevokeRoleDDL);
      }

      ShowIndexesDesc showIndexes = work.getShowIndexesDesc();
      if (showIndexes != null) {
        return showIndexes(db, showIndexes);
      }

      AlterTablePartMergeFilesDesc mergeFilesDesc = work.getMergeFilesDesc();
      if (mergeFilesDesc != null) {
        return mergeFiles(db, mergeFilesDesc);
      }

      AlterTableAlterPartDesc alterPartDesc = work.getAlterTableAlterPartDesc();
      if(alterPartDesc != null) {
        return alterTableAlterPart(db, alterPartDesc);
      }

      TruncateTableDesc truncateTableDesc = work.getTruncateTblDesc();
      if (truncateTableDesc != null) {
        return truncateTable(db, truncateTableDesc);
      }

      AlterTableExchangePartition alterTableExchangePartition =
          work.getAlterTableExchangePartition();
      if (alterTableExchangePartition != null) {
        return exchangeTablePartition(db, alterTableExchangePartition);
      }
    } catch (Throwable e) {
      failed(e);
      return 1;
    }
    assert false;
    return 0;
  }

  private void failed(Throwable e) {
    while (e.getCause() != null && e.getClass() == RuntimeException.class) {
      e = e.getCause();
    }
    setException(e);
    LOG.error(stringifyException(e));
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

  private DataOutputStream getOutputStream(Path outputFile) throws Exception {
    FileSystem fs = outputFile.getFileSystem(conf);
    return fs.create(outputFile);
  }

  /**
   * First, make sure the source table/partition is not
   * archived/indexes/non-rcfile. If either of these is true, throw an
   * exception.
   *
   * The way how it does the merge is to create a BlockMergeTask from the
   * mergeFilesDesc.
   *
   * @param db
   * @param mergeFilesDesc
   * @return
   * @throws HiveException
   */
  private int mergeFiles(Hive db, AlterTablePartMergeFilesDesc mergeFilesDesc)
      throws HiveException {
    ListBucketingCtx lbCtx = mergeFilesDesc.getLbCtx();
    boolean lbatc = lbCtx == null ? false : lbCtx.isSkewedStoredAsDir();
    int lbd = lbCtx == null ? 0 : lbCtx.calculateListBucketingLevel();

    // merge work only needs input and output.
    MergeFileWork mergeWork = new MergeFileWork(mergeFilesDesc.getInputDir(),
        mergeFilesDesc.getOutputDir(), mergeFilesDesc.getInputFormatClass().getName());
    LinkedHashMap<String, ArrayList<String>> pathToAliases =
        new LinkedHashMap<String, ArrayList<String>>();
    ArrayList<String> inputDirstr = new ArrayList<String>(1);
    inputDirstr.add(mergeFilesDesc.getInputDir().toString());
    pathToAliases.put(mergeFilesDesc.getInputDir().get(0).toString(), inputDirstr);
    mergeWork.setPathToAliases(pathToAliases);
    mergeWork.setListBucketingCtx(mergeFilesDesc.getLbCtx());
    mergeWork.resolveConcatenateMerge(db.getConf());
    mergeWork.setMapperCannotSpanPartns(true);
    mergeWork.setSourceTableInputFormat(mergeFilesDesc.getInputFormatClass().getName());
    final FileMergeDesc fmd;
    if (mergeFilesDesc.getInputFormatClass().equals(RCFileInputFormat.class)) {
      fmd = new RCFileMergeDesc();
    } else {
      // safe to assume else is ORC as semantic analyzer will check for RC/ORC
      fmd = new OrcFileMergeDesc();
    }

    fmd.setDpCtx(null);
    fmd.setHasDynamicPartitions(false);
    fmd.setListBucketingAlterTableConcatenate(lbatc);
    fmd.setListBucketingDepth(lbd);
    fmd.setOutputPath(mergeFilesDesc.getOutputDir());

    Operator<? extends OperatorDesc> mergeOp = OperatorFactory.get(fmd);

    LinkedHashMap<String, Operator<? extends  OperatorDesc>> aliasToWork =
        new LinkedHashMap<String, Operator<? extends OperatorDesc>>();
    aliasToWork.put(mergeFilesDesc.getInputDir().toString(), mergeOp);
    mergeWork.setAliasToWork(aliasToWork);
    DriverContext driverCxt = new DriverContext();
    Task task;
    if (conf.getVar(ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
      TezWork tezWork = new TezWork(conf.getVar(HiveConf.ConfVars.HIVEQUERYID));
      mergeWork.setName("File Merge");
      tezWork.add(mergeWork);
      task = new TezTask();
      task.setWork(tezWork);
    } else {
      task = new MergeFileTask();
      task.setWork(mergeWork);
    }

    // initialize the task and execute
    task.initialize(db.getConf(), getQueryPlan(), driverCxt);
    int ret = task.execute(driverCxt);
    return ret;
  }

  private HiveAuthorizer getSessionAuthorizer() {
    HiveAuthorizer authorizer = SessionState.get().getAuthorizerV2();
    if (authorizer == null) {
      authorizer = new HiveV1Authorizer(conf, db);
    }
    return authorizer;
  }

  private int grantOrRevokeRole(GrantRevokeRoleDDL grantOrRevokeRoleDDL)
      throws HiveException {
    HiveAuthorizer authorizer = getSessionAuthorizer();
    //convert to the types needed for plugin api
    HivePrincipal grantorPrinc = null;
    if(grantOrRevokeRoleDDL.getGrantor() != null){
      grantorPrinc = new HivePrincipal(grantOrRevokeRoleDDL.getGrantor(),
          AuthorizationUtils.getHivePrincipalType(grantOrRevokeRoleDDL.getGrantorType()));
    }
    List<HivePrincipal> principals =
        AuthorizationUtils.getHivePrincipals(grantOrRevokeRoleDDL.getPrincipalDesc());
    List<String> roles = grantOrRevokeRoleDDL.getRoles();

    boolean grantOption = grantOrRevokeRoleDDL.isGrantOption();
    if (grantOrRevokeRoleDDL.getGrant()) {
      authorizer.grantRole(principals, roles, grantOption, grantorPrinc);
    } else {
      authorizer.revokeRole(principals, roles, grantOption, grantorPrinc);
    }
    return 0;
  }

  private int showGrants(ShowGrantDesc showGrantDesc) throws HiveException {

    HiveAuthorizer authorizer = getSessionAuthorizer();
    try {
      List<HivePrivilegeInfo> privInfos = authorizer.showPrivileges(
          AuthorizationUtils.getHivePrincipal(showGrantDesc.getPrincipalDesc()),
          AuthorizationUtils.getHivePrivilegeObject(showGrantDesc.getHiveObj()));
      boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST);
      writeToFile(writeGrantInfo(privInfos, testMode), showGrantDesc.getResFile());
    } catch (IOException e) {
      throw new HiveException("Error in show grant statement", e);
    }
    return 0;
  }

  private int grantOrRevokePrivileges(List<PrincipalDesc> principals,
      List<PrivilegeDesc> privileges, PrivilegeObjectDesc privSubjectDesc,
      String grantor, PrincipalType grantorType, boolean grantOption, boolean isGrant)
          throws HiveException {

    HiveAuthorizer authorizer = getSessionAuthorizer();

    //Convert to object types used by the authorization plugin interface
    List<HivePrincipal> hivePrincipals = AuthorizationUtils.getHivePrincipals(principals);
    List<HivePrivilege> hivePrivileges = AuthorizationUtils.getHivePrivileges(privileges);
    HivePrivilegeObject hivePrivObject = AuthorizationUtils.getHivePrivilegeObject(privSubjectDesc);

    HivePrincipal grantorPrincipal = new HivePrincipal(
        grantor, AuthorizationUtils.getHivePrincipalType(grantorType));

    if(isGrant){
      authorizer.grantPrivileges(hivePrincipals, hivePrivileges, hivePrivObject,
          grantorPrincipal, grantOption);
    }else {
      authorizer.revokePrivileges(hivePrincipals, hivePrivileges,
          hivePrivObject, grantorPrincipal, grantOption);
    }
    //no exception thrown, so looks good
    return 0;
  }

  private int roleDDL(RoleDDLDesc roleDDLDesc) throws Exception {
    HiveAuthorizer authorizer = getSessionAuthorizer();
    RoleDDLDesc.RoleOperation operation = roleDDLDesc.getOperation();
    //call the appropriate hive authorizer function
    switch(operation){
    case CREATE_ROLE:
      authorizer.createRole(roleDDLDesc.getName(), null);
      break;
    case DROP_ROLE:
      authorizer.dropRole(roleDDLDesc.getName());
      break;
    case SHOW_ROLE_GRANT:
      boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST);
      List<HiveRoleGrant> roles = authorizer.getRoleGrantInfoForPrincipal(
          AuthorizationUtils.getHivePrincipal(roleDDLDesc.getName(), roleDDLDesc.getPrincipalType()));
      writeToFile(writeRolesGrantedInfo(roles, testMode), roleDDLDesc.getResFile());
      break;
    case SHOW_ROLES:
      List<String> allRoles = authorizer.getAllRoles();
      writeListToFileAfterSort(allRoles, roleDDLDesc.getResFile());
      break;
    case SHOW_CURRENT_ROLE:
      List<String> roleNames = authorizer.getCurrentRoleNames();
      writeListToFileAfterSort(roleNames, roleDDLDesc.getResFile());
      break;
    case SET_ROLE:
      authorizer.setCurrentRole(roleDDLDesc.getName());
      break;
    case SHOW_ROLE_PRINCIPALS:
      testMode = conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST);
      List<HiveRoleGrant> roleGrants = authorizer.getPrincipalGrantInfoForRole(roleDDLDesc.getName());
      writeToFile(writeHiveRoleGrantInfo(roleGrants, testMode), roleDDLDesc.getResFile());
      break;
    default:
      throw new HiveException("Unkown role operation "
          + operation.getOperationName());
    }

    return 0;
  }

  private String writeHiveRoleGrantInfo(List<HiveRoleGrant> roleGrants, boolean testMode) {
    if (roleGrants == null || roleGrants.isEmpty()) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    // sort the list to get sorted (deterministic) output (for ease of testing)
    Collections.sort(roleGrants);
    for (HiveRoleGrant roleGrant : roleGrants) {
      // schema:
      // principal_name,principal_type,grant_option,grantor,grantor_type,grant_time
      appendNonNull(builder, roleGrant.getPrincipalName(), true);
      appendNonNull(builder, roleGrant.getPrincipalType());
      appendNonNull(builder, roleGrant.isGrantOption());
      appendNonNull(builder, roleGrant.getGrantor());
      appendNonNull(builder, roleGrant.getGrantorType());
      appendNonNull(builder, testMode ? -1 : roleGrant.getGrantTime() * 1000L);
    }
    return builder.toString();
  }

  /**
   * Write list of string entries into given file
   * @param entries
   * @param resFile
   * @throws IOException
   */
  private void writeListToFileAfterSort(List<String> entries, String resFile) throws IOException {
    Collections.sort(entries);
    StringBuilder sb = new StringBuilder();
    for(String entry : entries){
      appendNonNull(sb, entry, true);
    }
    writeToFile(sb.toString(), resFile);
  }

  private int alterDatabase(AlterDatabaseDesc alterDbDesc) throws HiveException {

    String dbName = alterDbDesc.getDatabaseName();
    Database database = db.getDatabase(dbName);
    if (database == null) {
      throw new HiveException(ErrorMsg.DATABASE_NOT_EXISTS, dbName);
    }

    switch (alterDbDesc.getAlterType()) {
    case ALTER_PROPERTY:
      Map<String, String> newParams = alterDbDesc.getDatabaseProperties();
      Map<String, String> params = database.getParameters();
      // if both old and new params are not null, merge them
      if (params != null && newParams != null) {
        params.putAll(newParams);
        database.setParameters(params);
      } else {
        // if one of them is null, replace the old params with the new one
        database.setParameters(newParams);
      }
      break;

    case ALTER_OWNER:
      database.setOwnerName(alterDbDesc.getOwnerPrincipal().getName());
      database.setOwnerType(alterDbDesc.getOwnerPrincipal().getType());
      break;

    default:
      throw new AssertionError("Unsupported alter database type! : " + alterDbDesc.getAlterType());
    }

    db.alterDatabase(database.getName(), database);
    return 0;
  }

  private int dropIndex(Hive db, DropIndexDesc dropIdx) throws HiveException {
    db.dropIndex(dropIdx.getTableName(), dropIdx.getIndexName(), dropIdx.isThrowException(), true);
    return 0;
  }

  private int createIndex(Hive db, CreateIndexDesc crtIndex) throws HiveException {

    if( crtIndex.getSerde() != null) {
      validateSerDe(crtIndex.getSerde());
    }

    String indexTableName = crtIndex.getIndexTableName();
    if (!Utilities.isDefaultNameNode(conf)) {
      // If location is specified - ensure that it is a full qualified name
      makeLocationQualified(crtIndex, indexTableName);
    }

    db
    .createIndex(
        crtIndex.getTableName(), crtIndex.getIndexName(), crtIndex.getIndexTypeHandlerClass(),
        crtIndex.getIndexedCols(), crtIndex.getIndexTableName(), crtIndex.getDeferredRebuild(),
        crtIndex.getInputFormat(), crtIndex.getOutputFormat(), crtIndex.getSerde(),
        crtIndex.getStorageHandler(), crtIndex.getLocation(), crtIndex.getIdxProps(), crtIndex.getTblProps(),
        crtIndex.getSerdeProps(), crtIndex.getCollItemDelim(), crtIndex.getFieldDelim(), crtIndex.getFieldEscape(),
        crtIndex.getLineDelim(), crtIndex.getMapKeyDelim(), crtIndex.getIndexComment()
        );
    if (HiveUtils.getIndexHandler(conf, crtIndex.getIndexTypeHandlerClass()).usesIndexTable()) {
          Table indexTable = db.getTable(indexTableName);
          work.getOutputs().add(new WriteEntity(indexTable, WriteEntity.WriteType.DDL_NO_LOCK));
    }
    return 0;
  }

  private int alterIndex(Hive db, AlterIndexDesc alterIndex) throws HiveException {
    String baseTableName = alterIndex.getBaseTableName();
    String indexName = alterIndex.getIndexName();
    Index idx = db.getIndex(baseTableName, indexName);

    switch(alterIndex.getOp()) {
    case ADDPROPS:
      idx.getParameters().putAll(alterIndex.getProps());
      break;
    case UPDATETIMESTAMP:
      try {
        Map<String, String> props = new HashMap<String, String>();
        Map<Map<String, String>, Long> basePartTs = new HashMap<Map<String, String>, Long>();

        Table baseTbl = db.getTable(baseTableName);

        if (baseTbl.isPartitioned()) {
          List<Partition> baseParts;
          if (alterIndex.getSpec() != null) {
            baseParts = db.getPartitions(baseTbl, alterIndex.getSpec());
          } else {
            baseParts = db.getPartitions(baseTbl);
          }
          if (baseParts != null) {
            for (Partition p : baseParts) {
              FileSystem fs = p.getDataLocation().getFileSystem(db.getConf());
              FileStatus fss = fs.getFileStatus(p.getDataLocation());
              basePartTs.put(p.getSpec(), fss.getModificationTime());
            }
          }
        } else {
          FileSystem fs = baseTbl.getPath().getFileSystem(db.getConf());
          FileStatus fss = fs.getFileStatus(baseTbl.getPath());
          basePartTs.put(null, fss.getModificationTime());
        }
        for (Map<String, String> spec : basePartTs.keySet()) {
          if (spec != null) {
            props.put(spec.toString(), basePartTs.get(spec).toString());
          } else {
            props.put("base_timestamp", basePartTs.get(null).toString());
          }
        }
        idx.getParameters().putAll(props);
      } catch (HiveException e) {
        throw new HiveException("ERROR: Failed to update index timestamps");
      } catch (IOException e) {
        throw new HiveException("ERROR: Failed to look up timestamps on filesystem");
      }

      break;
    default:
      console.printError("Unsupported Alter commnad");
      return 1;
    }

    // set last modified by properties
    if (!updateModifiedParameters(idx.getParameters(), conf)) {
      return 1;
    }

    try {
      db.alterIndex(baseTableName, indexName, idx);
    } catch (InvalidOperationException e) {
      console.printError("Invalid alter operation: " + e.getMessage());
      LOG.info("alter index: " + stringifyException(e));
      return 1;
    } catch (HiveException e) {
      console.printError("Invalid alter operation: " + e.getMessage());
      return 1;
    }
    return 0;
  }

  /**
   * Add a partitions to a table.
   *
   * @param db
   *          Database to add the partition to.
   * @param addPartitionDesc
   *          Add these partitions.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   */
  private int addPartitions(Hive db, AddPartitionDesc addPartitionDesc) throws HiveException {
    List<Partition> parts = db.createPartitions(addPartitionDesc);
    for (Partition part : parts) {
      work.getOutputs().add(new WriteEntity(part, WriteEntity.WriteType.INSERT));
    }
    return 0;
  }

  /**
   * Rename a partition in a table
   *
   * @param db
   *          Database to rename the partition.
   * @param renamePartitionDesc
   *          rename old Partition to new one.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   */
  private int renamePartition(Hive db, RenamePartitionDesc renamePartitionDesc) throws HiveException {

    Table tbl = db.getTable(renamePartitionDesc.getTableName());

    LinkedHashMap<String, String> oldPartSpec = renamePartitionDesc.getOldPartSpec();
    Partition oldPart = db.getPartition(tbl, oldPartSpec, false);
    if (oldPart == null) {
      String partName = FileUtils.makePartName(new ArrayList<String>(oldPartSpec.keySet()),
          new ArrayList<String>(oldPartSpec.values()));
      throw new HiveException("Rename partition: source partition [" + partName
          + "] does not exist.");
    }
    Partition part = db.getPartition(tbl, oldPartSpec, false);
    part.setValues(renamePartitionDesc.getNewPartSpec());
    db.renamePartition(tbl, oldPartSpec, part);
    Partition newPart = db
        .getPartition(tbl, renamePartitionDesc.getNewPartSpec(), false);
    work.getInputs().add(new ReadEntity(oldPart));
    // We've already obtained a lock on the table, don't lock the partition too
    work.getOutputs().add(new WriteEntity(newPart, WriteEntity.WriteType.DDL_NO_LOCK));
    return 0;
  }

  /**
   * Alter partition column type in a table
   *
   * @param db
   *          Database to rename the partition.
   * @param alterPartitionDesc
   *          change partition column type.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   */
  private int alterTableAlterPart(Hive db, AlterTableAlterPartDesc alterPartitionDesc)
      throws HiveException {

    Table tbl = db.getTable(alterPartitionDesc.getTableName(), true);
    String tabName = alterPartitionDesc.getTableName();

    // This is checked by DDLSemanticAnalyzer
    assert(tbl.isPartitioned());

    List<FieldSchema> newPartitionKeys = new ArrayList<FieldSchema>();

    //Check if the existing partition values can be type casted to the new column type
    // with a non null value before trying to alter the partition column type.
    try {
      Set<Partition> partitions = db.getAllPartitionsOf(tbl);
      int colIndex = -1;
      for(FieldSchema col : tbl.getTTable().getPartitionKeys()) {
        colIndex++;
        if (col.getName().compareTo(alterPartitionDesc.getPartKeySpec().getName()) == 0) {
          break;
        }
      }

      if (colIndex == -1 || colIndex == tbl.getTTable().getPartitionKeys().size()) {
        throw new HiveException("Cannot find partition column " +
            alterPartitionDesc.getPartKeySpec().getName());
      }

      TypeInfo expectedType =
          TypeInfoUtils.getTypeInfoFromTypeString(alterPartitionDesc.getPartKeySpec().getType());
      ObjectInspector outputOI =
          TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(expectedType);
      Converter converter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector, outputOI);

      // For all the existing partitions, check if the value can be type casted to a non-null object
      for(Partition part : partitions) {
        if (part.getName().equals(conf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME))) {
          continue;
        }
        try {
          String value = part.getValues().get(colIndex);
          Object convertedValue =
              converter.convert(value);
          if (convertedValue == null) {
            throw new HiveException(" Converting from " + TypeInfoFactory.stringTypeInfo + " to " +
              expectedType + " for value : " + value + " resulted in NULL object");
          }
        } catch (Exception e) {
          throw new HiveException("Exception while converting " +
              TypeInfoFactory.stringTypeInfo + " to " +
              expectedType + " for value : " + part.getValues().get(colIndex));
        }
      }
    } catch(Exception e) {
      throw new HiveException(
          "Exception while checking type conversion of existing partition values to " +
          alterPartitionDesc.getPartKeySpec() + " : " + e.getMessage());
    }

    for(FieldSchema col : tbl.getTTable().getPartitionKeys()) {
      if (col.getName().compareTo(alterPartitionDesc.getPartKeySpec().getName()) == 0) {
        newPartitionKeys.add(alterPartitionDesc.getPartKeySpec());
      } else {
        newPartitionKeys.add(col);
      }
    }

    tbl.getTTable().setPartitionKeys(newPartitionKeys);

    try {
      db.alterTable(tabName, tbl);
    } catch (InvalidOperationException e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "Unable to alter " + tabName);
    }

    work.getInputs().add(new ReadEntity(tbl));
    // We've already locked the table as the input, don't relock it as the output.
    work.getOutputs().add(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK));

    return 0;
  }

  /**
   * Rewrite the partition's metadata and force the pre/post execute hooks to
   * be fired.
   *
   * @param db
   * @param touchDesc
   * @return
   * @throws HiveException
   */
  private int touch(Hive db, AlterTableSimpleDesc touchDesc)
      throws HiveException {

    Table tbl = db.getTable(touchDesc.getTableName());

    if (touchDesc.getPartSpec() == null) {
      try {
        db.alterTable(touchDesc.getTableName(), tbl);
      } catch (InvalidOperationException e) {
        throw new HiveException("Uable to update table");
      }
      work.getInputs().add(new ReadEntity(tbl));
      work.getOutputs().add(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK));
    } else {
      Partition part = db.getPartition(tbl, touchDesc.getPartSpec(), false);
      if (part == null) {
        throw new HiveException("Specified partition does not exist");
      }
      try {
        db.alterPartition(touchDesc.getTableName(), part);
      } catch (InvalidOperationException e) {
        throw new HiveException(e);
      }
      work.getInputs().add(new ReadEntity(part));
      work.getOutputs().add(new WriteEntity(part, WriteEntity.WriteType.DDL_NO_LOCK));
    }
    return 0;
  }

  /**
   * Sets archiving flag locally; it has to be pushed into metastore
   * @param p partition to set flag
   * @param state desired state of IS_ARCHIVED flag
   * @param level desired level for state == true, anything for false
   */
  private void setIsArchived(Partition p, boolean state, int level) {
    Map<String, String> params = p.getParameters();
    if (state) {
      params.put(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.IS_ARCHIVED,
          "true");
      params.put(ArchiveUtils.ARCHIVING_LEVEL, Integer
          .toString(level));
    } else {
      params.remove(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.IS_ARCHIVED);
      params.remove(ArchiveUtils.ARCHIVING_LEVEL);
    }
  }

  /**
   * Returns original partition of archived partition, null for unarchived one
   */
  private String getOriginalLocation(Partition p) {
    Map<String, String> params = p.getParameters();
    return params.get(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ORIGINAL_LOCATION);
  }

  /**
   * Sets original location of partition which is to be archived
   */
  private void setOriginalLocation(Partition p, String loc) {
    Map<String, String> params = p.getParameters();
    if (loc == null) {
      params.remove(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ORIGINAL_LOCATION);
    } else {
      params.put(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ORIGINAL_LOCATION, loc);
    }
  }

  /**
   * Sets the appropriate attributes in the supplied Partition object to mark
   * it as archived. Note that the metastore is not touched - a separate
   * call to alter_partition is needed.
   *
   * @param p - the partition object to modify
   * @param harPath - new location of partition (har schema URI)
   */
  private void setArchived(Partition p, Path harPath, int level) {
    assert(ArchiveUtils.isArchived(p) == false);
    setIsArchived(p, true, level);
    setOriginalLocation(p, p.getLocation());
    p.setLocation(harPath.toString());
  }

  /**
   * Sets the appropriate attributes in the supplied Partition object to mark
   * it as not archived. Note that the metastore is not touched - a separate
   * call to alter_partition is needed.
   *
   * @param p - the partition to modify
   */
  private void setUnArchived(Partition p) {
    assert(ArchiveUtils.isArchived(p) == true);
    String parentDir = getOriginalLocation(p);
    setIsArchived(p, false, 0);
    setOriginalLocation(p, null);
    assert(parentDir != null);
    p.setLocation(parentDir);
  }

  private boolean pathExists(Path p) throws HiveException {
    try {
      FileSystem fs = p.getFileSystem(conf);
      return fs.exists(p);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  private void moveDir(FileSystem fs, Path from, Path to) throws HiveException {
    try {
      if (!fs.rename(from, to)) {
        throw new HiveException("Moving " + from + " to " + to + " failed!");
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  private void deleteDir(Path dir) throws HiveException {
    try {
      Warehouse wh = new Warehouse(conf);
      wh.deleteDir(dir, true);
    } catch (MetaException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Checks in partition is in custom (not-standard) location.
   * @param tbl - table in which partition is
   * @param p - partition
   * @return true if partition location is custom, false if it is standard
   */
  boolean partitionInCustomLocation(Table tbl, Partition p)
      throws HiveException {
    String subdir = null;
    try {
      subdir = Warehouse.makePartName(tbl.getPartCols(), p.getValues());
    } catch (MetaException e) {
      throw new HiveException("Unable to get partition's directory", e);
    }
    Path tableDir = tbl.getDataLocation();
    if(tableDir == null) {
      throw new HiveException("Table has no location set");
    }

    String standardLocation = (new Path(tableDir, subdir)).toString();
    if(ArchiveUtils.isArchived(p)) {
      return !getOriginalLocation(p).equals(standardLocation);
    } else {
      return !p.getLocation().equals(standardLocation);
    }
  }

  private int archive(Hive db, AlterTableSimpleDesc simpleDesc,
      DriverContext driverContext)
          throws HiveException {

    Table tbl = db.getTable(simpleDesc.getTableName());

    if (tbl.getTableType() != TableType.MANAGED_TABLE) {
      throw new HiveException("ARCHIVE can only be performed on managed tables");
    }

    Map<String, String> partSpec = simpleDesc.getPartSpec();
    PartSpecInfo partSpecInfo = PartSpecInfo.create(tbl, partSpec);
    List<Partition> partitions = db.getPartitions(tbl, partSpec);

    Path originalDir = null;

    // when we have partial partitions specification we must assume partitions
    // lie in standard place - if they were in custom locations putting
    // them into one archive would involve mass amount of copying
    // in full partition specification case we allow custom locations
    // to keep backward compatibility
    if (partitions.isEmpty()) {
      throw new HiveException("No partition matches the specification");
    } else if(partSpecInfo.values.size() != tbl.getPartCols().size()) {
      // for partial specifications we need partitions to follow the scheme
      for(Partition p: partitions){
        if(partitionInCustomLocation(tbl, p)) {
          String message = String.format("ARCHIVE cannot run for partition " +
              "groups with custom locations like %s", p.getLocation());
          throw new HiveException(message);
        }
      }
      originalDir = partSpecInfo.createPath(tbl);
    } else {
      Partition p = partitions.get(0);
      // partition can be archived if during recovery
      if(ArchiveUtils.isArchived(p)) {
        originalDir = new Path(getOriginalLocation(p));
      } else {
        originalDir = p.getDataLocation();
      }
    }

    Path intermediateArchivedDir = new Path(originalDir.getParent(),
        originalDir.getName() + INTERMEDIATE_ARCHIVED_DIR_SUFFIX);
    Path intermediateOriginalDir = new Path(originalDir.getParent(),
        originalDir.getName() + INTERMEDIATE_ORIGINAL_DIR_SUFFIX);

    console.printInfo("intermediate.archived is " + intermediateArchivedDir.toString());
    console.printInfo("intermediate.original is " + intermediateOriginalDir.toString());

    String archiveName = "data.har";
    FileSystem fs = null;
    try {
      fs = originalDir.getFileSystem(conf);
    } catch (IOException e) {
      throw new HiveException(e);
    }

    URI archiveUri = (new Path(originalDir, archiveName)).toUri();
    URI originalUri = ArchiveUtils.addSlash(originalDir.toUri());
    ArchiveUtils.HarPathHelper harHelper = new ArchiveUtils.HarPathHelper(
        conf, archiveUri, originalUri);

    // we checked if partitions matching specification are marked as archived
    // in the metadata; if they are and their levels are the same as we would
    // set it later it means previous run failed and we have to do the recovery;
    // if they are different, we throw an error
    for(Partition p: partitions) {
      if(ArchiveUtils.isArchived(p)) {
        if(ArchiveUtils.getArchivingLevel(p) != partSpecInfo.values.size()) {
          String name = ArchiveUtils.getPartialName(p, ArchiveUtils.getArchivingLevel(p));
          String m = String.format("Conflict with existing archive %s", name);
          throw new HiveException(m);
        } else {
          throw new HiveException("Partition(s) already archived");
        }
      }
    }

    boolean recovery = false;
    if (pathExists(intermediateArchivedDir)
        || pathExists(intermediateOriginalDir)) {
      recovery = true;
      console.printInfo("Starting recovery after failed ARCHIVE");
    }

    // The following steps seem roundabout, but they are meant to aid in
    // recovery if a failure occurs and to keep a consistent state in the FS

    // Steps:
    // 1. Create the archive in a temporary folder
    // 2. Move the archive dir to an intermediate dir that is in at the same
    //    dir as the original partition dir. Call the new dir
    //    intermediate-archive.
    // 3. Rename the original partition dir to an intermediate dir. Call the
    //    renamed dir intermediate-original
    // 4. Rename intermediate-archive to the original partition dir
    // 5. Change the metadata
    // 6. Delete the original partition files in intermediate-original

    // The original partition files are deleted after the metadata change
    // because the presence of those files are used to indicate whether
    // the original partition directory contains archived or unarchived files.

    // Create an archived version of the partition in a directory ending in
    // ARCHIVE_INTERMEDIATE_DIR_SUFFIX that's the same level as the partition,
    // if it does not already exist. If it does exist, we assume the dir is good
    // to use as the move operation that created it is atomic.
    if (!pathExists(intermediateArchivedDir) &&
        !pathExists(intermediateOriginalDir)) {

      // First create the archive in a tmp dir so that if the job fails, the
      // bad files don't pollute the filesystem
      Path tmpPath = new Path(driverContext.getCtx()
          .getExternalTmpPath(originalDir), "partlevel");

      console.printInfo("Creating " + archiveName +
          " for " + originalDir.toString());
      console.printInfo("in " + tmpPath);
      console.printInfo("Please wait... (this may take a while)");

      // Create the Hadoop archive
      int ret=0;
      try {
        int maxJobNameLen = conf.getIntVar(HiveConf.ConfVars.HIVEJOBNAMELENGTH);
        String jobname = String.format("Archiving %s@%s",
            tbl.getTableName(), partSpecInfo.getName());
        jobname = Utilities.abbreviate(jobname, maxJobNameLen - 6);
        conf.setVar(HiveConf.ConfVars.HADOOPJOBNAME, jobname);
        HadoopArchives har = new HadoopArchives(conf);
        List<String> args = new ArrayList<String>();

        args.add("-archiveName");
        args.add(archiveName);
        args.add("-p");
        args.add(originalDir.toString());
        args.add(tmpPath.toString());

        ret = ToolRunner.run(har, args.toArray(new String[0]));;
      } catch (Exception e) {
        throw new HiveException(e);
      }
      if (ret != 0) {
        throw new HiveException("Error while creating HAR");
      }

      // Move from the tmp dir to an intermediate directory, in the same level as
      // the partition directory. e.g. .../hr=12-intermediate-archived
      try {
        console.printInfo("Moving " + tmpPath + " to " + intermediateArchivedDir);
        if (pathExists(intermediateArchivedDir)) {
          throw new HiveException("The intermediate archive directory already exists.");
        }
        fs.rename(tmpPath, intermediateArchivedDir);
      } catch (IOException e) {
        throw new HiveException("Error while moving tmp directory");
      }
    } else {
      if (pathExists(intermediateArchivedDir)) {
        console.printInfo("Intermediate archive directory " + intermediateArchivedDir +
            " already exists. Assuming it contains an archived version of the partition");
      }
    }

    // If we get to here, we know that we've archived the partition files, but
    // they may be in the original partition location, or in the intermediate
    // original dir.

    // Move the original parent directory to the intermediate original directory
    // if the move hasn't been made already
    if (!pathExists(intermediateOriginalDir)) {
      console.printInfo("Moving " + originalDir + " to " +
          intermediateOriginalDir);
      moveDir(fs, originalDir, intermediateOriginalDir);
    } else {
      console.printInfo(intermediateOriginalDir + " already exists. " +
          "Assuming it contains the original files in the partition");
    }

    // If there's a failure from here to when the metadata is updated,
    // there will be no data in the partition, or an error while trying to read
    // the partition (if the archive files have been moved to the original
    // partition directory.) But re-running the archive command will allow
    // recovery

    // Move the intermediate archived directory to the original parent directory
    if (!pathExists(originalDir)) {
      console.printInfo("Moving " + intermediateArchivedDir + " to " +
          originalDir);
      moveDir(fs, intermediateArchivedDir, originalDir);
    } else {
      console.printInfo(originalDir + " already exists. " +
          "Assuming it contains the archived version of the partition");
    }

    // Record this change in the metastore
    try {
      for(Partition p: partitions) {
        URI originalPartitionUri = ArchiveUtils.addSlash(p.getDataLocation().toUri());
        URI harPartitionDir = harHelper.getHarUri(originalPartitionUri);
        StringBuilder authority = new StringBuilder();
        if(harPartitionDir.getUserInfo() != null) {
          authority.append(harPartitionDir.getUserInfo()).append("@");
        }
        authority.append(harPartitionDir.getHost());
        if(harPartitionDir.getPort() != -1) {
          authority.append(":").append(harPartitionDir.getPort());
        }
        Path harPath = new Path(harPartitionDir.getScheme(),
            authority.toString(),
            harPartitionDir.getPath()); // make in Path to ensure no slash at the end
        setArchived(p, harPath, partSpecInfo.values.size());
        db.alterPartition(simpleDesc.getTableName(), p);
      }
    } catch (Exception e) {
      throw new HiveException("Unable to change the partition info for HAR", e);
    }

    // If a failure occurs here, the directory containing the original files
    // will not be deleted. The user will run ARCHIVE again to clear this up
    if(pathExists(intermediateOriginalDir)) {
      deleteDir(intermediateOriginalDir);
    }

    if(recovery) {
      console.printInfo("Recovery after ARCHIVE succeeded");
    }

    return 0;
  }

  private int unarchive(Hive db, AlterTableSimpleDesc simpleDesc)
      throws HiveException, URISyntaxException {

    Table tbl = db.getTable(simpleDesc.getTableName());

    // Means user specified a table, not a partition
    if (simpleDesc.getPartSpec() == null) {
      throw new HiveException("UNARCHIVE is for partitions only");
    }

    if (tbl.getTableType() != TableType.MANAGED_TABLE) {
      throw new HiveException("UNARCHIVE can only be performed on managed tables");
    }

    Map<String, String> partSpec = simpleDesc.getPartSpec();
    PartSpecInfo partSpecInfo = PartSpecInfo.create(tbl, partSpec);
    List<Partition> partitions = db.getPartitions(tbl, partSpec);

    int partSpecLevel = partSpec.size();

    Path originalDir = null;

    // when we have partial partitions specification we must assume partitions
    // lie in standard place - if they were in custom locations putting
    // them into one archive would involve mass amount of copying
    // in full partition specification case we allow custom locations
    // to keep backward compatibility
    if (partitions.isEmpty()) {
      throw new HiveException("No partition matches the specification");
    } else if(partSpecInfo.values.size() != tbl.getPartCols().size()) {
      // for partial specifications we need partitions to follow the scheme
      for(Partition p: partitions){
        if(partitionInCustomLocation(tbl, p)) {
          String message = String.format("UNARCHIVE cannot run for partition " +
              "groups with custom locations like %s", p.getLocation());
          throw new HiveException(message);
        }
      }
      originalDir = partSpecInfo.createPath(tbl);
    } else {
      Partition p = partitions.get(0);
      if(ArchiveUtils.isArchived(p)) {
        originalDir = new Path(getOriginalLocation(p));
      } else {
        originalDir = new Path(p.getLocation());
      }
    }

    URI originalUri = ArchiveUtils.addSlash(originalDir.toUri());
    Path intermediateArchivedDir = new Path(originalDir.getParent(),
        originalDir.getName() + INTERMEDIATE_ARCHIVED_DIR_SUFFIX);
    Path intermediateExtractedDir = new Path(originalDir.getParent(),
        originalDir.getName() + INTERMEDIATE_EXTRACTED_DIR_SUFFIX);
    boolean recovery = false;
    if(pathExists(intermediateArchivedDir) || pathExists(intermediateExtractedDir)) {
      recovery = true;
      console.printInfo("Starting recovery after failed UNARCHIVE");
    }

    for(Partition p: partitions) {
      checkArchiveProperty(partSpecLevel, recovery, p);
    }

    String archiveName = "data.har";
    FileSystem fs = null;
    try {
      fs = originalDir.getFileSystem(conf);
    } catch (IOException e) {
      throw new HiveException(e);
    }

    // assume the archive is in the original dir, check if it exists
    Path archivePath = new Path(originalDir, archiveName);
    URI archiveUri = archivePath.toUri();
    ArchiveUtils.HarPathHelper harHelper = new ArchiveUtils.HarPathHelper(conf,
        archiveUri, originalUri);
    URI sourceUri = harHelper.getHarUri(originalUri);
    Path sourceDir = new Path(sourceUri.getScheme(), sourceUri.getAuthority(), sourceUri.getPath());

    if(!pathExists(intermediateArchivedDir) && !pathExists(archivePath)) {
      throw new HiveException("Haven't found any archive where it should be");
    }

    Path tmpPath = driverContext.getCtx().getExternalTmpPath(originalDir);

    try {
      fs = tmpPath.getFileSystem(conf);
    } catch (IOException e) {
      throw new HiveException(e);
    }

    // Clarification of terms:
    // - The originalDir directory represents the original directory of the
    //   partitions' files. They now contain an archived version of those files
    //   eg. hdfs:/warehouse/myTable/ds=1/
    // - The source directory is the directory containing all the files that
    //   should be in the partitions. e.g. har:/warehouse/myTable/ds=1/myTable.har/
    //   Note the har:/ scheme

    // Steps:
    // 1. Extract the archive in a temporary folder
    // 2. Move the archive dir to an intermediate dir that is in at the same
    //    dir as originalLocation. Call the new dir intermediate-extracted.
    // 3. Rename the original partitions dir to an intermediate dir. Call the
    //    renamed dir intermediate-archive
    // 4. Rename intermediate-extracted to the original partitions dir
    // 5. Change the metadata
    // 6. Delete the archived partitions files in intermediate-archive

    if (!pathExists(intermediateExtractedDir) &&
        !pathExists(intermediateArchivedDir)) {
      try {

        // Copy the files out of the archive into the temporary directory
        String copySource = sourceDir.toString();
        String copyDest = tmpPath.toString();
        List<String> args = new ArrayList<String>();
        args.add("-cp");
        args.add(copySource);
        args.add(copyDest);

        console.printInfo("Copying " + copySource + " to " + copyDest);
        FileSystem srcFs = FileSystem.get(sourceDir.toUri(), conf);
        srcFs.initialize(sourceDir.toUri(), conf);

        FsShell fss = new FsShell(conf);
        int ret = 0;
        try {
          ret = ToolRunner.run(fss, args.toArray(new String[0]));
        } catch (Exception e) {
          e.printStackTrace();
          throw new HiveException(e);
        }

        if (ret != 0) {
          throw new HiveException("Error while copying files from archive, return code=" + ret);
        } else {
          console.printInfo("Succefully Copied " + copySource + " to " + copyDest);
        }

        console.printInfo("Moving " + tmpPath + " to " + intermediateExtractedDir);
        if (fs.exists(intermediateExtractedDir)) {
          throw new HiveException("Invalid state: the intermediate extracted " +
              "directory already exists.");
        }
        fs.rename(tmpPath, intermediateExtractedDir);
      } catch (Exception e) {
        throw new HiveException(e);
      }
    }

    // At this point, we know that the extracted files are in the intermediate
    // extracted dir, or in the the original directory.

    if (!pathExists(intermediateArchivedDir)) {
      try {
        console.printInfo("Moving " + originalDir + " to " + intermediateArchivedDir);
        fs.rename(originalDir, intermediateArchivedDir);
      } catch (IOException e) {
        throw new HiveException(e);
      }
    } else {
      console.printInfo(intermediateArchivedDir + " already exists. " +
          "Assuming it contains the archived version of the partition");
    }

    // If there is a failure from here to until when the metadata is changed,
    // the partition will be empty or throw errors on read.

    // If the original location exists here, then it must be the extracted files
    // because in the previous step, we moved the previous original location
    // (containing the archived version of the files) to intermediateArchiveDir
    if (!pathExists(originalDir)) {
      try {
        console.printInfo("Moving " + intermediateExtractedDir + " to " + originalDir);
        fs.rename(intermediateExtractedDir, originalDir);
      } catch (IOException e) {
        throw new HiveException(e);
      }
    } else {
      console.printInfo(originalDir + " already exists. " +
          "Assuming it contains the extracted files in the partition");
    }

    for(Partition p: partitions) {
      setUnArchived(p);
      try {
        db.alterPartition(simpleDesc.getTableName(), p);
      } catch (InvalidOperationException e) {
        throw new HiveException(e);
      }
    }

    // If a failure happens here, the intermediate archive files won't be
    // deleted. The user will need to call unarchive again to clear those up.
    if(pathExists(intermediateArchivedDir)) {
      deleteDir(intermediateArchivedDir);
    }

    if(recovery) {
      console.printInfo("Recovery after UNARCHIVE succeeded");
    }

    return 0;
  }

  private void checkArchiveProperty(int partSpecLevel,
      boolean recovery, Partition p) throws HiveException {
    if (!ArchiveUtils.isArchived(p) && !recovery) {
      throw new HiveException("Partition " + p.getName()
          + " is not archived.");
    }
    int archiveLevel = ArchiveUtils.getArchivingLevel(p);
    if (partSpecLevel > archiveLevel) {
      throw new HiveException("Partition " + p.getName()
          + " is archived at level " + archiveLevel
          + ", and given partspec only has " + partSpecLevel
          + " specs.");
    }
  }

  private void msckAddPartitionsOneByOne(Hive db, Table table,
      List<CheckResult.PartitionResult> partsNotInMs, List<String> repairOutput) {
    for (CheckResult.PartitionResult part : partsNotInMs) {
      try {
        db.createPartition(table, Warehouse.makeSpecFromName(part.getPartitionName()));
        repairOutput.add("Repair: Added partition to metastore "
            + table.getTableName() + ':' + part.getPartitionName());
      } catch (Exception e) {
        LOG.warn("Repair error, could not add partition to metastore: ", e);
      }
    }
  }

  private int compact(Hive db, AlterTableSimpleDesc desc) throws HiveException {

    Table tbl = db.getTable(desc.getTableName());

    String partName = null;
    if (desc.getPartSpec() == null) {
      // Compaction can only be done on the whole table if the table is non-partitioned.
      if (tbl.isPartitioned()) {
        throw new HiveException(ErrorMsg.NO_COMPACTION_PARTITION);
      }
    } else {
      Map<String, String> partSpec = desc.getPartSpec();
      List<Partition> partitions = db.getPartitions(tbl, partSpec);
      if (partitions.size() > 1) {
        throw new HiveException(ErrorMsg.TOO_MANY_COMPACTION_PARTITIONS);
      } else if (partitions.size() == 0) {
        throw new HiveException(ErrorMsg.INVALID_PARTITION_SPEC);
      }
      partName = partitions.get(0).getName();
    }
    db.compact(tbl.getDbName(), tbl.getTableName(), partName, desc.getCompactionType());
    console.printInfo("Compaction enqueued.");
    return 0;
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
    CheckResult result = new CheckResult();
    List<String> repairOutput = new ArrayList<String>();
    try {
      HiveMetaStoreChecker checker = new HiveMetaStoreChecker(db);
      String[] names = Utilities.getDbTableName(msckDesc.getTableName());
      checker.checkMetastore(names[0], names[1], msckDesc.getPartSpecs(), result);
      List<CheckResult.PartitionResult> partsNotInMs = result.getPartitionsNotInMs();
      if (msckDesc.isRepairPartitions() && !partsNotInMs.isEmpty()) {
        Table table = db.getTable(msckDesc.getTableName());
        AddPartitionDesc apd = new AddPartitionDesc(
            table.getDbName(), table.getTableName(), false);
        try {
          for (CheckResult.PartitionResult part : partsNotInMs) {
            apd.addPartition(Warehouse.makeSpecFromName(part.getPartitionName()), null);
            repairOutput.add("Repair: Added partition to metastore "
                + msckDesc.getTableName() + ':' + part.getPartitionName());
          }
          db.createPartitions(apd);
        } catch (Exception e) {
          LOG.info("Could not bulk-add partitions to metastore; trying one by one", e);
          repairOutput.clear();
          msckAddPartitionsOneByOne(db, table, partsNotInMs, repairOutput);
        }
      }
    } catch (HiveException e) {
      LOG.warn("Failed to run metacheck: ", e);
      return 1;
    } catch (IOException e) {
      LOG.warn("Failed to run metacheck: ", e);
      return 1;
    } finally {
      BufferedWriter resultOut = null;
      try {
        Path resFile = new Path(msckDesc.getResFile());
        FileSystem fs = resFile.getFileSystem(conf);
        resultOut = new BufferedWriter(new OutputStreamWriter(fs
            .create(resFile)));

        boolean firstWritten = false;
        firstWritten |= writeMsckResult(result.getTablesNotInMs(),
            "Tables not in metastore:", resultOut, firstWritten);
        firstWritten |= writeMsckResult(result.getTablesNotOnFs(),
            "Tables missing on filesystem:", resultOut, firstWritten);
        firstWritten |= writeMsckResult(result.getPartitionsNotInMs(),
            "Partitions not in metastore:", resultOut, firstWritten);
        firstWritten |= writeMsckResult(result.getPartitionsNotOnFs(),
            "Partitions missing from filesystem:", resultOut, firstWritten);
        for (String rout : repairOutput) {
          if (firstWritten) {
            resultOut.write(terminator);
          } else {
            firstWritten = true;
          }
          resultOut.write(rout);
        }
      } catch (IOException e) {
        LOG.warn("Failed to save metacheck output: ", e);
        return 1;
      } finally {
        if (resultOut != null) {
          try {
            resultOut.close();
          } catch (IOException e) {
            LOG.warn("Failed to close output file: ", e);
            return 1;
          }
        }
      }
    }

    return 0;
  }

  /**
   * Write the result of msck to a writer.
   *
   * @param result
   *          The result we're going to write
   * @param msg
   *          Message to write.
   * @param out
   *          Writer to write to
   * @param wrote
   *          if any previous call wrote data
   * @return true if something was written
   * @throws IOException
   *           In case the writing fails
   */
  private boolean writeMsckResult(List<? extends Object> result, String msg,
      Writer out, boolean wrote) throws IOException {

    if (!result.isEmpty()) {
      if (wrote) {
        out.write(terminator);
      }

      out.write(msg);
      for (Object entry : result) {
        out.write(separator);
        out.write(entry.toString());
      }
      return true;
    }

    return false;
  }

  /**
   * Write a list of partitions to a file.
   *
   * @param db
   *          The database in question.
   * @param showParts
   *          These are the partitions we're interested in.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int showPartitions(Hive db, ShowPartitionsDesc showParts) throws HiveException {
    // get the partitions for the table and populate the output
    String tabName = showParts.getTabName();
    Table tbl = null;
    List<String> parts = null;

    tbl = db.getTable(tabName);

    if (!tbl.isPartitioned()) {
      throw new HiveException(ErrorMsg.TABLE_NOT_PARTITIONED, tabName);
    }
    if (showParts.getPartSpec() != null) {
      parts = db.getPartitionNames(tbl.getDbName(),
          tbl.getTableName(), showParts.getPartSpec(), (short) -1);
    } else {
      parts = db.getPartitionNames(tbl.getDbName(), tbl.getTableName(), (short) -1);
    }

    // write the results in the file
    DataOutputStream outStream = null;
    try {
      Path resFile = new Path(showParts.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);

      formatter.showTablePartitons(outStream, parts);

      outStream.close();
      outStream = null;
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "show partitions for table " + tabName);
    } finally {
      IOUtils.closeStream(outStream);
    }

    return 0;
  }

  private static final String[] DELIMITER_PREFIXES = new String[] {
      "FIELDS TERMINATED BY",
      "COLLECTION ITEMS TERMINATED BY",
      "MAP KEYS TERMINATED BY",
      "LINES TERMINATED BY",
      "NULL DEFINED AS"
  };

  /**
   * Write a statement of how to create a table to a file.
   *
   * @param db
   *          The database in question.
   * @param showCreateTbl
   *          This is the table we're interested in.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int showCreateTable(Hive db, ShowCreateTableDesc showCreateTbl) throws HiveException {
    // get the create table statement for the table and populate the output
    final String EXTERNAL = "external";
    final String TEMPORARY = "temporary";
    final String LIST_COLUMNS = "columns";
    final String TBL_COMMENT = "tbl_comment";
    final String LIST_PARTITIONS = "partitions";
    final String SORT_BUCKET = "sort_bucket";
    final String ROW_FORMAT = "row_format";
    final String TBL_LOCATION = "tbl_location";
    final String TBL_PROPERTIES = "tbl_properties";
    boolean needsLocation = true;
    StringBuilder createTab_str = new StringBuilder();

    String tableName = showCreateTbl.getTableName();
    Table tbl = db.getTable(tableName, false);
    DataOutputStream outStream = null;
    List<String> duplicateProps = new ArrayList<String>();
    try {
      Path resFile = new Path(showCreateTbl.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);

      needsLocation = doesTableNeedLocation(tbl);

      if (tbl.isView()) {
        String createTab_stmt = "CREATE VIEW `" + tableName + "` AS " + tbl.getViewExpandedText();
        outStream.writeBytes(createTab_stmt.toString());
        outStream.close();
        outStream = null;
        return 0;
      }

      createTab_str.append("CREATE <" + TEMPORARY + "><" + EXTERNAL + ">TABLE `");
      createTab_str.append(tableName + "`(\n");
      createTab_str.append("<" + LIST_COLUMNS + ">)\n");
      createTab_str.append("<" + TBL_COMMENT + ">\n");
      createTab_str.append("<" + LIST_PARTITIONS + ">\n");
      createTab_str.append("<" + SORT_BUCKET + ">\n");
      createTab_str.append("<" + ROW_FORMAT + ">\n");
      if (needsLocation) {
        createTab_str.append("LOCATION\n");
        createTab_str.append("<" + TBL_LOCATION + ">\n");
      }
      createTab_str.append("TBLPROPERTIES (\n");
      createTab_str.append("<" + TBL_PROPERTIES + ">)\n");
      ST createTab_stmt = new ST(createTab_str.toString());

      // For cases where the table is temporary
      String tbl_temp = "";
      if (tbl.isTemporary()) {
        duplicateProps.add("TEMPORARY");
        tbl_temp = "TEMPORARY ";
      }
      // For cases where the table is external
      String tbl_external = "";
      if (tbl.getTableType() == TableType.EXTERNAL_TABLE) {
        duplicateProps.add("EXTERNAL");
        tbl_external = "EXTERNAL ";
      }

      // Columns
      String tbl_columns = "";
      List<FieldSchema> cols = tbl.getCols();
      List<String> columns = new ArrayList<String>();
      for (FieldSchema col : cols) {
        String columnDesc = "  `" + col.getName() + "` " + col.getType();
        if (col.getComment() != null) {
          columnDesc = columnDesc + " COMMENT '" + escapeHiveCommand(col.getComment()) + "'";
        }
        columns.add(columnDesc);
      }
      tbl_columns = StringUtils.join(columns, ", \n");

      // Table comment
      String tbl_comment = "";
      String tabComment = tbl.getProperty("comment");
      if (tabComment != null) {
        duplicateProps.add("comment");
        tbl_comment = "COMMENT '" + escapeHiveCommand(tabComment) + "'";
      }

      // Partitions
      String tbl_partitions = "";
      List<FieldSchema> partKeys = tbl.getPartitionKeys();
      if (partKeys.size() > 0) {
        tbl_partitions += "PARTITIONED BY ( \n";
        List<String> partCols = new ArrayList<String>();
        for (FieldSchema partKey : partKeys) {
          String partColDesc = "  `" + partKey.getName() + "` " + partKey.getType();
          if (partKey.getComment() != null) {
            partColDesc = partColDesc + " COMMENT '" +
                escapeHiveCommand(partKey.getComment()) + "'";
          }
          partCols.add(partColDesc);
        }
        tbl_partitions += StringUtils.join(partCols, ", \n");
        tbl_partitions += ")";
      }

      // Clusters (Buckets)
      String tbl_sort_bucket = "";
      List<String> buckCols = tbl.getBucketCols();
      if (buckCols.size() > 0) {
        duplicateProps.add("SORTBUCKETCOLSPREFIX");
        tbl_sort_bucket += "CLUSTERED BY ( \n  ";
        tbl_sort_bucket += StringUtils.join(buckCols, ", \n  ");
        tbl_sort_bucket += ") \n";
        List<Order> sortCols = tbl.getSortCols();
        if (sortCols.size() > 0) {
          tbl_sort_bucket += "SORTED BY ( \n";
          // Order
          List<String> sortKeys = new ArrayList<String>();
          for (Order sortCol : sortCols) {
            String sortKeyDesc = "  " + sortCol.getCol() + " ";
            if (sortCol.getOrder() == BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_ASC) {
              sortKeyDesc = sortKeyDesc + "ASC";
            }
            else if (sortCol.getOrder() == BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_DESC) {
              sortKeyDesc = sortKeyDesc + "DESC";
            }
            sortKeys.add(sortKeyDesc);
          }
          tbl_sort_bucket += StringUtils.join(sortKeys, ", \n");
          tbl_sort_bucket += ") \n";
        }
        tbl_sort_bucket += "INTO " + tbl.getNumBuckets() + " BUCKETS";
      }

      // Row format (SerDe)
      StringBuilder tbl_row_format = new StringBuilder();
      StorageDescriptor sd = tbl.getTTable().getSd();
      SerDeInfo serdeInfo = sd.getSerdeInfo();
      tbl_row_format.append("ROW FORMAT");
      if (tbl.getStorageHandler() == null) {
        Map<String, String> serdeParams = serdeInfo.getParameters();
        String[] delimiters = new String[] {
            serdeParams.remove(serdeConstants.FIELD_DELIM),
            serdeParams.remove(serdeConstants.COLLECTION_DELIM),
            serdeParams.remove(serdeConstants.MAPKEY_DELIM),
            serdeParams.remove(serdeConstants.LINE_DELIM),
            serdeParams.remove(serdeConstants.SERIALIZATION_NULL_FORMAT)
        };
        serdeParams.remove(serdeConstants.SERIALIZATION_FORMAT);
        if (containsNonNull(delimiters)) {
          // There is a "serialization.format" property by default,
          // even with a delimited row format.
          // But our result will only cover the following four delimiters.
          tbl_row_format.append(" DELIMITED \n");

          // Warn:
          // If the four delimiters all exist in a CREATE TABLE query,
          // this following order needs to be strictly followed,
          // or the query will fail with a ParseException.
          for (int i = 0; i < DELIMITER_PREFIXES.length; i++) {
            if (delimiters[i] != null) {
              tbl_row_format.append("  ").append(DELIMITER_PREFIXES[i]).append(" '");
              tbl_row_format.append(escapeHiveCommand(StringEscapeUtils.escapeJava(delimiters[i])));
              tbl_row_format.append("' \n");
            }
          }
        } else {
          tbl_row_format.append(" SERDE \n  '" +
              escapeHiveCommand(serdeInfo.getSerializationLib()) + "' \n");
        }
        if (!serdeParams.isEmpty()) {
          appendSerdeParams(tbl_row_format, serdeParams).append(" \n");
        }
        tbl_row_format.append("STORED AS INPUTFORMAT \n  '" +
            escapeHiveCommand(sd.getInputFormat()) + "' \n");
        tbl_row_format.append("OUTPUTFORMAT \n  '" +
            escapeHiveCommand(sd.getOutputFormat()) + "'");
      } else {
        duplicateProps.add(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE);
        tbl_row_format.append(" SERDE \n  '" +
            escapeHiveCommand(serdeInfo.getSerializationLib()) + "' \n");
        tbl_row_format.append("STORED BY \n  '" + escapeHiveCommand(tbl.getParameters().get(
            org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE)) + "' \n");
        // SerDe Properties
        if (serdeInfo.getParametersSize() > 0) {
          appendSerdeParams(tbl_row_format, serdeInfo.getParameters());
        }
      }
      String tbl_location = "  '" + escapeHiveCommand(sd.getLocation()) + "'";

      // Table properties
      String tbl_properties = "";
      if (!tbl.getParameters().isEmpty()) {
        Map<String, String> properties = new TreeMap<String, String>(tbl.getParameters());
        List<String> realProps = new ArrayList<String>();
        for (String key : properties.keySet()) {
          if (properties.get(key) != null && !duplicateProps.contains(key)) {
            realProps.add("  '" + key + "'='" +
                escapeHiveCommand(StringEscapeUtils.escapeJava(properties.get(key))) + "'");
          }
        }
        tbl_properties += StringUtils.join(realProps, ", \n");
      }

      createTab_stmt.add(TEMPORARY, tbl_temp);
      createTab_stmt.add(EXTERNAL, tbl_external);
      createTab_stmt.add(LIST_COLUMNS, tbl_columns);
      createTab_stmt.add(TBL_COMMENT, tbl_comment);
      createTab_stmt.add(LIST_PARTITIONS, tbl_partitions);
      createTab_stmt.add(SORT_BUCKET, tbl_sort_bucket);
      createTab_stmt.add(ROW_FORMAT, tbl_row_format);
      // Table location should not be printed with hbase backed tables
      if (needsLocation) {
        createTab_stmt.add(TBL_LOCATION, tbl_location);
      }
      createTab_stmt.add(TBL_PROPERTIES, tbl_properties);

      outStream.writeBytes(createTab_stmt.render());
      outStream.close();
      outStream = null;
    } catch (FileNotFoundException e) {
      LOG.info("show create table: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.info("show create table: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e);
    } finally {
      IOUtils.closeStream(outStream);
    }

    return 0;
  }

  private boolean containsNonNull(String[] values) {
    for (String value : values) {
      if (value != null) {
        return true;
      }
    }
    return false;
  }

  private StringBuilder appendSerdeParams(StringBuilder builder, Map<String, String> serdeParam) {
    serdeParam = new TreeMap<String, String>(serdeParam);
    builder.append("WITH SERDEPROPERTIES ( \n");
    List<String> serdeCols = new ArrayList<String>();
    for (Entry<String, String> entry : serdeParam.entrySet()) {
      serdeCols.add("  '" + entry.getKey() + "'='"
          + escapeHiveCommand(StringEscapeUtils.escapeJava(entry.getValue())) + "'");
    }
    builder.append(StringUtils.join(serdeCols, ", \n")).append(')');
    return builder;
  }

  /**
   * Write a list of indexes to a file.
   *
   * @param db
   *          The database in question.
   * @param showIndexes
   *          These are the indexes we're interested in.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int showIndexes(Hive db, ShowIndexesDesc showIndexes) throws HiveException {
    // get the indexes for the table and populate the output
    String tableName = showIndexes.getTableName();
    Table tbl = null;
    List<Index> indexes = null;

    tbl = db.getTable(tableName);

    indexes = db.getIndexes(tbl.getDbName(), tbl.getTableName(), (short) -1);

    // write the results in the file
    DataOutputStream outStream = null;
    try {
      Path resFile = new Path(showIndexes.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);

      if (showIndexes.isFormatted()) {
        // column headers
        outStream.writeBytes(MetaDataFormatUtils.getIndexColumnsHeader());
        outStream.write(terminator);
        outStream.write(terminator);
      }

      for (Index index : indexes)
      {
        outStream.writeBytes(MetaDataFormatUtils.getAllColumnsInformation(index));
      }

      outStream.close();
      outStream = null;

    } catch (FileNotFoundException e) {
      LOG.info("show indexes: " + stringifyException(e));
      throw new HiveException(e.toString());
    } catch (IOException e) {
      LOG.info("show indexes: " + stringifyException(e));
      throw new HiveException(e.toString());
    } catch (Exception e) {
      throw new HiveException(e.toString());
    } finally {
      IOUtils.closeStream(outStream);
    }

    return 0;
  }

  /**
   * Write a list of the available databases to a file.
   *
   * @param showDatabasesDesc
   *          These are the databases we're interested in.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int showDatabases(Hive db, ShowDatabasesDesc showDatabasesDesc) throws HiveException {
    // get the databases for the desired pattern - populate the output stream
    List<String> databases = null;
    if (showDatabasesDesc.getPattern() != null) {
      LOG.info("pattern: " + showDatabasesDesc.getPattern());
      databases = db.getDatabasesByPattern(showDatabasesDesc.getPattern());
    } else {
      databases = db.getAllDatabases();
    }
    LOG.info("results : " + databases.size());

    // write the results in the file
    DataOutputStream outStream = null;
    try {
      Path resFile = new Path(showDatabasesDesc.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);

      formatter.showDatabases(outStream, databases);
      outStream.close();
      outStream = null;
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "show databases");
    } finally {
      IOUtils.closeStream(outStream);
    }
    return 0;
  }

  /**
   * Write a list of the tables in the database to a file.
   *
   * @param db
   *          The database in question.
   * @param showTbls
   *          These are the tables we're interested in.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int showTables(Hive db, ShowTablesDesc showTbls) throws HiveException {
    // get the tables for the desired pattern - populate the output stream
    List<String> tbls = null;
    String dbName = showTbls.getDbName();

    if (!db.databaseExists(dbName)) {
      throw new HiveException(ErrorMsg.DATABASE_NOT_EXISTS, dbName);
    }
    if (showTbls.getPattern() != null) {
      LOG.info("pattern: " + showTbls.getPattern());
      tbls = db.getTablesByPattern(dbName, showTbls.getPattern());
      LOG.info("results : " + tbls.size());
    } else {
      tbls = db.getAllTables(dbName);
    }

    // write the results in the file
    DataOutputStream outStream = null;
    try {
      Path resFile = new Path(showTbls.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);

      SortedSet<String> sortedTbls = new TreeSet<String>(tbls);
      formatter.showTables(outStream, sortedTbls);
      outStream.close();
      outStream = null;
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "in database" + dbName);
    } finally {
      IOUtils.closeStream(outStream);
    }
    return 0;
  }

  public int showColumns(Hive db, ShowColumnsDesc showCols)
      throws HiveException {

    Table table = db.getTable(showCols.getTableName());

    // write the results in the file
    DataOutputStream outStream = null;
    try {
      Path resFile = new Path(showCols.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);

      List<FieldSchema> cols = table.getCols();
      cols.addAll(table.getPartCols());
      // In case the query is served by HiveServer2, don't pad it with spaces,
      // as HiveServer2 output is consumed by JDBC/ODBC clients.
      boolean isOutputPadded = !SessionState.get().isHiveServerQuery();
      outStream.writeBytes(MetaDataFormatUtils.getAllColumnsInformation(
          cols, false, isOutputPadded, null));
      outStream.close();
      outStream = null;
    } catch (IOException e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR);
    } finally {
      IOUtils.closeStream(outStream);
    }
    return 0;
  }

  /**
   * Write a list of the user defined functions to a file.
   *
   * @param showFuncs
   *          are the functions we're interested in.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int showFunctions(ShowFunctionsDesc showFuncs) throws HiveException {
    // get the tables for the desired patten - populate the output stream
    Set<String> funcs = null;
    if (showFuncs.getPattern() != null) {
      LOG.info("pattern: " + showFuncs.getPattern());
      if (showFuncs.getIsLikePattern()) {
         funcs = FunctionRegistry.getFunctionNamesByLikePattern(showFuncs.getPattern());
      } else {
         console.printInfo("SHOW FUNCTIONS is deprecated, please use SHOW FUNCTIONS LIKE instead.");
         funcs = FunctionRegistry.getFunctionNames(showFuncs.getPattern());
      }
      LOG.info("results : " + funcs.size());
    } else {
      funcs = FunctionRegistry.getFunctionNames();
    }

    // write the results in the file
    DataOutputStream outStream = null;
    try {
      Path resFile = new Path(showFuncs.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);
      SortedSet<String> sortedFuncs = new TreeSet<String>(funcs);
      // To remove the primitive types
      sortedFuncs.removeAll(serdeConstants.PrimitiveTypes);
      Iterator<String> iterFuncs = sortedFuncs.iterator();

      while (iterFuncs.hasNext()) {
        // create a row per table name
        outStream.writeBytes(iterFuncs.next());
        outStream.write(terminator);
      }
      outStream.close();
      outStream = null;
    } catch (FileNotFoundException e) {
      LOG.warn("show function: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show function: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e);
    } finally {
      IOUtils.closeStream(outStream);
    }
    return 0;
  }

  /**
   * Write a list of the current locks to a file.
   *
   * @param showLocks
   *          the locks we're interested in.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int showLocks(ShowLocksDesc showLocks) throws HiveException {
    Context ctx = driverContext.getCtx();
    HiveTxnManager txnManager = ctx.getHiveTxnManager();
    HiveLockManager lockMgr = txnManager.getLockManager();

    if (txnManager.useNewShowLocksFormat()) return showLocksNewFormat(showLocks, lockMgr);

    boolean isExt = showLocks.isExt();
    if (lockMgr == null) {
      throw new HiveException("show Locks LockManager not specified");
    }

    // write the results in the file
    DataOutputStream outStream = null;
    try {
      Path resFile = new Path(showLocks.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);
      List<HiveLock> locks = null;

      if (showLocks.getTableName() == null) {
        // TODO should be doing security check here.  Users should not be
        // able to see each other's locks.
        locks = lockMgr.getLocks(false, isExt);
      }
      else {
        locks = lockMgr.getLocks(getHiveObject(showLocks.getTableName(),
            showLocks.getPartSpec()),
            true, isExt);
      }

      Collections.sort(locks, new Comparator<HiveLock>() {

        @Override
        public int compare(HiveLock o1, HiveLock o2) {
          int cmp = o1.getHiveLockObject().getName().compareTo(o2.getHiveLockObject().getName());
          if (cmp == 0) {
            if (o1.getHiveLockMode() == o2.getHiveLockMode()) {
              return cmp;
            }
            // EXCLUSIVE locks occur before SHARED locks
            if (o1.getHiveLockMode() == HiveLockMode.EXCLUSIVE) {
              return -1;
            }
            return +1;
          }
          return cmp;
        }

      });

      Iterator<HiveLock> locksIter = locks.iterator();

      while (locksIter.hasNext()) {
        HiveLock lock = locksIter.next();
        outStream.writeBytes(lock.getHiveLockObject().getDisplayName());
        outStream.write(separator);
        outStream.writeBytes(lock.getHiveLockMode().toString());
        if (isExt) {
          HiveLockObjectData lockData = lock.getHiveLockObject().getData();
          if (lockData != null) {
            outStream.write(terminator);
            outStream.writeBytes("LOCK_QUERYID:" + lockData.getQueryId());
            outStream.write(terminator);
            outStream.writeBytes("LOCK_TIME:" + lockData.getLockTime());
            outStream.write(terminator);
            outStream.writeBytes("LOCK_MODE:" + lockData.getLockMode());
            outStream.write(terminator);
            outStream.writeBytes("LOCK_QUERYSTRING:" + lockData.getQueryStr());
          }
        }
        outStream.write(terminator);
      }
      outStream.close();
      outStream = null;
    } catch (FileNotFoundException e) {
      LOG.warn("show function: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show function: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString(), e);
    } finally {
      IOUtils.closeStream(outStream);
    }
    return 0;
  }

  private int showLocksNewFormat(ShowLocksDesc showLocks, HiveLockManager lm)
      throws  HiveException {

    DbLockManager lockMgr;
    if (!(lm instanceof DbLockManager)) {
      throw new RuntimeException("New lock format only supported with db lock manager.");
    }
    lockMgr = (DbLockManager)lm;

    ShowLocksResponse rsp = lockMgr.getLocks();

    // write the results in the file
    DataOutputStream os = null;
    try {
      Path resFile = new Path(showLocks.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      os = fs.create(resFile);

      // Write a header
      os.writeBytes("Lock ID");
      os.write(separator);
      os.writeBytes("Database");
      os.write(separator);
      os.writeBytes("Table");
      os.write(separator);
      os.writeBytes("Partition");
      os.write(separator);
      os.writeBytes("State");
      os.write(separator);
      os.writeBytes("Type");
      os.write(separator);
      os.writeBytes("Transaction ID");
      os.write(separator);
      os.writeBytes("Last Hearbeat");
      os.write(separator);
      os.writeBytes("Acquired At");
      os.write(separator);
      os.writeBytes("User");
      os.write(separator);
      os.writeBytes("Hostname");
      os.write(terminator);

      List<ShowLocksResponseElement> locks = rsp.getLocks();
      if (locks != null) {
        for (ShowLocksResponseElement lock : locks) {
          os.writeBytes(Long.toString(lock.getLockid()));
          os.write(separator);
          os.writeBytes(lock.getDbname());
          os.write(separator);
          os.writeBytes((lock.getTablename() == null) ? "NULL" : lock.getTablename());
          os.write(separator);
          os.writeBytes((lock.getPartname() == null) ? "NULL" : lock.getPartname());
          os.write(separator);
          os.writeBytes(lock.getState().toString());
          os.write(separator);
          os.writeBytes(lock.getType().toString());
          os.write(separator);
          os.writeBytes((lock.getTxnid() == 0) ? "NULL" : Long.toString(lock.getTxnid()));
          os.write(separator);
          os.writeBytes(Long.toString(lock.getLastheartbeat()));
          os.write(separator);
          os.writeBytes((lock.getAcquiredat() == 0) ? "NULL" : Long.toString(lock.getAcquiredat()));
          os.write(separator);
          os.writeBytes(lock.getUser());
          os.write(separator);
          os.writeBytes(lock.getHostname());
          os.write(separator);
          os.write(terminator);
        }

      }

      os.close();
      os = null;
    } catch (FileNotFoundException e) {
      LOG.warn("show function: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show function: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    } finally {
      IOUtils.closeStream(os);
    }
    return 0;
  }

  private int showCompactions(ShowCompactionsDesc desc) throws HiveException {
    // Call the metastore to get the currently queued and running compactions.
    ShowCompactResponse rsp = db.showCompactions();

    // Write the results into the file
    DataOutputStream os = null;
    try {
      Path resFile = new Path(desc.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      os = fs.create(resFile);

      // Write a header
      os.writeBytes("Database");
      os.write(separator);
      os.writeBytes("Table");
      os.write(separator);
      os.writeBytes("Partition");
      os.write(separator);
      os.writeBytes("Type");
      os.write(separator);
      os.writeBytes("State");
      os.write(separator);
      os.writeBytes("Worker");
      os.write(separator);
      os.writeBytes("Start Time");
      os.write(terminator);

      if (rsp.getCompacts() != null) {
        for (ShowCompactResponseElement e : rsp.getCompacts()) {
          os.writeBytes(e.getDbname());
          os.write(separator);
          os.writeBytes(e.getTablename());
          os.write(separator);
          String part = e.getPartitionname();
          os.writeBytes(part == null ? "NULL" : part);
          os.write(separator);
          os.writeBytes(e.getType().toString());
          os.write(separator);
          os.writeBytes(e.getState());
          os.write(separator);
          String wid = e.getWorkerid();
          os.writeBytes(wid == null ? "NULL" : wid);
          os.write(separator);
          os.writeBytes(Long.toString(e.getStart()));
          os.write(terminator);
        }
      }
      os.close();
    } catch (IOException e) {
      LOG.warn("show compactions: " + stringifyException(e));
      return 1;
    } finally {
      IOUtils.closeStream(os);
    }
    return 0;
  }

  private int showTxns(ShowTxnsDesc desc) throws HiveException {
    // Call the metastore to get the currently queued and running compactions.
    GetOpenTxnsInfoResponse rsp = db.showTransactions();

    // Write the results into the file
    DataOutputStream os = null;
    try {
      Path resFile = new Path(desc.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      os = fs.create(resFile);

      // Write a header
      os.writeBytes("Transaction ID");
      os.write(separator);
      os.writeBytes("Transaction State");
      os.write(separator);
      os.writeBytes("User");
      os.write(separator);
      os.writeBytes("Hostname");
      os.write(terminator);

      for (TxnInfo txn : rsp.getOpen_txns()) {
        os.writeBytes(Long.toString(txn.getId()));
        os.write(separator);
        os.writeBytes(txn.getState().toString());
        os.write(separator);
        os.writeBytes(txn.getUser());
        os.write(separator);
        os.writeBytes(txn.getHostname());
        os.write(terminator);
      }
      os.close();
    } catch (IOException e) {
      LOG.warn("show transactions: " + stringifyException(e));
      return 1;
    } finally {
      IOUtils.closeStream(os);
    }
    return 0;
  }

   /**
   * Lock the table/partition specified
   *
   * @param lockTbl
   *          the table/partition to be locked along with the mode
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int lockTable(LockTableDesc lockTbl) throws HiveException {
    Context ctx = driverContext.getCtx();
    HiveTxnManager txnManager = ctx.getHiveTxnManager();
    if (!txnManager.supportsExplicitLock()) {
      throw new HiveException(ErrorMsg.LOCK_REQUEST_UNSUPPORTED,
          conf.getVar(HiveConf.ConfVars.HIVE_TXN_MANAGER));
    }
    HiveLockManager lockMgr = txnManager.getLockManager();
    if (lockMgr == null) {
      throw new HiveException("lock Table LockManager not specified");
    }

    HiveLockMode mode = HiveLockMode.valueOf(lockTbl.getMode());
    String tabName = lockTbl.getTableName();
    Table  tbl = db.getTable(tabName);
    if (tbl == null) {
      throw new HiveException("Table " + tabName + " does not exist ");
    }

    Map<String, String> partSpec = lockTbl.getPartSpec();
    HiveLockObjectData lockData =
        new HiveLockObjectData(lockTbl.getQueryId(),
            String.valueOf(System.currentTimeMillis()),
            "EXPLICIT",
            lockTbl.getQueryStr());

    if (partSpec == null) {
      HiveLock lck = lockMgr.lock(new HiveLockObject(tbl, lockData), mode, true);
      if (lck == null) {
        return 1;
      }
      return 0;
    }

    Partition par = db.getPartition(tbl, partSpec, false);
    if (par == null) {
      throw new HiveException("Partition " + partSpec + " for table " + tabName + " does not exist");
    }
    HiveLock lck = lockMgr.lock(new HiveLockObject(par, lockData), mode, true);
    if (lck == null) {
      return 1;
    }
    return 0;
  }

  /**
   * Lock the database
   *
   * @param lockDb
   *          the database to be locked along with the mode
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int lockDatabase(LockDatabaseDesc lockDb) throws HiveException {
    Context ctx = driverContext.getCtx();
    HiveTxnManager txnManager = ctx.getHiveTxnManager();
    if (!txnManager.supportsExplicitLock()) {
      throw new HiveException(ErrorMsg.LOCK_REQUEST_UNSUPPORTED,
          conf.getVar(HiveConf.ConfVars.HIVE_TXN_MANAGER));
    }
    HiveLockManager lockMgr = txnManager.getLockManager();
    if (lockMgr == null) {
      throw new HiveException("lock Database LockManager not specified");
    }

    HiveLockMode mode = HiveLockMode.valueOf(lockDb.getMode());
    String dbName = lockDb.getDatabaseName();

    Database dbObj = db.getDatabase(dbName);
    if (dbObj == null) {
      throw new HiveException("Database " + dbName + " does not exist ");
    }

    HiveLockObjectData lockData =
        new HiveLockObjectData(lockDb.getQueryId(),
            String.valueOf(System.currentTimeMillis()),
            "EXPLICIT", lockDb.getQueryStr());

    HiveLock lck = lockMgr.lock(new HiveLockObject(dbObj.getName(), lockData), mode, true);
    if (lck == null) {
      return 1;
    }
    return 0;
  }

  /**
   * Unlock the database specified
   *
   * @param unlockDb
   *          the database to be unlocked
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int unlockDatabase(UnlockDatabaseDesc unlockDb) throws HiveException {
    Context ctx = driverContext.getCtx();
    HiveTxnManager txnManager = ctx.getHiveTxnManager();
    if (!txnManager.supportsExplicitLock()) {
      throw new HiveException(ErrorMsg.LOCK_REQUEST_UNSUPPORTED,
          conf.getVar(HiveConf.ConfVars.HIVE_TXN_MANAGER));
    }
    HiveLockManager lockMgr = txnManager.getLockManager();
    if (lockMgr == null) {
      throw new HiveException("unlock Database LockManager not specified");
    }

    String dbName = unlockDb.getDatabaseName();

    Database dbObj = db.getDatabase(dbName);
    if (dbObj == null) {
      throw new HiveException("Database " + dbName + " does not exist ");
    }
    HiveLockObject obj = new HiveLockObject(dbObj.getName(), null);

    List<HiveLock> locks = lockMgr.getLocks(obj, false, false);
    if ((locks == null) || (locks.isEmpty())) {
      throw new HiveException("Database " + dbName + " is not locked ");
    }

    for (HiveLock lock: locks) {
      lockMgr.unlock(lock);

    }
    return 0;
  }

  private HiveLockObject getHiveObject(String tabName,
      Map<String, String> partSpec) throws HiveException {
    Table  tbl = db.getTable(tabName);
    if (tbl == null) {
      throw new HiveException("Table " + tabName + " does not exist ");
    }

    HiveLockObject obj = null;

    if  (partSpec == null) {
      obj = new HiveLockObject(tbl, null);
    }
    else {
      Partition par = db.getPartition(tbl, partSpec, false);
      if (par == null) {
        throw new HiveException("Partition " + partSpec + " for table " + tabName + " does not exist");
      }
      obj = new HiveLockObject(par, null);
    }
    return obj;
  }

  /**
   * Unlock the table/partition specified
   *
   * @param unlockTbl
   *          the table/partition to be unlocked
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int unlockTable(UnlockTableDesc unlockTbl) throws HiveException {
    Context ctx = driverContext.getCtx();
    HiveTxnManager txnManager = ctx.getHiveTxnManager();
    if (!txnManager.supportsExplicitLock()) {
      throw new HiveException(ErrorMsg.LOCK_REQUEST_UNSUPPORTED,
          conf.getVar(HiveConf.ConfVars.HIVE_TXN_MANAGER));
    }
    HiveLockManager lockMgr = txnManager.getLockManager();
    if (lockMgr == null) {
      throw new HiveException("unlock Table LockManager not specified");
    }

    String tabName = unlockTbl.getTableName();
    HiveLockObject obj = getHiveObject(tabName, unlockTbl.getPartSpec());

    List<HiveLock> locks = lockMgr.getLocks(obj, false, false);
    if ((locks == null) || (locks.isEmpty())) {
      throw new HiveException("Table " + tabName + " is not locked ");
    }
    Iterator<HiveLock> locksIter = locks.iterator();
    while (locksIter.hasNext()) {
      HiveLock lock = locksIter.next();
      lockMgr.unlock(lock);
    }

    return 0;
  }

  /**
   * Shows a description of a function.
   *
   * @param descFunc
   *          is the function we are describing
   * @throws HiveException
   */
  private int describeFunction(DescFunctionDesc descFunc) throws HiveException, SQLException {
    String funcName = descFunc.getName();

    // write the results in the file
    DataOutputStream outStream = null;
    try {
      Path resFile = new Path(descFunc.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);

      // get the function documentation
      Description desc = null;
      Class<?> funcClass = null;
      FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(funcName);
      if (functionInfo != null) {
        funcClass = functionInfo.getFunctionClass();
      }
      if (funcClass != null) {
        desc = AnnotationUtils.getAnnotation(funcClass, Description.class);
      }
      if (desc != null) {
        outStream.writeBytes(desc.value().replace("_FUNC_", funcName));
        if (descFunc.isExtended()) {
          Set<String> synonyms = FunctionRegistry.getFunctionSynonyms(funcName);
          if (synonyms.size() > 0) {
            outStream.writeBytes("\nSynonyms: " + join(synonyms, ", "));
          }
          if (desc.extended().length() > 0) {
            outStream.writeBytes("\n"
                + desc.extended().replace("_FUNC_", funcName));
          }
        }
      } else {
        if (funcClass != null) {
          outStream.writeBytes("There is no documentation for function '"
              + funcName + "'");
        } else {
          outStream.writeBytes("Function '" + funcName + "' does not exist.");
        }
      }

      outStream.write(terminator);

      outStream.close();
      outStream = null;
    } catch (FileNotFoundException e) {
      LOG.warn("describe function: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("describe function: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e);
    } finally {
      IOUtils.closeStream(outStream);
    }
    return 0;
  }

  private int descDatabase(DescDatabaseDesc descDatabase) throws HiveException {
    DataOutputStream outStream = null;
    try {
      Path resFile = new Path(descDatabase.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);

      Database database = db.getDatabase(descDatabase.getDatabaseName());

      if (database == null) {
        throw new HiveException(ErrorMsg.DATABASE_NOT_EXISTS, descDatabase.getDatabaseName());
      }
      Map<String, String> params = null;
      if (descDatabase.isExt()) {
        params = database.getParameters();
      }

      // If this is a q-test, let's order the params map (lexicographically) by
      // key. This is to get consistent param ordering between Java7 and Java8.
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_IN_TEST) &&
          params != null) {
        params = new TreeMap<String, String>(params);
      }

      String location = database.getLocationUri();
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_IN_TEST)) {
        location = "location/in/test";
      }
      PrincipalType ownerType = database.getOwnerType();
      formatter.showDatabaseDescription(outStream, database.getName(),
          database.getDescription(), location,
          database.getOwnerName(), (null == ownerType) ? null : ownerType.name(), params);

      outStream.close();
      outStream = null;
    } catch (IOException e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR);
    } finally {
      IOUtils.closeStream(outStream);
    }
    return 0;
  }

  /**
   * Write the status of tables to a file.
   *
   * @param db
   *          The database in question.
   * @param showTblStatus
   *          tables we are interested in
   * @return Return 0 when execution succeeds and above 0 if it fails.
   */
  private int showTableStatus(Hive db, ShowTableStatusDesc showTblStatus) throws HiveException {
    // get the tables for the desired pattern - populate the output stream
    List<Table> tbls = new ArrayList<Table>();
    Map<String, String> part = showTblStatus.getPartSpec();
    Partition par = null;
    if (part != null) {
      Table tbl = db.getTable(showTblStatus.getDbName(), showTblStatus.getPattern());
      par = db.getPartition(tbl, part, false);
      if (par == null) {
        throw new HiveException("Partition " + part + " for table "
            + showTblStatus.getPattern() + " does not exist.");
      }
      tbls.add(tbl);
    } else {
      LOG.info("pattern: " + showTblStatus.getPattern());
      List<String> tblStr = db.getTablesForDb(showTblStatus.getDbName(),
          showTblStatus.getPattern());
      SortedSet<String> sortedTbls = new TreeSet<String>(tblStr);
      Iterator<String> iterTbls = sortedTbls.iterator();
      while (iterTbls.hasNext()) {
        // create a row per table name
        String tblName = iterTbls.next();
        Table tbl = db.getTable(showTblStatus.getDbName(), tblName);
        tbls.add(tbl);
      }
      LOG.info("results : " + tblStr.size());
    }

    // write the results in the file
    DataOutputStream outStream = null;
    try {
      Path resFile = new Path(showTblStatus.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);

      formatter.showTableStatus(outStream, db, conf, tbls, part, par);

      outStream.close();
      outStream = null;
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "show table status");
    } finally {
      IOUtils.closeStream(outStream);
    }
    return 0;
  }

  /**
   * Write the properties of a table to a file.
   *
   * @param db
   *          The database in question.
   * @param showTblPrpt
   *          This is the table we're interested in.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int showTableProperties(Hive db, ShowTblPropertiesDesc showTblPrpt) throws HiveException {
    String tableName = showTblPrpt.getTableName();

    // show table properties - populate the output stream
    Table tbl = db.getTable(tableName, false);
    try {
      if (tbl == null) {
        String errMsg = "Table " + tableName + " does not exist";
        writeToFile(errMsg, showTblPrpt.getResFile());
        return 0;
      }

      LOG.info("DDLTask: show properties for " + tbl.getTableName());

      StringBuilder builder = new StringBuilder();
      String propertyName = showTblPrpt.getPropertyName();
      if (propertyName != null) {
        String propertyValue = tbl.getProperty(propertyName);
        if (propertyValue == null) {
          String errMsg = "Table " + tableName + " does not have property: " + propertyName;
          builder.append(errMsg);
        }
        else {
          builder.append(propertyValue);
        }
      }
      else {
        Map<String, String> properties = new TreeMap<String, String>(tbl.getParameters());
        for (Entry<String, String> entry : properties.entrySet()) {
          appendNonNull(builder, entry.getKey(), true);
          appendNonNull(builder, entry.getValue());
        }
      }

      LOG.info("DDLTask: written data for showing properties of " + tbl.getTableName());
      writeToFile(builder.toString(), showTblPrpt.getResFile());

    } catch (FileNotFoundException e) {
      LOG.info("show table properties: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.info("show table properties: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e);
    }

    return 0;
  }

  private void writeToFile(String data, String file) throws IOException {
    Path resFile = new Path(file);
    FileSystem fs = resFile.getFileSystem(conf);
    FSDataOutputStream out = fs.create(resFile);
    try {
      if (data != null && !data.isEmpty()) {
        OutputStreamWriter writer = new OutputStreamWriter(out, "UTF-8");
        writer.write(data);
        writer.write((char) terminator);
        writer.flush();
      }
    } finally {
      IOUtils.closeStream(out);
    }
  }

  /**
   * Write the description of a table to a file.
   *
   * @param db
   *          The database in question.
   * @param descTbl
   *          This is the table we're interested in.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int describeTable(Hive db, DescTableDesc descTbl) throws HiveException {
    String colPath = descTbl.getColumnPath();
    String tableName = descTbl.getTableName();

    // describe the table - populate the output stream
    Table tbl = db.getTable(tableName, false);
    Partition part = null;
    DataOutputStream outStream = null;
    try {
      Path resFile = new Path(descTbl.getResFile());
      if (tbl == null) {
        FileSystem fs = resFile.getFileSystem(conf);
        outStream = fs.create(resFile);
        outStream.close();
        outStream = null;
        throw new HiveException(ErrorMsg.INVALID_TABLE, tableName);
      }
      if (descTbl.getPartSpec() != null) {
        part = db.getPartition(tbl, descTbl.getPartSpec(), false);
        if (part == null) {
          FileSystem fs = resFile.getFileSystem(conf);
          outStream = fs.create(resFile);
          outStream.close();
          outStream = null;
          throw new HiveException(ErrorMsg.INVALID_PARTITION,
              StringUtils.join(descTbl.getPartSpec().keySet(), ','), tableName);
        }
        tbl = part.getTable();
      }
    } catch (IOException e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, tableName);
    } finally {
      IOUtils.closeStream(outStream);
    }

    try {
      LOG.info("DDLTask: got data for " + tbl.getTableName());
      Path resFile = new Path(descTbl.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);

      List<FieldSchema> cols = null;
      List<ColumnStatisticsObj> colStats = null;

      Deserializer deserializer = tbl.getDeserializer(true);
      if (deserializer instanceof AbstractSerDe) {
        String errorMsgs = ((AbstractSerDe) deserializer).getConfigurationErrors();
        if (errorMsgs != null && !errorMsgs.isEmpty()) {
          throw new SQLException(errorMsgs);
        }
      }

      if (colPath.equals(tableName)) {
        cols = (part == null || tbl.getTableType() == TableType.VIRTUAL_VIEW) ?
            tbl.getCols() : part.getCols();

        if (!descTbl.isFormatted()) {
          cols.addAll(tbl.getPartCols());
        }
      } else {
        cols = Hive.getFieldsFromDeserializer(colPath, deserializer);
        if (descTbl.isFormatted()) {
          // when column name is specified in describe table DDL, colPath will
          // will be table_name.column_name
          String colName = colPath.split("\\.")[1];
          String[] dbTab = Utilities.getDbTableName(tableName);
          List<String> colNames = new ArrayList<String>();
          colNames.add(colName.toLowerCase());
          if (null == part) {
            colStats = db.getTableColumnStatistics(dbTab[0].toLowerCase(), dbTab[1].toLowerCase(), colNames);
          } else {
            List<String> partitions = new ArrayList<String>();
            partitions.add(part.getName());
            colStats = db.getPartitionColumnStatistics(dbTab[0].toLowerCase(), dbTab[1].toLowerCase(), partitions, colNames).get(part.getName());
          }
        }
      }

      fixDecimalColumnTypeName(cols);
      // In case the query is served by HiveServer2, don't pad it with spaces,
      // as HiveServer2 output is consumed by JDBC/ODBC clients.
      boolean isOutputPadded = !SessionState.get().isHiveServerQuery();
      formatter.describeTable(outStream, colPath, tableName, tbl, part,
          cols, descTbl.isFormatted(), descTbl.isExt(),
          descTbl.isPretty(), isOutputPadded, colStats);

      LOG.info("DDLTask: written data for " + tbl.getTableName());
      outStream.close();
      outStream = null;

    } catch (SQLException e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, tableName);
    } catch (IOException e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, tableName);
    } finally {
      IOUtils.closeStream(outStream);
    }

    return 0;
  }

  /**
   * Fix the type name of a column of type decimal w/o precision/scale specified. This makes
   * the describe table show "decimal(10,0)" instead of "decimal" even if the type stored
   * in metastore is "decimal", which is possible with previous hive.
   *
   * @param cols columns that to be fixed as such
   */
  private static void fixDecimalColumnTypeName(List<FieldSchema> cols) {
    for (FieldSchema col : cols) {
      if (serdeConstants.DECIMAL_TYPE_NAME.equals(col.getType())) {
        col.setType(DecimalTypeInfo.getQualifiedName(HiveDecimal.USER_DEFAULT_PRECISION,
            HiveDecimal.USER_DEFAULT_SCALE));
      }
    }
  }

  static String writeGrantInfo(List<HivePrivilegeInfo> privileges, boolean testMode) {
    if (privileges == null || privileges.isEmpty()) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    //sort the list to get sorted (deterministic) output (for ease of testing)
    Collections.sort(privileges, new Comparator<HivePrivilegeInfo>() {
      @Override
      public int compare(HivePrivilegeInfo o1, HivePrivilegeInfo o2) {
        int compare = o1.getObject().compareTo(o2.getObject());
        if (compare == 0) {
          compare = o1.getPrincipal().compareTo(o2.getPrincipal());
        }
        if (compare == 0) {
          compare = o1.getPrivilege().compareTo(o2.getPrivilege());
        }
        return compare;
      }
    });

    for (HivePrivilegeInfo privilege : privileges) {
      HivePrincipal principal = privilege.getPrincipal();
      HivePrivilegeObject resource = privilege.getObject();
      HivePrincipal grantor = privilege.getGrantorPrincipal();

      appendNonNull(builder, resource.getDbname(), true);
      appendNonNull(builder, resource.getObjectName());
      appendNonNull(builder, resource.getPartKeys());
      appendNonNull(builder, resource.getColumns());
      appendNonNull(builder, principal.getName());
      appendNonNull(builder, principal.getType());
      appendNonNull(builder, privilege.getPrivilege().getName());
      appendNonNull(builder, privilege.isGrantOption());
      appendNonNull(builder, testMode ? -1 : privilege.getGrantTime() * 1000L);
      appendNonNull(builder, grantor.getName());
    }
    return builder.toString();
  }

  static String writeRoleGrantsInfo(List<RolePrincipalGrant> roleGrants, boolean testMode) {
    if (roleGrants == null || roleGrants.isEmpty()) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    //sort the list to get sorted (deterministic) output (for ease of testing)
    Collections.sort(roleGrants);
    for (RolePrincipalGrant roleGrant : roleGrants) {
      appendNonNull(builder, roleGrant.getRoleName(), true);
      appendNonNull(builder, roleGrant.isGrantOption());
      appendNonNull(builder, testMode ? -1 : roleGrant.getGrantTime() * 1000L);
      appendNonNull(builder, roleGrant.getGrantorName());
    }
    return builder.toString();
  }

  static String writeRolesGrantedInfo(List<HiveRoleGrant> roles, boolean testMode) {
    if (roles == null || roles.isEmpty()) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    //sort the list to get sorted (deterministic) output (for ease of testing)
    Collections.sort(roles);
    for (HiveRoleGrant role : roles) {
      appendNonNull(builder, role.getRoleName(), true);
      appendNonNull(builder, role.isGrantOption());
      appendNonNull(builder, testMode ? -1 : role.getGrantTime() * 1000L);
      appendNonNull(builder, role.getGrantor());
    }
    return builder.toString();
  }

  static StringBuilder appendNonNull(StringBuilder builder, Object value) {
    return appendNonNull(builder, value, false);
  }

  static StringBuilder appendNonNull(StringBuilder builder, Object value, boolean firstColumn) {
    if (!firstColumn) {
      builder.append((char)separator);
    } else if (builder.length() > 0) {
      builder.append((char)terminator);
    }
    if (value != null) {
      builder.append(value);
    }
    return builder;
  }

  private void setAlterProtectMode(boolean protectModeEnable,
      AlterTableDesc.ProtectModeType protectMode,
      ProtectMode mode) {
    if (protectMode == AlterTableDesc.ProtectModeType.OFFLINE) {
      mode.offline = protectModeEnable;
    } else if (protectMode == AlterTableDesc.ProtectModeType.NO_DROP) {
      mode.noDrop = protectModeEnable;
    } else if (protectMode == AlterTableDesc.ProtectModeType.NO_DROP_CASCADE) {
      mode.noDropCascade = protectModeEnable;
    }
  }
  /**
   * Alter a given table.
   *
   * @param db
   *          The database in question.
   * @param alterTbl
   *          This is the table we're altering.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int alterTable(Hive db, AlterTableDesc alterTbl) throws HiveException {
    // alter the table
    Table tbl = db.getTable(alterTbl.getOldName());

    List<Partition> allPartitions = null;
    if (alterTbl.getPartSpec() != null) {
      Map<String, String> partSpec = alterTbl.getPartSpec();
      if (DDLSemanticAnalyzer.isFullSpec(tbl, partSpec)) {
        allPartitions = new ArrayList<Partition>();
        Partition part = db.getPartition(tbl, partSpec, false);
        if (part == null) {
          // User provided a fully specified partition spec but it doesn't exist, fail.
          throw new HiveException(ErrorMsg.INVALID_PARTITION,
                StringUtils.join(alterTbl.getPartSpec().keySet(), ',') + " for table " + alterTbl.getOldName());

        }
        allPartitions.add(part);
      } else {
        // DDLSemanticAnalyzer has already checked if partial partition specs are allowed,
        // thus we should not need to check it here.
        allPartitions = db.getPartitions(tbl, alterTbl.getPartSpec());
      }
    }

    Table oldTbl = tbl.copy();
    if (allPartitions != null) {
      // Alter all partitions
      for (Partition part : allPartitions) {
        alterTableOrSinglePartition(alterTbl, tbl, part);
      }
    } else {
      // Just alter the table
      alterTableOrSinglePartition(alterTbl, tbl, null);
    }

    if (allPartitions == null) {
      updateModifiedParameters(tbl.getTTable().getParameters(), conf);
      tbl.checkValidity();
    } else {
      for (Partition tmpPart: allPartitions) {
        updateModifiedParameters(tmpPart.getParameters(), conf);
      }
    }

    try {
      if (allPartitions == null) {
        db.alterTable(alterTbl.getOldName(), tbl, alterTbl.getIsCascade());
      } else {
        db.alterPartitions(tbl.getTableName(), allPartitions);
      }
    } catch (InvalidOperationException e) {
      LOG.error("alter table: " + stringifyException(e));
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR);
    }

    // This is kind of hacky - the read entity contains the old table, whereas
    // the write entity
    // contains the new table. This is needed for rename - both the old and the
    // new table names are
    // passed
    // Don't acquire locks for any of these, we have already asked for them in DDLSemanticAnalyzer.
    if (allPartitions != null ) {
      for (Partition tmpPart: allPartitions) {
        work.getInputs().add(new ReadEntity(tmpPart));
        work.getOutputs().add(new WriteEntity(tmpPart, WriteEntity.WriteType.DDL_NO_LOCK));
      }
    } else {
      work.getInputs().add(new ReadEntity(oldTbl));
      work.getOutputs().add(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK));
    }
    return 0;
  }

  private int alterTableOrSinglePartition(AlterTableDesc alterTbl, Table tbl, Partition part)
      throws HiveException {

    if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.RENAME) {
      tbl.setDbName(Utilities.getDatabaseName(alterTbl.getNewName()));
      tbl.setTableName(Utilities.getTableName(alterTbl.getNewName()));
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDCOLS) {
      List<FieldSchema> oldCols = (part == null ? tbl.getCols() : part.getCols());
      StorageDescriptor sd = (part == null ? tbl.getTTable().getSd() : part.getTPartition().getSd());
      List<FieldSchema> newCols = alterTbl.getNewCols();
      String serializationLib = sd.getSerdeInfo().getSerializationLib();
      if (serializationLib.equals(
          "org.apache.hadoop.hive.serde.thrift.columnsetSerDe")) {
        console
        .printInfo("Replacing columns for columnsetSerDe and changing to LazySimpleSerDe");
        sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
        sd.setCols(newCols);
      } else {
        // make sure the columns does not already exist
        Iterator<FieldSchema> iterNewCols = newCols.iterator();
        while (iterNewCols.hasNext()) {
          FieldSchema newCol = iterNewCols.next();
          String newColName = newCol.getName();
          Iterator<FieldSchema> iterOldCols = oldCols.iterator();
          while (iterOldCols.hasNext()) {
            String oldColName = iterOldCols.next().getName();
            if (oldColName.equalsIgnoreCase(newColName)) {
              throw new HiveException(ErrorMsg.DUPLICATE_COLUMN_NAMES, newColName);
            }
          }
          oldCols.add(newCol);
        }
        sd.setCols(oldCols);
      }
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.RENAMECOLUMN) {
      List<FieldSchema> oldCols = (part == null ? tbl.getCols() : part.getCols());
      StorageDescriptor sd = (part == null ? tbl.getTTable().getSd() : part.getTPartition().getSd());
      List<FieldSchema> newCols = new ArrayList<FieldSchema>();
      Iterator<FieldSchema> iterOldCols = oldCols.iterator();
      String oldName = alterTbl.getOldColName();
      String newName = alterTbl.getNewColName();
      String type = alterTbl.getNewColType();
      String comment = alterTbl.getNewColComment();
      boolean first = alterTbl.getFirst();
      String afterCol = alterTbl.getAfterCol();
      FieldSchema column = null;

      boolean found = false;
      int position = -1;
      if (first) {
        position = 0;
      }

      int i = 1;
      while (iterOldCols.hasNext()) {
        FieldSchema col = iterOldCols.next();
        String oldColName = col.getName();
        if (oldColName.equalsIgnoreCase(newName)
            && !oldColName.equalsIgnoreCase(oldName)) {
          throw new HiveException(ErrorMsg.DUPLICATE_COLUMN_NAMES, newName);
        } else if (oldColName.equalsIgnoreCase(oldName)) {
          col.setName(newName);
          if (type != null && !type.trim().equals("")) {
            col.setType(type);
          }
          if (comment != null) {
            col.setComment(comment);
          }
          found = true;
          if (first || (afterCol != null && !afterCol.trim().equals(""))) {
            column = col;
            continue;
          }
        }

        if (afterCol != null && !afterCol.trim().equals("")
            && oldColName.equalsIgnoreCase(afterCol)) {
          position = i;
        }

        i++;
        newCols.add(col);
      }

      // did not find the column
      if (!found) {
        throw new HiveException(ErrorMsg.INVALID_COLUMN, oldName);
      }
      // after column is not null, but we did not find it.
      if ((afterCol != null && !afterCol.trim().equals("")) && position < 0) {
        throw new HiveException(ErrorMsg.INVALID_COLUMN, afterCol);
      }

      if (position >= 0) {
        newCols.add(position, column);
      }

      sd.setCols(newCols);
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.REPLACECOLS) {
      StorageDescriptor sd = (part == null ? tbl.getTTable().getSd() : part.getTPartition().getSd());
      // change SerDe to LazySimpleSerDe if it is columnsetSerDe
      String serializationLib = sd.getSerdeInfo().getSerializationLib();
      if (serializationLib.equals(
          "org.apache.hadoop.hive.serde.thrift.columnsetSerDe")) {
        console
        .printInfo("Replacing columns for columnsetSerDe and changing to LazySimpleSerDe");
        sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
      } else if (!serializationLib.equals(
          MetadataTypedColumnsetSerDe.class.getName())
          && !serializationLib.equals(LazySimpleSerDe.class.getName())
          && !serializationLib.equals(ColumnarSerDe.class.getName())
          && !serializationLib.equals(DynamicSerDe.class.getName())
          && !serializationLib.equals(ParquetHiveSerDe.class.getName())) {
        throw new HiveException(ErrorMsg.CANNOT_REPLACE_COLUMNS, alterTbl.getOldName());
      }
      sd.setCols(alterTbl.getNewCols());
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDPROPS) {
      tbl.getTTable().getParameters().putAll(alterTbl.getProps());
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.DROPPROPS) {
      Iterator<String> keyItr = alterTbl.getProps().keySet().iterator();
      while (keyItr.hasNext()) {
        tbl.getTTable().getParameters().remove(keyItr.next());
      }
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDSERDEPROPS) {
      StorageDescriptor sd = (part == null ? tbl.getTTable().getSd() : part.getTPartition().getSd());
      sd.getSerdeInfo().getParameters().putAll(alterTbl.getProps());
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDSERDE) {
      StorageDescriptor sd = (part == null ? tbl.getTTable().getSd() : part.getTPartition().getSd());
      String serdeName = alterTbl.getSerdeName();
      sd.getSerdeInfo().setSerializationLib(serdeName);
      if ((alterTbl.getProps() != null) && (alterTbl.getProps().size() > 0)) {
        sd.getSerdeInfo().getParameters().putAll(alterTbl.getProps());
      }
      if (part != null) {
        part.getTPartition().getSd().setCols(part.getTPartition().getSd().getCols());
      } else {
        if (!Table.hasMetastoreBasedSchema(conf, serdeName)) {
          tbl.setFields(Hive.getFieldsFromDeserializer(tbl.getTableName(), tbl.
              getDeserializer()));
        }
      }
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDFILEFORMAT) {
      StorageDescriptor sd = (part == null ? tbl.getTTable().getSd() : part.getTPartition().getSd());
      sd.setInputFormat(alterTbl.getInputFormat());
      sd.setOutputFormat(alterTbl.getOutputFormat());
      if (alterTbl.getSerdeName() != null) {
        sd.getSerdeInfo().setSerializationLib(alterTbl.getSerdeName());
      }
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ALTERPROTECTMODE) {
      boolean protectModeEnable = alterTbl.isProtectModeEnable();
      AlterTableDesc.ProtectModeType protectMode = alterTbl.getProtectModeType();

      ProtectMode mode = null;
      if (part != null) {
        mode = part.getProtectMode();
        setAlterProtectMode(protectModeEnable, protectMode, mode);
        part.setProtectMode(mode);
      } else {
        mode = tbl.getProtectMode();
        setAlterProtectMode(protectModeEnable,protectMode, mode);
        tbl.setProtectMode(mode);
      }
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDCLUSTERSORTCOLUMN) {
      StorageDescriptor sd = (part == null ? tbl.getTTable().getSd() : part.getTPartition().getSd());
      // validate sort columns and bucket columns
      List<String> columns = Utilities.getColumnNamesFromFieldSchema(tbl
          .getCols());
      if (!alterTbl.isTurnOffSorting()) {
        Utilities.validateColumnNames(columns, alterTbl.getBucketColumns());
      }
      if (alterTbl.getSortColumns() != null) {
        Utilities.validateColumnNames(columns, Utilities
            .getColumnNamesFromSortCols(alterTbl.getSortColumns()));
      }

      if (alterTbl.isTurnOffSorting()) {
        sd.setSortCols(new ArrayList<Order>());
      } else if (alterTbl.getNumberBuckets() == -1) {
        // -1 buckets means to turn off bucketing
        sd.setBucketCols(new ArrayList<String>());
        sd.setNumBuckets(-1);
        sd.setSortCols(new ArrayList<Order>());
      } else {
        sd.setBucketCols(alterTbl.getBucketColumns());
        sd.setNumBuckets(alterTbl.getNumberBuckets());
        sd.setSortCols(alterTbl.getSortColumns());
      }
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ALTERLOCATION) {
      StorageDescriptor sd = (part == null ? tbl.getTTable().getSd() : part.getTPartition().getSd());
      String newLocation = alterTbl.getNewLocation();
      try {
        URI locUri = new URI(newLocation);
        if (!locUri.isAbsolute() || locUri.getScheme() == null
            || locUri.getScheme().trim().equals("")) {
          throw new HiveException(ErrorMsg.BAD_LOCATION_VALUE, newLocation);
        }
        sd.setLocation(newLocation);
      } catch (URISyntaxException e) {
        throw new HiveException(e);
      }
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDSKEWEDBY) {
      // Validation's been done at compile time. no validation is needed here.
      List<String> skewedColNames = null;
      List<List<String>> skewedValues = null;

      if (alterTbl.isTurnOffSkewed()) {
        // Convert skewed table to non-skewed table.
        skewedColNames = new ArrayList<String>();
        skewedValues = new ArrayList<List<String>>();
      } else {
        skewedColNames = alterTbl.getSkewedColNames();
        skewedValues = alterTbl.getSkewedColValues();
      }

      if ( null == tbl.getSkewedInfo()) {
        // Convert non-skewed table to skewed table.
        SkewedInfo skewedInfo = new SkewedInfo();
        skewedInfo.setSkewedColNames(skewedColNames);
        skewedInfo.setSkewedColValues(skewedValues);
        tbl.setSkewedInfo(skewedInfo);
      } else {
        tbl.setSkewedColNames(skewedColNames);
        tbl.setSkewedColValues(skewedValues);
      }

      tbl.setStoredAsSubDirectories(alterTbl.isStoredAsSubDirectories());
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ALTERSKEWEDLOCATION) {
      // process location one-by-one
      Map<List<String>,String> locMaps = alterTbl.getSkewedLocations();
      Set<List<String>> keys = locMaps.keySet();
      for(List<String> key:keys){
        String newLocation = locMaps.get(key);
        try {
          URI locUri = new URI(newLocation);
          if (part != null) {
            List<String> slk = new ArrayList<String>(key);
            part.setSkewedValueLocationMap(slk, locUri.toString());
          } else {
            List<String> slk = new ArrayList<String>(key);
            tbl.setSkewedValueLocationMap(slk, locUri.toString());
          }
        } catch (URISyntaxException e) {
          throw new HiveException(e);
        }
      }
    } else if (alterTbl.getOp() == AlterTableTypes.ALTERBUCKETNUM) {
      if (part != null) {
        if (part.getBucketCount() == alterTbl.getNumberBuckets()) {
          return 0;
        }
        part.setBucketCount(alterTbl.getNumberBuckets());
      } else {
        if (tbl.getNumBuckets() == alterTbl.getNumberBuckets()) {
          return 0;
        }
        tbl.setNumBuckets(alterTbl.getNumberBuckets());
      }
    } else {
      throw new HiveException(ErrorMsg.UNSUPPORTED_ALTER_TBL_OP, alterTbl.getOp().toString());
    }

    return 0;
  }
  /**
   * Drop a given table or some partitions. DropTableDesc is currently used for both.
   *
   * @param db
   *          The database in question.
   * @param dropTbl
   *          This is the table we're dropping.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private void dropTableOrPartitions(Hive db, DropTableDesc dropTbl) throws HiveException {
    // We need to fetch the table before it is dropped so that it can be passed to
    // post-execution hook
    Table tbl = null;
    try {
      tbl = db.getTable(dropTbl.getTableName());
    } catch (InvalidTableException e) {
      // drop table is idempotent
    }

    if (dropTbl.getPartSpecs() == null) {
      dropTable(db, tbl, dropTbl);
    } else {
      dropPartitions(db, tbl, dropTbl);
    }
  }

  private void dropPartitions(Hive db, Table tbl, DropTableDesc dropTbl) throws HiveException {

    ReplicationSpec replicationSpec = dropTbl.getReplicationSpec();
    if (replicationSpec.isInReplicationScope()){
      /**
       * ALTER TABLE DROP PARTITION ... FOR REPLICATION(x) behaves as a DROP PARTITION IF OLDER THAN x
       *
       * So, we check each partition that matches our DropTableDesc.getPartSpecs(), and drop it only
       * if it's older than the event that spawned this replicated request to drop partition
       */
      // TODO: Current implementation of replication will result in DROP_PARTITION under replication
      // scope being called per-partition instead of multiple partitions. However, to be robust, we
      // must still handle the case of multiple partitions in case this assumption changes in the
      // future. However, if this assumption changes, we will not be very performant if we fetch
      // each partition one-by-one, and then decide on inspection whether or not this is a candidate
      // for dropping. Thus, we need a way to push this filter (replicationSpec.allowEventReplacementInto)
      // to the  metastore to allow it to do drop a partition or not, depending on a Predicate on the
      // parameter key values.
      for (DropTableDesc.PartSpec partSpec : dropTbl.getPartSpecs()){
        try {
          for (Partition p : Iterables.filter(
              db.getPartitionsByFilter(tbl, partSpec.getPartSpec().getExprString()),
              replicationSpec.allowEventReplacementInto())){
            db.dropPartition(tbl.getDbName(),tbl.getTableName(),p.getValues(),true);
          }
        } catch (NoSuchObjectException e){
          // ignore NSOE because that means there's nothing to drop.
        } catch (Exception e) {
          throw new HiveException(e.getMessage(), e);
        }
      }
      return;
    }

    // ifExists is currently verified in DDLSemanticAnalyzer
    List<Partition> droppedParts
        = db.dropPartitions(dropTbl.getTableName(),
                            dropTbl.getPartSpecs(),
                            PartitionDropOptions.instance()
                                                .deleteData(true)
                                                .ignoreProtection(dropTbl.getIgnoreProtection())
                                                .ifExists(true)
                                                .purgeData(dropTbl.getIfPurge()));
    for (Partition partition : droppedParts) {
      console.printInfo("Dropped the partition " + partition.getName());
      // We have already locked the table, don't lock the partitions.
      work.getOutputs().add(new WriteEntity(partition, WriteEntity.WriteType.DDL_NO_LOCK));
    };
  }

  private void dropTable(Hive db, Table tbl, DropTableDesc dropTbl) throws HiveException {
    // This is a true DROP TABLE
    if (tbl != null) {
      if (tbl.isView()) {
        if (!dropTbl.getExpectView()) {
          if (dropTbl.getIfExists()) {
            return;
          }
          throw new HiveException("Cannot drop a view with DROP TABLE");
        }
      } else {
        if (dropTbl.getExpectView()) {
          if (dropTbl.getIfExists()) {
            return;
          }
          throw new HiveException(
              "Cannot drop a base table with DROP VIEW");
        }
      }
    }

    if (tbl != null && !tbl.canDrop()) {
      throw new HiveException("Table " + tbl.getTableName() +
          " is protected from being dropped");
    }

    ReplicationSpec replicationSpec = dropTbl.getReplicationSpec();
    if ((tbl!= null) && replicationSpec.isInReplicationScope()){
      /**
       * DROP TABLE FOR REPLICATION behaves differently from DROP TABLE IF EXISTS - it more closely
       * matches a DROP TABLE IF OLDER THAN(x) semantic.
       *
       * Ideally, commands executed under the scope of replication need to be idempotent and resilient
       * to repeats. What can happen, sometimes, is that a drone processing a replication task can
       * have been abandoned for not returning in time, but still execute its task after a while,
       * which should not result in it mucking up data that has been impressed later on. So, for eg.,
       * if we create partition P1, followed by droppping it, followed by creating it yet again,
       * the replication of that drop should not drop the newer partition if it runs after the destination
       * object is already in the newer state.
       *
       * Thus, we check the replicationSpec.allowEventReplacementInto to determine whether or not we can
       * drop the object in question(will return false if object is newer than the event, true if not)
       *
       * In addition, since DROP TABLE FOR REPLICATION can result in a table not being dropped, while DROP
       * TABLE will always drop the table, and the included partitions, DROP TABLE FOR REPLICATION must
       * do one more thing - if it does not drop the table because the table is in a newer state, it must
       * drop the partitions inside it that are older than this event. To wit, DROP TABLE FOR REPL
       * acts like a recursive DROP TABLE IF OLDER.
       */
      if (!replicationSpec.allowEventReplacementInto(tbl)){
        // Drop occured as part of replicating a drop, but the destination
        // table was newer than the event being replicated. Ignore, but drop
        // any partitions inside that are older.
        if (tbl.isPartitioned()){

          PartitionIterable partitions = new PartitionIterable(db,tbl,null,conf.getIntVar(
              HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX));

          for (Partition p : Iterables.filter(partitions, replicationSpec.allowEventReplacementInto())){
            db.dropPartition(tbl.getDbName(),tbl.getTableName(),p.getValues(),true);
          }
        }
        return; // table is newer, leave it be.
      }
    }

    int partitionBatchSize = HiveConf.getIntVar(conf,
        ConfVars.METASTORE_BATCH_RETRIEVE_TABLE_PARTITION_MAX);

    // We should check that all the partitions of the table can be dropped
    if (tbl != null && tbl.isPartitioned()) {
      List<String> partitionNames = db.getPartitionNames(tbl.getDbName(), tbl.getTableName(), (short)-1);

      for(int i=0; i < partitionNames.size(); i+= partitionBatchSize) {
        List<String> partNames = partitionNames.subList(i, Math.min(i+partitionBatchSize,
            partitionNames.size()));
        List<Partition> listPartitions = db.getPartitionsByNames(tbl, partNames);
        for (Partition p: listPartitions) {
          if (!p.canDrop()) {
            throw new HiveException("Table " + tbl.getTableName() +
                " Partition" + p.getName() +
                " is protected from being dropped");
          }
        }
      }
    }

    // drop the table
    db.dropTable(dropTbl.getTableName(), dropTbl.getIfPurge());
    if (tbl != null) {
      // We have already locked the table in DDLSemanticAnalyzer, don't do it again here
      work.getOutputs().add(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK));
    }
  }

  /**
   * Update last_modified_by and last_modified_time parameters in parameter map.
   *
   * @param params
   *          Parameters.
   * @param conf
   *          HiveConf of session
   */
  private boolean updateModifiedParameters(Map<String, String> params, HiveConf conf) throws HiveException {
    String user = null;
    user = SessionState.getUserFromAuthenticator();
    params.put("last_modified_by", user);
    params.put("last_modified_time", Long.toString(System.currentTimeMillis() / 1000));
    return true;
  }

  /**
   * Check if the given serde is valid.
   */
  private void validateSerDe(String serdeName) throws HiveException {
    try {

      Deserializer d = ReflectionUtil.newInstance(conf.getClassByName(serdeName).
          asSubclass(Deserializer.class), conf);
      if (d != null) {
        LOG.debug("Found class for " + serdeName);
      }
    } catch (Exception e) {
      throw new HiveException("Cannot validate serde: " + serdeName, e);
    }
  }

  /**
   * Create a Database
   * @param db
   * @param crtDb
   * @return Always returns 0
   * @throws HiveException
   */
  private int createDatabase(Hive db, CreateDatabaseDesc crtDb)
      throws HiveException {
    Database database = new Database();
    database.setName(crtDb.getName());
    database.setDescription(crtDb.getComment());
    database.setLocationUri(crtDb.getLocationUri());
    database.setParameters(crtDb.getDatabaseProperties());
    database.setOwnerName(SessionState.getUserFromAuthenticator());
    database.setOwnerType(PrincipalType.USER);
    try {
      if (!Utilities.isDefaultNameNode(conf)) {
        makeLocationQualified(database);
      }
      db.createDatabase(database, crtDb.getIfNotExists());
    }
    catch (AlreadyExistsException ex) {
      //it would be better if AlreadyExistsException had an errorCode field....
      throw new HiveException(ex, ErrorMsg.DATABSAE_ALREADY_EXISTS, crtDb.getName());
    }
    return 0;
  }

  /**
   * Drop a Database
   * @param db
   * @param dropDb
   * @return Always returns 0
   * @throws HiveException
   */
  private int dropDatabase(Hive db, DropDatabaseDesc dropDb)
      throws HiveException {
    try {
      db.dropDatabase(dropDb.getDatabaseName(), true, dropDb.getIfExists(), dropDb.isCasdade());
    }
    catch (NoSuchObjectException ex) {
      throw new HiveException(ex, ErrorMsg.DATABASE_NOT_EXISTS, dropDb.getDatabaseName());
    }
    return 0;
  }

  /**
   * Switch to a different Database
   * @param db
   * @param switchDb
   * @return Always returns 0
   * @throws HiveException
   */
  private int switchDatabase(Hive db, SwitchDatabaseDesc switchDb)
      throws HiveException {
    String dbName = switchDb.getDatabaseName();
    if (!db.databaseExists(dbName)) {
      throw new HiveException(ErrorMsg.DATABASE_NOT_EXISTS, dbName);
    }
    SessionState.get().setCurrentDatabase(dbName);

    // set database specific parameters
    Database database = db.getDatabase(dbName);
    assert(database != null);
    Map<String, String> dbParams = database.getParameters();
    if (dbParams != null) {
      for (HiveConf.ConfVars var: HiveConf.dbVars) {
        String newValue = dbParams.get(var.varname);
        if (newValue != null) {
          LOG.info("Changing " + var.varname +
              " from " + conf.getVar(var) + " to " + newValue);
          conf.setVar(var, newValue);
        }
      }
    }

    return 0;
  }

  /**
   * Create a new table.
   *
   * @param db
   *          The database in question.
   * @param crtTbl
   *          This is the table we're creating.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int createTable(Hive db, CreateTableDesc crtTbl) throws HiveException {
    // create the table
    Table tbl;
    if (crtTbl.getDatabaseName() == null || (crtTbl.getTableName().contains("."))){
      tbl = db.newTable(crtTbl.getTableName());
    }else {
      tbl = new Table(crtTbl.getDatabaseName(),crtTbl.getTableName());
    }

    if (crtTbl.getTblProps() != null) {
      tbl.getTTable().getParameters().putAll(crtTbl.getTblProps());
    }

    if (crtTbl.getPartCols() != null) {
      tbl.setPartCols(crtTbl.getPartCols());
    }
    if (crtTbl.getNumBuckets() != -1) {
      tbl.setNumBuckets(crtTbl.getNumBuckets());
    }

    if (crtTbl.getStorageHandler() != null) {
      tbl.setProperty(
          org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE,
          crtTbl.getStorageHandler());
    }
    HiveStorageHandler storageHandler = tbl.getStorageHandler();

    /*
     * We use LazySimpleSerDe by default.
     *
     * If the user didn't specify a SerDe, and any of the columns are not simple
     * types, we will have to use DynamicSerDe instead.
     */
    if (crtTbl.getSerName() == null) {
      if (storageHandler == null) {
        LOG.info("Default to LazySimpleSerDe for table " + crtTbl.getTableName());
        tbl.setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
      } else {
        String serDeClassName = storageHandler.getSerDeClass().getName();
        LOG.info("Use StorageHandler-supplied " + serDeClassName
            + " for table " + crtTbl.getTableName());
        tbl.setSerializationLib(serDeClassName);
      }
    } else {
      // let's validate that the serde exists
      validateSerDe(crtTbl.getSerName());
      tbl.setSerializationLib(crtTbl.getSerName());
    }

    if (crtTbl.getFieldDelim() != null) {
      tbl.setSerdeParam(serdeConstants.FIELD_DELIM, crtTbl.getFieldDelim());
      tbl.setSerdeParam(serdeConstants.SERIALIZATION_FORMAT, crtTbl.getFieldDelim());
    }
    if (crtTbl.getFieldEscape() != null) {
      tbl.setSerdeParam(serdeConstants.ESCAPE_CHAR, crtTbl.getFieldEscape());
    }

    if (crtTbl.getCollItemDelim() != null) {
      tbl.setSerdeParam(serdeConstants.COLLECTION_DELIM, crtTbl.getCollItemDelim());
    }
    if (crtTbl.getMapKeyDelim() != null) {
      tbl.setSerdeParam(serdeConstants.MAPKEY_DELIM, crtTbl.getMapKeyDelim());
    }
    if (crtTbl.getLineDelim() != null) {
      tbl.setSerdeParam(serdeConstants.LINE_DELIM, crtTbl.getLineDelim());
    }
    if (crtTbl.getNullFormat() != null) {
      tbl.setSerdeParam(serdeConstants.SERIALIZATION_NULL_FORMAT, crtTbl.getNullFormat());
    }
    if (crtTbl.getSerdeProps() != null) {
      Iterator<Entry<String, String>> iter = crtTbl.getSerdeProps().entrySet()
          .iterator();
      while (iter.hasNext()) {
        Entry<String, String> m = iter.next();
        tbl.setSerdeParam(m.getKey(), m.getValue());
      }
    }

    if (crtTbl.getCols() != null) {
      tbl.setFields(crtTbl.getCols());
    }
    if (crtTbl.getBucketCols() != null) {
      tbl.setBucketCols(crtTbl.getBucketCols());
    }
    if (crtTbl.getSortCols() != null) {
      tbl.setSortCols(crtTbl.getSortCols());
    }
    if (crtTbl.getComment() != null) {
      tbl.setProperty("comment", crtTbl.getComment());
    }
    if (crtTbl.getLocation() != null) {
      tbl.setDataLocation(new Path(crtTbl.getLocation()));
    }

    if (crtTbl.getSkewedColNames() != null) {
      tbl.setSkewedColNames(crtTbl.getSkewedColNames());
    }
    if (crtTbl.getSkewedColValues() != null) {
      tbl.setSkewedColValues(crtTbl.getSkewedColValues());
    }

    tbl.getTTable().setTemporary(crtTbl.isTemporary());

    tbl.setStoredAsSubDirectories(crtTbl.isStoredAsSubDirectories());

    tbl.setInputFormatClass(crtTbl.getInputFormat());
    tbl.setOutputFormatClass(crtTbl.getOutputFormat());

    // only persist input/output format to metadata when it is explicitly specified.
    // Otherwise, load lazily via StorageHandler at query time.
    if (crtTbl.getInputFormat() != null && !crtTbl.getInputFormat().isEmpty()) {
      tbl.getTTable().getSd().setInputFormat(tbl.getInputFormatClass().getName());
    }
    if (crtTbl.getOutputFormat() != null && !crtTbl.getOutputFormat().isEmpty()) {
      tbl.getTTable().getSd().setOutputFormat(tbl.getOutputFormatClass().getName());
    }

    if (!Utilities.isDefaultNameNode(conf) && doesTableNeedLocation(tbl)) {
      // If location is specified - ensure that it is a full qualified name
      makeLocationQualified(tbl.getDbName(), tbl.getTTable().getSd(), tbl.getTableName());
    }

    if (crtTbl.isExternal()) {
      tbl.setProperty("EXTERNAL", "TRUE");
      tbl.setTableType(TableType.EXTERNAL_TABLE);
    }

    // If the sorted columns is a superset of bucketed columns, store this fact.
    // It can be later used to
    // optimize some group-by queries. Note that, the order does not matter as
    // long as it in the first
    // 'n' columns where 'n' is the length of the bucketed columns.
    if ((tbl.getBucketCols() != null) && (tbl.getSortCols() != null)) {
      List<String> bucketCols = tbl.getBucketCols();
      List<Order> sortCols = tbl.getSortCols();

      if ((sortCols.size() > 0) && (sortCols.size() >= bucketCols.size())) {
        boolean found = true;

        Iterator<String> iterBucketCols = bucketCols.iterator();
        while (iterBucketCols.hasNext()) {
          String bucketCol = iterBucketCols.next();
          boolean colFound = false;
          for (int i = 0; i < bucketCols.size(); i++) {
            if (bucketCol.equals(sortCols.get(i).getCol())) {
              colFound = true;
              break;
            }
          }
          if (colFound == false) {
            found = false;
            break;
          }
        }
        if (found) {
          tbl.setProperty("SORTBUCKETCOLSPREFIX", "TRUE");
        }
      }
    }

    // create the table
    if (crtTbl.getReplaceMode()){
      // replace-mode creates are really alters using CreateTableDesc.
      try {
        db.alterTable(tbl.getDbName()+"."+tbl.getTableName(),tbl);
      } catch (InvalidOperationException e) {
        throw new HiveException("Unable to alter table. " + e.getMessage(), e);
      }
    } else {
      db.createTable(tbl, crtTbl.getIfNotExists());
    }
    work.getOutputs().add(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK));
    return 0;
  }

  /**
   * Create a new table like an existing table.
   *
   * @param db
   *          The database in question.
   * @param crtTbl
   *          This is the table we're creating.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int createTableLike(Hive db, CreateTableLikeDesc crtTbl) throws Exception {
    // Get the existing table
    Table oldtbl = db.getTable(crtTbl.getLikeTableName());
    Table tbl;
    if (oldtbl.getTableType() == TableType.VIRTUAL_VIEW) {
      String targetTableName = crtTbl.getTableName();
      tbl=db.newTable(targetTableName);

      if (crtTbl.getTblProps() != null) {
        tbl.getTTable().getParameters().putAll(crtTbl.getTblProps());
      }

      tbl.setTableType(TableType.MANAGED_TABLE);

      if (crtTbl.isExternal()) {
        tbl.setProperty("EXTERNAL", "TRUE");
        tbl.setTableType(TableType.EXTERNAL_TABLE);
      }

      tbl.setFields(oldtbl.getCols());
      tbl.setPartCols(oldtbl.getPartCols());

      if (crtTbl.getDefaultSerName() == null) {
        LOG.info("Default to LazySimpleSerDe for table " + crtTbl.getTableName());
        tbl.setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
      } else {
        // let's validate that the serde exists
        validateSerDe(crtTbl.getDefaultSerName());
        tbl.setSerializationLib(crtTbl.getDefaultSerName());
      }

      if (crtTbl.getDefaultSerdeProps() != null) {
        Iterator<Entry<String, String>> iter = crtTbl.getDefaultSerdeProps().entrySet()
            .iterator();
        while (iter.hasNext()) {
          Entry<String, String> m = iter.next();
          tbl.setSerdeParam(m.getKey(), m.getValue());
        }
      }

      tbl.setInputFormatClass(crtTbl.getDefaultInputFormat());
      tbl.setOutputFormatClass(crtTbl.getDefaultOutputFormat());

      tbl.getTTable().getSd().setInputFormat(
          tbl.getInputFormatClass().getName());
      tbl.getTTable().getSd().setOutputFormat(
          tbl.getOutputFormatClass().getName());
    } else {
      tbl=oldtbl;

      // find out database name and table name of target table
      String targetTableName = crtTbl.getTableName();
      String[] names = Utilities.getDbTableName(targetTableName);

      tbl.setDbName(names[0]);
      tbl.setTableName(names[1]);

      // using old table object, hence reset the owner to current user for new table.
      tbl.setOwner(SessionState.getUserFromAuthenticator());

      if (crtTbl.getLocation() != null) {
        tbl.setDataLocation(new Path(crtTbl.getLocation()));
      } else {
        tbl.unsetDataLocation();
      }

      Class<? extends Deserializer> serdeClass = oldtbl.getDeserializerClass();

      Map<String, String> params = tbl.getParameters();
      // We should copy only those table parameters that are specified in the config.
      SerDeSpec spec = AnnotationUtils.getAnnotation(serdeClass, SerDeSpec.class);
      String paramsStr = HiveConf.getVar(conf, HiveConf.ConfVars.DDL_CTL_PARAMETERS_WHITELIST);

      Set<String> retainer = new HashSet<String>();
      if (spec != null && spec.schemaProps() != null) {
        retainer.addAll(Arrays.asList(spec.schemaProps()));
      }
      if (paramsStr != null) {
        retainer.addAll(Arrays.asList(paramsStr.split(",")));
      }
      if (!retainer.isEmpty()) {
        params.keySet().retainAll(retainer);
      } else {
        params.clear();
      }

      if (crtTbl.getTblProps() != null) {
        params.putAll(crtTbl.getTblProps());
      }

      if (crtTbl.isUserStorageFormat()) {
        tbl.setInputFormatClass(crtTbl.getDefaultInputFormat());
        tbl.setOutputFormatClass(crtTbl.getDefaultOutputFormat());
        tbl.getTTable().getSd().setInputFormat(
        tbl.getInputFormatClass().getName());
        tbl.getTTable().getSd().setOutputFormat(
        tbl.getOutputFormatClass().getName());
        if (crtTbl.getDefaultSerName() == null) {
          LOG.info("Default to LazySimpleSerDe for like table " + crtTbl.getTableName());
          tbl.setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
        } else {
          // let's validate that the serde exists
          validateSerDe(crtTbl.getDefaultSerName());
          tbl.setSerializationLib(crtTbl.getDefaultSerName());
        }
      }

      tbl.getTTable().setTemporary(crtTbl.isTemporary());

      if (crtTbl.isExternal()) {
        tbl.setProperty("EXTERNAL", "TRUE");
        tbl.setTableType(TableType.EXTERNAL_TABLE);
      } else {
        tbl.getParameters().remove("EXTERNAL");
      }
    }

    if (!Utilities.isDefaultNameNode(conf)) {
      // If location is specified - ensure that it is a full qualified name
      makeLocationQualified(tbl.getDbName(), tbl.getTTable().getSd(), tbl.getTableName());
    }

    // create the table
    db.createTable(tbl, crtTbl.getIfNotExists());
    work.getOutputs().add(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK));
    return 0;
  }

  /**
   * Create a new view.
   *
   * @param db
   *          The database in question.
   * @param crtView
   *          This is the view we're creating.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int createView(Hive db, CreateViewDesc crtView) throws HiveException {
    Table oldview = db.getTable(crtView.getViewName(), false);
    if (crtView.getOrReplace() && oldview != null) {
      // replace existing view
      // remove the existing partition columns from the field schema
      oldview.setViewOriginalText(crtView.getViewOriginalText());
      oldview.setViewExpandedText(crtView.getViewExpandedText());
      oldview.setFields(crtView.getSchema());
      if (crtView.getComment() != null) {
        oldview.setProperty("comment", crtView.getComment());
      }
      if (crtView.getTblProps() != null) {
        oldview.getTTable().getParameters().putAll(crtView.getTblProps());
      }
      oldview.setPartCols(crtView.getPartCols());
      oldview.checkValidity();
      try {
        db.alterTable(crtView.getViewName(), oldview);
      } catch (InvalidOperationException e) {
        throw new HiveException(e);
      }
      work.getOutputs().add(new WriteEntity(oldview, WriteEntity.WriteType.DDL_NO_LOCK));
    } else {
      // create new view
      Table tbl = db.newTable(crtView.getViewName());
      tbl.setTableType(TableType.VIRTUAL_VIEW);
      tbl.setSerializationLib(null);
      tbl.clearSerDeInfo();
      tbl.setViewOriginalText(crtView.getViewOriginalText());
      tbl.setViewExpandedText(crtView.getViewExpandedText());
      tbl.setFields(crtView.getSchema());
      if (crtView.getComment() != null) {
        tbl.setProperty("comment", crtView.getComment());
      }
      if (crtView.getTblProps() != null) {
        tbl.getTTable().getParameters().putAll(crtView.getTblProps());
      }

      if (crtView.getPartCols() != null) {
        tbl.setPartCols(crtView.getPartCols());
      }

      db.createTable(tbl, crtView.getIfNotExists());
      work.getOutputs().add(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK));
    }
    return 0;
  }

  private int truncateTable(Hive db, TruncateTableDesc truncateTableDesc) throws HiveException {

    if (truncateTableDesc.getColumnIndexes() != null) {
      ColumnTruncateWork truncateWork = new ColumnTruncateWork(
          truncateTableDesc.getColumnIndexes(), truncateTableDesc.getInputDir(),
          truncateTableDesc.getOutputDir());
      truncateWork.setListBucketingCtx(truncateTableDesc.getLbCtx());
      truncateWork.setMapperCannotSpanPartns(true);
      DriverContext driverCxt = new DriverContext();
      ColumnTruncateTask taskExec = new ColumnTruncateTask();
      taskExec.initialize(db.getConf(), null, driverCxt);
      taskExec.setWork(truncateWork);
      taskExec.setQueryPlan(this.getQueryPlan());
      return taskExec.execute(driverCxt);
    }

    String tableName = truncateTableDesc.getTableName();
    Map<String, String> partSpec = truncateTableDesc.getPartSpec();

    Table table = db.getTable(tableName, true);

    try {
      // this is not transactional
      HadoopShims shim = ShimLoader.getHadoopShims();
      for (Path location : getLocations(db, table, partSpec)) {
        FileSystem fs = location.getFileSystem(conf);
        
        HdfsFileStatus fullFileStatus = shim.getFullFileStatus(conf, fs, location);
        fs.delete(location, true);
        fs.mkdirs(location);
        try {
          shim.setFullFileStatus(conf, fullFileStatus, fs, location);
        } catch (Exception e) {
          LOG.warn("Error setting permissions of " + location, e);
        }
      }
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR);
    }
    return 0;
  }

  private int exchangeTablePartition(Hive db,
      AlterTableExchangePartition exchangePartition) throws HiveException {
    Map<String, String> partitionSpecs = exchangePartition.getPartitionSpecs();
    Table destTable = exchangePartition.getDestinationTable();
    Table sourceTable = exchangePartition.getSourceTable();
    db.exchangeTablePartitions(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(),destTable.getDbName(),
        destTable.getTableName());
    return 0;
  }

  private List<Path> getLocations(Hive db, Table table, Map<String, String> partSpec)
      throws HiveException, InvalidOperationException {
    List<Path> locations = new ArrayList<Path>();
    if (partSpec == null) {
      if (table.isPartitioned()) {
        for (Partition partition : db.getPartitions(table)) {
          locations.add(partition.getDataLocation());
          if (needToUpdateStats(partition.getParameters())) {
            db.alterPartition(table.getDbName(), table.getTableName(), partition);
          }
        }
      } else {
        locations.add(table.getPath());
        if (needToUpdateStats(table.getParameters())) {
          db.alterTable(table.getDbName()+"."+table.getTableName(), table);
        }
      }
    } else {
      for (Partition partition : db.getPartitionsByNames(table, partSpec)) {
        locations.add(partition.getDataLocation());
        if (needToUpdateStats(partition.getParameters())) {
          db.alterPartition(table.getDbName(), table.getTableName(), partition);
        }
      }
    }
    return locations;
  }

  private boolean needToUpdateStats(Map<String,String> props) {
    if (null == props) {
      return false;
    }
    boolean statsPresent = false;
    for (String stat : StatsSetupConst.supportedStats) {
      String statVal = props.get(stat);
      if (statVal != null && Long.parseLong(statVal) > 0) {
        statsPresent = true;
        props.put(stat, "0");
        props.put(StatsSetupConst.COLUMN_STATS_ACCURATE, "false");
      }
    }
    return statsPresent;
  }

  private String escapeHiveCommand(String str) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < str.length(); i ++) {
      char c = str.charAt(i);
      if (c == '\'' || c == ';') {
        sb.append('\\');
      }
      sb.append(c);
    }
    return sb.toString();
  }

  @Override
  public StageType getType() {
    return StageType.DDL;
  }

  @Override
  public String getName() {
    return "DDL";
  }

   /**
   * Make location in specified sd qualified.
   *
   * @param databaseName
   *          Database name.
   * @param sd
   *          Storage descriptor.
   * @param name
   *          Object name.
   */
  private void makeLocationQualified(String databaseName, StorageDescriptor sd, String name)
      throws HiveException {
    Path path = null;
    if (!sd.isSetLocation())
    {
      // Location is not set, leave it as-is if this is not a default DB
      if (databaseName.equalsIgnoreCase(MetaStoreUtils.DEFAULT_DATABASE_NAME))
      {
        // Default database name path is always ignored, use METASTOREWAREHOUSE and object name
        // instead
        path = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.METASTOREWAREHOUSE), name.toLowerCase());
      }
    }
    else
    {
      path = new Path(sd.getLocation());
    }

    if (path != null)
    {
      sd.setLocation(Utilities.getQualifiedPath(conf, path));
    }
  }

   /**
   * Make qualified location for an index .
   *
   * @param crtIndex
   *          Create index descriptor.
   * @param name
   *          Object name.
   */
  private void makeLocationQualified(CreateIndexDesc crtIndex, String name) throws HiveException
  {
    Path path = null;
    if (crtIndex.getLocation() == null) {
      // Location is not set, leave it as-is if index doesn't belong to default DB
      // Currently all indexes are created in current DB only
      if (Utilities.getDatabaseName(name).equalsIgnoreCase(MetaStoreUtils.DEFAULT_DATABASE_NAME)) {
        // Default database name path is always ignored, use METASTOREWAREHOUSE and object name
        // instead
        String warehouse = HiveConf.getVar(conf, ConfVars.METASTOREWAREHOUSE);
        String tableName = Utilities.getTableName(name);
        path = new Path(warehouse, tableName.toLowerCase());
      }
    }
    else {
      path = new Path(crtIndex.getLocation());
    }

    if (path != null) {
      crtIndex.setLocation(Utilities.getQualifiedPath(conf, path));
    }
  }

   /**
   * Make qualified location for a database .
   *
   * @param database
   *          Database.
   */
  private void makeLocationQualified(Database database) throws HiveException {
    if (database.isSetLocationUri()) {
      database.setLocationUri(Utilities.getQualifiedPath(conf, new Path(database.getLocationUri())));
    }
    else {
      // Location is not set we utilize METASTOREWAREHOUSE together with database name
      database.setLocationUri(
          Utilities.getQualifiedPath(conf, new Path(HiveConf.getVar(conf, HiveConf.ConfVars.METASTOREWAREHOUSE),
              database.getName().toLowerCase() + ".db")));
    }
  }

  private static boolean doesTableNeedLocation(Table tbl) {
    // If we are ok with breaking compatibility of existing 3rd party StorageHandlers,
    // this method could be moved to the HiveStorageHandler interface.
    boolean retval = true;
    if (tbl.getStorageHandler() != null) {
      retval = !tbl.getStorageHandler().toString().equals(
          "org.apache.hadoop.hive.hbase.HBaseStorageHandler");
    }
    return retval;
  }
}
