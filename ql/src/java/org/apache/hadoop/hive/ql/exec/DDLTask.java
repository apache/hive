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
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.ProtectMode;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.rcfile.merge.BlockMergeTask;
import org.apache.hadoop.hive.ql.io.rcfile.merge.MergeWork;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockMode;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject.HiveLockObjectData;
import org.apache.hadoop.hive.ql.metadata.CheckResult;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveMetaStoreChecker;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.MetaDataFormatUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.AlterTablePartMergeFilesDesc;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.AlterDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.AlterIndexDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
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
import org.apache.hadoop.hive.ql.plan.GrantDesc;
import org.apache.hadoop.hive.ql.plan.GrantRevokeRoleDDL;
import org.apache.hadoop.hive.ql.plan.LockTableDesc;
import org.apache.hadoop.hive.ql.plan.MsckDesc;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.plan.RevokeDesc;
import org.apache.hadoop.hive.ql.plan.RoleDDLDesc;
import org.apache.hadoop.hive.ql.plan.ShowDatabasesDesc;
import org.apache.hadoop.hive.ql.plan.ShowFunctionsDesc;
import org.apache.hadoop.hive.ql.plan.ShowGrantDesc;
import org.apache.hadoop.hive.ql.plan.ShowIndexesDesc;
import org.apache.hadoop.hive.ql.plan.ShowLocksDesc;
import org.apache.hadoop.hive.ql.plan.ShowPartitionsDesc;
import org.apache.hadoop.hive.ql.plan.ShowTableStatusDesc;
import org.apache.hadoop.hive.ql.plan.ShowTablesDesc;
import org.apache.hadoop.hive.ql.plan.SwitchDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.UnlockTableDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;

/**
 * DDLTask implementation.
 *
 **/
public class DDLTask extends Task<DDLWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog("hive.ql.exec.DDLTask");

  transient HiveConf conf;
  private static final int separator = Utilities.tabCode;
  private static final int terminator = Utilities.newLineCode;

  // These are suffixes attached to intermediate directory names used in the
  // archiving / un-archiving process.
  private static String INTERMEDIATE_ARCHIVED_DIR_SUFFIX;
  private static String INTERMEDIATE_ORIGINAL_DIR_SUFFIX;
  private static String INTERMEDIATE_EXTRACTED_DIR_SUFFIX;

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
    this.conf = conf;

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
        return dropTable(db, dropTbl);
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
        return addPartition(db, addPartitionDesc);
      }

      AlterTableSimpleDesc simpleDesc = work.getAlterTblSimpleDesc();
      if (simpleDesc != null) {
        if (simpleDesc.getType() == AlterTableTypes.TOUCH) {
          return touch(db, simpleDesc);
        } else if (simpleDesc.getType() == AlterTableTypes.ARCHIVE) {
          return archive(db, simpleDesc, driverContext);
        } else if (simpleDesc.getType() == AlterTableTypes.UNARCHIVE) {
          return unarchive(db, simpleDesc);
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

      ShowTableStatusDesc showTblStatus = work.getShowTblStatusDesc();
      if (showTblStatus != null) {
        return showTableStatus(db, showTblStatus);
      }

      ShowFunctionsDesc showFuncs = work.getShowFuncsDesc();
      if (showFuncs != null) {
        return showFunctions(showFuncs);
      }

      ShowLocksDesc showLocks = work.getShowLocksDesc();
      if (showLocks != null) {
        return showLocks(showLocks);
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

      RoleDDLDesc roleDDLDesc = work.getRoleDDLDesc();
      if (roleDDLDesc != null) {
        return roleDDL(roleDDLDesc);
      }

      GrantDesc grantDesc = work.getGrantDesc();
      if (grantDesc != null) {
        return grantOrRevokePrivileges(grantDesc.getPrincipals(), grantDesc
            .getPrivileges(), grantDesc.getPrivilegeSubjectDesc(), grantDesc.getGrantor(), grantDesc.getGrantorType(), grantDesc.isGrantOption(), true);
      }

      RevokeDesc revokeDesc = work.getRevokeDesc();
      if (revokeDesc != null) {
        return grantOrRevokePrivileges(revokeDesc.getPrincipals(), revokeDesc
            .getPrivileges(), revokeDesc.getPrivilegeSubjectDesc(), null, null, false, false);
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
      if(mergeFilesDesc != null) {
        return mergeFiles(db, mergeFilesDesc);
      }

    } catch (InvalidTableException e) {
      console.printError("Table " + e.getTableName() + " does not exist");
      LOG.debug(stringifyException(e));
      return 1;
    } catch (HiveException e) {
      console.printError("FAILED: Error in metadata: " + e.getMessage(), "\n"
          + stringifyException(e));
      LOG.debug(stringifyException(e));
      return 1;
    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + stringifyException(e));
      return (1);
    }
    assert false;
    return 0;
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
    // merge work only needs input and output.
    MergeWork mergeWork = new MergeWork(mergeFilesDesc.getInputDir(),
        mergeFilesDesc.getOutputDir());
    DriverContext driverCxt = new DriverContext();
    BlockMergeTask taskExec = new BlockMergeTask();
    taskExec.initialize(db.getConf(), null, driverCxt);
    taskExec.setWork(mergeWork);
    taskExec.setQueryPlan(this.getQueryPlan());
    int ret = taskExec.execute(driverCxt);

    return ret;
  }

  private int grantOrRevokeRole(GrantRevokeRoleDDL grantOrRevokeRoleDDL)
      throws HiveException {
    try {
      boolean grantRole = grantOrRevokeRoleDDL.getGrant();
      List<PrincipalDesc> principals = grantOrRevokeRoleDDL.getPrincipalDesc();
      List<String> roles = grantOrRevokeRoleDDL.getRoles();
      for (PrincipalDesc principal : principals) {
        String userName = principal.getName();
        for (String roleName : roles) {
          if (grantRole) {
            db.grantRole(roleName, userName, principal.getType(),
                grantOrRevokeRoleDDL.getGrantor(), grantOrRevokeRoleDDL
                    .getGrantorType(), grantOrRevokeRoleDDL.isGrantOption());
          } else {
            db.revokeRole(roleName, userName, principal.getType());
          }
        }
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
    return 0;
  }

  private int showGrants(ShowGrantDesc showGrantDesc) throws HiveException {
    DataOutput outStream = null;
    try {
      Path resFile = new Path(showGrantDesc.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);
      PrincipalDesc principalDesc = showGrantDesc.getPrincipalDesc();
      PrivilegeObjectDesc hiveObjectDesc = showGrantDesc.getHiveObj();
      String principalName = principalDesc.getName();
      if (hiveObjectDesc == null) {
        List<HiveObjectPrivilege> users = db.showPrivilegeGrant(
            HiveObjectType.GLOBAL, principalName, principalDesc.getType(),
            null, null, null, null);
        if (users != null && users.size() > 0) {
          boolean first = true;
          for (HiveObjectPrivilege usr : users) {
            if (!first) {
              outStream.write(terminator);
            } else {
              first = false;
            }

            writeGrantInfo(outStream, principalDesc.getType(), principalName,
                null, null, null, null, usr.getGrantInfo());

          }
        }
      } else {
        String obj = hiveObjectDesc.getObject();
        boolean notFound = true;
        String dbName = null;
        String tableName = null;
        Table tableObj = null;
        Database dbObj = null;

        if (hiveObjectDesc.getTable()) {
          String[] dbTab = obj.split("\\.");
          if (dbTab.length == 2) {
            dbName = dbTab[0];
            tableName = dbTab[1];
          } else {
            dbName = db.getCurrentDatabase();
            tableName = obj;
          }
          dbObj = db.getDatabase(dbName);
          tableObj = db.getTable(dbName, tableName);
          notFound = (dbObj == null || tableObj == null);
        } else {
          dbName = hiveObjectDesc.getObject();
          dbObj = db.getDatabase(dbName);
          notFound = (dbObj == null);
        }
        if (notFound) {
          throw new HiveException(obj + " can not be found");
        }

        String partName = null;
        List<String> partValues = null;
        if (hiveObjectDesc.getPartSpec() != null) {
          partName = Warehouse
              .makePartName(hiveObjectDesc.getPartSpec(), false);
          partValues = Warehouse.getPartValuesFromPartName(partName);
        }

        if (!hiveObjectDesc.getTable()) {
          // show database level privileges
          List<HiveObjectPrivilege> dbs = db.showPrivilegeGrant(HiveObjectType.DATABASE, principalName,
              principalDesc.getType(), dbName, null, null, null);
          if (dbs != null && dbs.size() > 0) {
            boolean first = true;
            for (HiveObjectPrivilege db : dbs) {
              if (!first) {
                outStream.write(terminator);
              } else {
                first = false;
              }

              writeGrantInfo(outStream, principalDesc.getType(), principalName,
                  dbName, null, null, null, db.getGrantInfo());

            }
          }

        } else {
          if (showGrantDesc.getColumns() != null) {
            // show column level privileges
            for (String columnName : showGrantDesc.getColumns()) {
              List<HiveObjectPrivilege> columnss = db.showPrivilegeGrant(
                  HiveObjectType.COLUMN, principalName,
                  principalDesc.getType(), dbName, tableName, partValues,
                  columnName);
              if (columnss != null && columnss.size() > 0) {
                boolean first = true;
                for (HiveObjectPrivilege col : columnss) {
                  if (!first) {
                    outStream.write(terminator);
                  } else {
                    first = false;
                  }

                  writeGrantInfo(outStream, principalDesc.getType(),
                      principalName, dbName, tableName, partName, columnName,
                      col.getGrantInfo());
                }
              }
            }
          } else if (hiveObjectDesc.getPartSpec() != null) {
            // show partition level privileges
            List<HiveObjectPrivilege> parts = db.showPrivilegeGrant(
                HiveObjectType.PARTITION, principalName, principalDesc
                    .getType(), dbName, tableName, partValues, null);
            if (parts != null && parts.size() > 0) {
              boolean first = true;
              for (HiveObjectPrivilege part : parts) {
                if (!first) {
                  outStream.write(terminator);
                } else {
                  first = false;
                }

                writeGrantInfo(outStream, principalDesc.getType(),
                    principalName, dbName, tableName, partName, null, part.getGrantInfo());

              }
            }
          } else {
            // show table level privileges
            List<HiveObjectPrivilege> tbls = db.showPrivilegeGrant(
                HiveObjectType.TABLE, principalName, principalDesc.getType(),
                dbName, tableName, null, null);
            if (tbls != null && tbls.size() > 0) {
              boolean first = true;
              for (HiveObjectPrivilege tbl : tbls) {
                if (!first) {
                  outStream.write(terminator);
                } else {
                  first = false;
                }

                writeGrantInfo(outStream, principalDesc.getType(),
                    principalName, dbName, tableName, null, null, tbl.getGrantInfo());

              }
            }
          }
        }
      }
      ((FSDataOutputStream) outStream).close();
      outStream = null;
    } catch (FileNotFoundException e) {
      LOG.info("show table status: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.info("show table status: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    } finally {
      IOUtils.closeStream((FSDataOutputStream) outStream);
    }
    return 0;
  }

  private int grantOrRevokePrivileges(List<PrincipalDesc> principals,
      List<PrivilegeDesc> privileges, PrivilegeObjectDesc privSubjectDesc,
      String grantor, PrincipalType grantorType, boolean grantOption, boolean isGrant) {
    if (privileges == null || privileges.size() == 0) {
      console.printError("No privilege found.");
      return 1;
    }

    String dbName = null;
    String tableName = null;
    Table tableObj = null;
    Database dbObj = null;

    try {

      if (privSubjectDesc != null) {
        if (privSubjectDesc.getPartSpec() != null && isGrant) {
          throw new HiveException("Grant does not support partition level.");
        }
        String obj = privSubjectDesc.getObject();
        boolean notFound = true;
        if (privSubjectDesc.getTable()) {
          String[] dbTab = obj.split("\\.");
          if (dbTab.length == 2) {
            dbName = dbTab[0];
            tableName = dbTab[1];
          } else {
            dbName = db.getCurrentDatabase();
            tableName = obj;
          }
          dbObj = db.getDatabase(dbName);
          tableObj = db.getTable(dbName, tableName);
          notFound = (dbObj == null || tableObj == null);
        } else {
          dbName = privSubjectDesc.getObject();
          dbObj = db.getDatabase(dbName);
          notFound = (dbObj == null);
        }
        if (notFound) {
          throw new HiveException(obj + " can not be found");
        }
      }

      PrivilegeBag privBag = new PrivilegeBag();
      if (privSubjectDesc == null) {
        for (int idx = 0; idx < privileges.size(); idx++) {
          Privilege priv = privileges.get(idx).getPrivilege();
          if (privileges.get(idx).getColumns() != null
              && privileges.get(idx).getColumns().size() > 0) {
            throw new HiveException(
                "For user-level privileges, column sets should be null. columns="
                    + privileges.get(idx).getColumns().toString());
          }

          privBag.addToPrivileges(new HiveObjectPrivilege(new HiveObjectRef(
              HiveObjectType.GLOBAL, null, null, null, null), null, null,
              new PrivilegeGrantInfo(priv.toString(), 0, grantor, grantorType,
                  grantOption)));
        }
      } else {
        org.apache.hadoop.hive.metastore.api.Partition partObj = null;
        List<String> partValues = null;
        if (tableObj != null) {
          if ((!tableObj.isPartitioned())
              && privSubjectDesc.getPartSpec() != null) {
            throw new HiveException(
                "Table is not partitioned, but partition name is present: partSpec="
                    + privSubjectDesc.getPartSpec().toString());
          }

          if (privSubjectDesc.getPartSpec() != null) {
            partObj = db.getPartition(tableObj, privSubjectDesc.getPartSpec(),
                false).getTPartition();
            partValues = partObj.getValues();
          }
        }

        for (PrivilegeDesc privDesc : privileges) {
          List<String> columns = privDesc.getColumns();
          Privilege priv = privDesc.getPrivilege();
          if (columns != null && columns.size() > 0) {
            if (!priv.supportColumnLevel()) {
              throw new HiveException(priv.toString()
                  + " does not support column level.");
            }
            if (privSubjectDesc == null || tableName == null) {
              throw new HiveException(
                  "For user-level/database-level privileges, column sets should be null. columns="
                      + columns);
            }
            for (int i = 0; i < columns.size(); i++) {
              privBag.addToPrivileges(new HiveObjectPrivilege(
                  new HiveObjectRef(HiveObjectType.COLUMN, dbName, tableName,
                      partValues, columns.get(i)), null, null,  new PrivilegeGrantInfo(priv.toString(), 0, grantor, grantorType, grantOption)));
            }
          } else {
            if (privSubjectDesc.getTable()) {
              if (privSubjectDesc.getPartSpec() != null) {
                privBag.addToPrivileges(new HiveObjectPrivilege(
                    new HiveObjectRef(HiveObjectType.PARTITION, dbName,
                        tableName, partValues, null), null, null,  new PrivilegeGrantInfo(priv.toString(), 0, grantor, grantorType, grantOption)));
              } else {
                privBag
                    .addToPrivileges(new HiveObjectPrivilege(
                        new HiveObjectRef(HiveObjectType.TABLE, dbName,
                            tableName, null, null), null, null, new PrivilegeGrantInfo(priv.toString(), 0, grantor, grantorType, grantOption)));
              }
            } else {
              privBag.addToPrivileges(new HiveObjectPrivilege(
                  new HiveObjectRef(HiveObjectType.DATABASE, dbName, null,
                      null, null), null, null, new PrivilegeGrantInfo(priv.toString(), 0, grantor, grantorType, grantOption)));
            }
          }
        }
      }

      for (PrincipalDesc principal : principals) {
        for (int i = 0; i < privBag.getPrivileges().size(); i++) {
          HiveObjectPrivilege objPrivs = privBag.getPrivileges().get(i);
          objPrivs.setPrincipalName(principal.getName());
          objPrivs.setPrincipalType(principal.getType());
        }
        if (isGrant) {
          db.grantPrivileges(privBag);
        } else {
          db.revokePrivileges(privBag);
        }
      }
    } catch (Exception e) {
      console.printError("Error: " + e.getMessage());
      return 1;
    }

    return 0;
  }

  private int roleDDL(RoleDDLDesc roleDDLDesc) {
    RoleDDLDesc.RoleOperation operation = roleDDLDesc.getOperation();
    DataOutput outStream = null;
    try {
      if (operation.equals(RoleDDLDesc.RoleOperation.CREATE_ROLE)) {
        db.createRole(roleDDLDesc.getName(), roleDDLDesc.getRoleOwnerName());
      } else if (operation.equals(RoleDDLDesc.RoleOperation.DROP_ROLE)) {
        db.dropRole(roleDDLDesc.getName());
      } else if (operation.equals(RoleDDLDesc.RoleOperation.SHOW_ROLE_GRANT)) {
        List<Role> roles = db.showRoleGrant(roleDDLDesc.getName(), roleDDLDesc
            .getPrincipalType());
        if (roles != null && roles.size() > 0) {
          Path resFile = new Path(roleDDLDesc.getResFile());
          FileSystem fs = resFile.getFileSystem(conf);
          outStream = fs.create(resFile);
          for (Role role : roles) {
            outStream.writeBytes("role name:" + role.getRoleName());
            outStream.write(terminator);
          }
          ((FSDataOutputStream) outStream).close();
          outStream = null;
        }
      } else {
        throw new HiveException("Unkown role operation "
            + operation.getOperationName());
      }
    } catch (HiveException e) {
      console.printError("Error in role operation "
          + operation.getOperationName() + " on role name "
          + roleDDLDesc.getName() + ", error message " + e.getMessage());
      return 1;
    } catch (IOException e) {
      LOG.info("role ddl exception: " + stringifyException(e));
      return 1;
    } finally {
      IOUtils.closeStream((FSDataOutputStream) outStream);
    }

    return 0;
  }

  private int alterDatabase(AlterDatabaseDesc alterDbDesc) throws HiveException {

    String dbName = alterDbDesc.getDatabaseName();
    Database database = db.getDatabase(dbName);
    Map<String, String> newParams = alterDbDesc.getDatabaseProperties();

    if (database != null) {
      Map<String, String> params = database.getParameters();
      // if both old and new params are not null, merge them
      if (params != null && newParams != null) {
        params.putAll(newParams);
        database.setParameters(params);
      } else { // if one of them is null, replace the old params with the new one
        database.setParameters(newParams);
      }
      db.alterDatabase(database.getName(), database);
    } else {
      throw new HiveException("ERROR: The database " + dbName + " does not exist.");
    }
    return 0;
  }

  private int dropIndex(Hive db, DropIndexDesc dropIdx) throws HiveException {
    db.dropIndex(db.getCurrentDatabase(), dropIdx.getTableName(),
        dropIdx.getIndexName(), true);
    return 0;
  }

  private int createIndex(Hive db, CreateIndexDesc crtIndex) throws HiveException {

    if( crtIndex.getSerde() != null) {
      validateSerDe(crtIndex.getSerde());
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
    return 0;
  }

  private int alterIndex(Hive db, AlterIndexDesc alterIndex) throws HiveException {
    String dbName = alterIndex.getDbName();
    String baseTableName = alterIndex.getBaseTableName();
    String indexName = alterIndex.getIndexName();
    Index idx = db.getIndex(dbName, baseTableName, indexName);

    switch(alterIndex.getOp()) {
      case ADDPROPS: 
        idx.getParameters().putAll(alterIndex.getProps());
        break;
      case UPDATETIMESTAMP:
        try {
          Map<String, String> props = new HashMap<String, String>();
          Map<Map<String, String>, Long> basePartTs = new HashMap<Map<String, String>, Long>();
          Table baseTbl = db.getTable(db.getCurrentDatabase(), baseTableName);
          if (baseTbl.isPartitioned()) {
            List<Partition> baseParts;
            if (alterIndex.getSpec() != null) {
              baseParts = db.getPartitions(baseTbl, alterIndex.getSpec());
            } else {
              baseParts = db.getPartitions(baseTbl);
            }
            if (baseParts != null) {
              for (Partition p : baseParts) {
                FileSystem fs = p.getPartitionPath().getFileSystem(db.getConf());
                FileStatus fss = fs.getFileStatus(p.getPartitionPath());
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
      db.alterIndex(dbName, baseTableName, indexName, idx);
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
   * Add a partition to a table.
   *
   * @param db
   *          Database to add the partition to.
   * @param addPartitionDesc
   *          Add this partition.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   */
  /**
   * Add a partition to a table.
   *
   * @param db
   *          Database to add the partition to.
   * @param addPartitionDesc
   *          Add this partition.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   */
  private int addPartition(Hive db, AddPartitionDesc addPartitionDesc) throws HiveException {

    Table tbl = db.getTable(addPartitionDesc.getDbName(), addPartitionDesc.getTableName());

    validateAlterTableType(
      tbl, AlterTableDesc.AlterTableTypes.ADDPARTITION,
      addPartitionDesc.getExpectView());

    // If the add partition was created with IF NOT EXISTS, then we should
    // not throw an error if the specified part does exist.
    Partition checkPart = db.getPartition(tbl, addPartitionDesc.getPartSpec(), false);
    if (checkPart != null && addPartitionDesc.getIfNotExists()) {
      return 0;
    }



    if (addPartitionDesc.getLocation() == null) {
      db.createPartition(tbl, addPartitionDesc.getPartSpec(), null,
          addPartitionDesc.getPartParams(),
                    addPartitionDesc.getInputFormat(),
                    addPartitionDesc.getOutputFormat(),
                    addPartitionDesc.getNumBuckets(),
                    addPartitionDesc.getCols(),
                    addPartitionDesc.getSerializationLib(),
                    addPartitionDesc.getSerdeParams(),
                    addPartitionDesc.getBucketCols(),
                    addPartitionDesc.getSortCols());

    } else {
      if (tbl.isView()) {
        throw new HiveException("LOCATION clause illegal for view partition");
      }
      // set partition path relative to table
      db.createPartition(tbl, addPartitionDesc.getPartSpec(), new Path(tbl
                    .getPath(), addPartitionDesc.getLocation()), addPartitionDesc.getPartParams(),
                    addPartitionDesc.getInputFormat(),
                    addPartitionDesc.getOutputFormat(),
                    addPartitionDesc.getNumBuckets(),
                    addPartitionDesc.getCols(),
                    addPartitionDesc.getSerializationLib(),
                    addPartitionDesc.getSerdeParams(),
                    addPartitionDesc.getBucketCols(),
                    addPartitionDesc.getSortCols());
    }

    Partition part = db
        .getPartition(tbl, addPartitionDesc.getPartSpec(), false);
    work.getOutputs().add(new WriteEntity(part));

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

    String dbName = touchDesc.getDbName();
    String tblName = touchDesc.getTableName();

    Table tbl = db.getTable(dbName, tblName);

    validateAlterTableType(tbl, AlterTableDesc.AlterTableTypes.TOUCH);

    if (touchDesc.getPartSpec() == null) {
      try {
        db.alterTable(tblName, tbl);
      } catch (InvalidOperationException e) {
        throw new HiveException("Uable to update table");
      }
      work.getInputs().add(new ReadEntity(tbl));
      work.getOutputs().add(new WriteEntity(tbl));
    } else {
      Partition part = db.getPartition(tbl, touchDesc.getPartSpec(), false);
      if (part == null) {
        throw new HiveException("Specified partition does not exist");
      }
      try {
        db.alterPartition(tblName, part);
      } catch (InvalidOperationException e) {
        throw new HiveException(e);
      }
      work.getInputs().add(new ReadEntity(part));
      work.getOutputs().add(new WriteEntity(part));
    }
    return 0;
  }

  private void setIsArchived(Partition p, boolean state) {
    Map<String, String> params = p.getParameters();
    if (state) {
      params.put(org.apache.hadoop.hive.metastore.api.Constants.IS_ARCHIVED,
          "true");
    } else {
      params.remove(org.apache.hadoop.hive.metastore.api.Constants.IS_ARCHIVED);
    }
  }

  private String getOriginalLocation(Partition p) {
    Map<String, String> params = p.getParameters();
    return params.get(
        org.apache.hadoop.hive.metastore.api.Constants.ORIGINAL_LOCATION);
  }

  private void setOriginalLocation(Partition p, String loc) {
    Map<String, String> params = p.getParameters();
    if (loc == null) {
      params.remove(org.apache.hadoop.hive.metastore.api.Constants.ORIGINAL_LOCATION);
    } else {
      params.put(org.apache.hadoop.hive.metastore.api.Constants.ORIGINAL_LOCATION, loc);
    }
  }

  // Returns only the path component of the URI
  private String getArchiveDirOnly(Path parentDir, String archiveName) {
    URI parentUri = parentDir.toUri();
    Path harDir = new Path(parentUri.getPath(), archiveName);
    return harDir.toString();
  }

  /**
   * Sets the appropriate attributes in the supplied Partition object to mark
   * it as archived. Note that the metastore is not touched - a separate
   * call to alter_partition is needed.
   *
   * @param p - the partition object to modify
   * @param parentDir - the parent directory of the archive, which is the
   * original directory that the partition's files resided in
   * @param dirInArchive - the directory within the archive file that contains
   * the partitions files
   * @param archiveName - the name of the archive
   * @throws URISyntaxException
   */
  private void setArchived(Partition p, Path parentDir, String dirInArchive, String archiveName)
      throws URISyntaxException {
    assert(Utilities.isArchived(p) == false);
    Map<String, String> params = p.getParameters();

    URI parentUri = parentDir.toUri();
    String parentHost = parentUri.getHost();
    String harHost = null;
    if (parentHost == null) {
     harHost = "";
    } else {
      harHost = parentUri.getScheme() + "-" + parentHost;
    }

    // harUri is used to access the partition's files, which are in the archive
    // The format of the RI is something like:
    // har://underlyingfsscheme-host:port/archivepath
    URI harUri = null;
    if (dirInArchive.length() == 0) {
      harUri = new URI("har", parentUri.getUserInfo(), harHost, parentUri.getPort(),
          getArchiveDirOnly(parentDir, archiveName),
          parentUri.getQuery(), parentUri.getFragment());
    } else {
      harUri = new URI("har", parentUri.getUserInfo(), harHost, parentUri.getPort(),
          new Path(getArchiveDirOnly(parentDir, archiveName), dirInArchive).toUri().getPath(),
          parentUri.getQuery(), parentUri.getFragment());
    }
    setIsArchived(p, true);
    setOriginalLocation(p, parentDir.toString());
    p.setLocation(harUri.toString());
  }

  /**
   * Sets the appropriate attributes in the supplied Partition object to mark
   * it as not archived. Note that the metastore is not touched - a separate
   * call to alter_partition is needed.
   *
   * @param p - the partition to modify
   */
  private void setUnArchived(Partition p) {
    assert(Utilities.isArchived(p) == true);
    String parentDir = getOriginalLocation(p);
    setIsArchived(p, false);
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

  private int archive(Hive db, AlterTableSimpleDesc simpleDesc, DriverContext driverContext)
      throws HiveException {
    String dbName = simpleDesc.getDbName();
    String tblName = simpleDesc.getTableName();

    Table tbl = db.getTable(dbName, tblName);
    validateAlterTableType(tbl, AlterTableDesc.AlterTableTypes.ARCHIVE);

    Map<String, String> partSpec = simpleDesc.getPartSpec();
    Partition p = db.getPartition(tbl, partSpec, false);

    if (tbl.getTableType() != TableType.MANAGED_TABLE) {
      throw new HiveException("ARCHIVE can only be performed on managed tables");
    }

    if (p == null) {
      throw new HiveException("Specified partition does not exist");
    }

    if (Utilities.isArchived(p)) {
      // If there were a failure right after the metadata was updated in an
      // archiving operation, it's possible that the original, unarchived files
      // weren't deleted.
      Path originalDir = new Path(getOriginalLocation(p));
      Path leftOverIntermediateOriginal = new Path(originalDir.getParent(),
          originalDir.getName() + INTERMEDIATE_ORIGINAL_DIR_SUFFIX);

      if (pathExists(leftOverIntermediateOriginal)) {
        console.printInfo("Deleting " + leftOverIntermediateOriginal +
        " left over from a previous archiving operation");
        deleteDir(leftOverIntermediateOriginal);
      }

      throw new HiveException("Specified partition is already archived");
    }

    Path originalDir = p.getPartitionPath();
    Path intermediateArchivedDir = new Path(originalDir.getParent(),
        originalDir.getName() + INTERMEDIATE_ARCHIVED_DIR_SUFFIX);
    Path intermediateOriginalDir = new Path(originalDir.getParent(),
        originalDir.getName() + INTERMEDIATE_ORIGINAL_DIR_SUFFIX);
    String archiveName = "data.har";
    FileSystem fs = null;
    try {
      fs = originalDir.getFileSystem(conf);
    } catch (IOException e) {
      throw new HiveException(e);
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
      Path tmpDir = new Path(driverContext.getCtx().getExternalTmpFileURI(originalDir.toUri()), "partlevel");

      console.printInfo("Creating " + archiveName + " for " + originalDir.toString());
      console.printInfo("in " + tmpDir);
      console.printInfo("Please wait... (this may take a while)");

      // Create the Hadoop archive
      HadoopShims shim = ShimLoader.getHadoopShims();
      int ret=0;
      try {
        int maxJobNameLen = conf.getIntVar(HiveConf.ConfVars.HIVEJOBNAMELENGTH);
        String jobname = String.format("Archiving %s@%s",
          tbl.getTableName(), p.getName());
        jobname = Utilities.abbreviate(jobname, maxJobNameLen - 6);
        conf.setVar(HiveConf.ConfVars.HADOOPJOBNAME, jobname);
        ret = shim.createHadoopArchive(conf, originalDir, tmpDir, archiveName);
      } catch (Exception e) {
        throw new HiveException(e);
      }
      if (ret != 0) {
        throw new HiveException("Error while creating HAR");
      }
      // Move from the tmp dir to an intermediate directory, in the same level as
      // the partition directory. e.g. .../hr=12-intermediate-archived
      try {
        console.printInfo("Moving " + tmpDir + " to " + intermediateArchivedDir);
        if (pathExists(intermediateArchivedDir)) {
          throw new HiveException("The intermediate archive directory already exists.");
        }
        fs.rename(tmpDir, intermediateArchivedDir);
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
      boolean parentSettable =
        conf.getBoolVar(HiveConf.ConfVars.HIVEHARPARENTDIRSETTABLE);

      // dirInArchive is the directory within the archive that has all the files
      // for this partition. With older versions of Hadoop, archiving a
      // a directory would produce the same directory structure
      // in the archive. So if you created myArchive.har of /tmp/myDir, the
      // files in /tmp/myDir would be located under myArchive.har/tmp/myDir/*
      // In this case, dirInArchive should be tmp/myDir

      // With newer versions of Hadoop, the parent directory could be specified.
      // Assuming the parent directory was set to /tmp/myDir when creating the
      // archive, the files can be found under myArchive.har/*
      // In this case, dirInArchive should be empty

      String dirInArchive = "";
      if (!parentSettable) {
        dirInArchive = originalDir.toUri().getPath();
        if(dirInArchive.length() > 1 && dirInArchive.charAt(0)=='/') {
          dirInArchive = dirInArchive.substring(1);
        }
      }
      setArchived(p, originalDir, dirInArchive, archiveName);
      db.alterPartition(tblName, p);
    } catch (Exception e) {
      throw new HiveException("Unable to change the partition info for HAR", e);
    }

    // If a failure occurs here, the directory containing the original files
    // will not be deleted. The user will run ARCHIVE again to clear this up
    deleteDir(intermediateOriginalDir);


    return 0;
  }

  private int unarchive(Hive db, AlterTableSimpleDesc simpleDesc)
      throws HiveException {
    String dbName = simpleDesc.getDbName();
    String tblName = simpleDesc.getTableName();

    Table tbl = db.getTable(dbName, tblName);
    validateAlterTableType(tbl, AlterTableDesc.AlterTableTypes.UNARCHIVE);

    // Means user specified a table, not a partition
    if (simpleDesc.getPartSpec() == null) {
      throw new HiveException("ARCHIVE is for partitions only");
    }

    Map<String, String> partSpec = simpleDesc.getPartSpec();
    Partition p = db.getPartition(tbl, partSpec, false);

    if (tbl.getTableType() != TableType.MANAGED_TABLE) {
      throw new HiveException("UNARCHIVE can only be performed on managed tables");
    }

    if (p == null) {
      throw new HiveException("Specified partition does not exist");
    }

    if (!Utilities.isArchived(p)) {
      Path location = new Path(p.getLocation());
      Path leftOverArchiveDir = new Path(location.getParent(),
          location.getName() + INTERMEDIATE_ARCHIVED_DIR_SUFFIX);

      if (pathExists(leftOverArchiveDir)) {
        console.printInfo("Deleting " + leftOverArchiveDir + " left over " +
        "from a previous unarchiving operation");
        deleteDir(leftOverArchiveDir);
      }

      throw new HiveException("Specified partition is not archived");
    }

    Path originalLocation = new Path(getOriginalLocation(p));
    Path sourceDir = new Path(p.getLocation());
    Path intermediateArchiveDir = new Path(originalLocation.getParent(),
        originalLocation.getName() + INTERMEDIATE_ARCHIVED_DIR_SUFFIX);
    Path intermediateExtractedDir = new Path(originalLocation.getParent(),
        originalLocation.getName() + INTERMEDIATE_EXTRACTED_DIR_SUFFIX);

    Path tmpDir = new Path(driverContext
          .getCtx()
          .getExternalTmpFileURI(originalLocation.toUri()));

    FileSystem fs = null;
    try {
      fs = tmpDir.getFileSystem(conf);
      // Verify that there are no files in the tmp dir, because if there are, it
      // would be copied to the partition
      FileStatus [] filesInTmpDir = fs.listStatus(tmpDir);
      if (filesInTmpDir != null && filesInTmpDir.length != 0) {
        for (FileStatus file : filesInTmpDir) {
          console.printInfo(file.getPath().toString());
        }
        throw new HiveException("Temporary directory " + tmpDir + " is not empty");
      }

    } catch (IOException e) {
      throw new HiveException(e);
    }

    // Some sanity checks
    if (originalLocation == null) {
      throw new HiveException("Missing archive data in the partition");
    }
    if (!"har".equals(sourceDir.toUri().getScheme())) {
      throw new HiveException("Location should refer to a HAR");
    }

    // Clarification of terms:
    // - The originalLocation directory represents the original directory of the
    //   partition's files. They now contain an archived version of those files
    //   eg. hdfs:/warehouse/myTable/ds=1/
    // - The source directory is the directory containing all the files that
    //   should be in the partition. e.g. har:/warehouse/myTable/ds=1/myTable.har/
    //   Note the har:/ scheme

    // Steps:
    // 1. Extract the archive in a temporary folder
    // 2. Move the archive dir to an intermediate dir that is in at the same
    //    dir as originalLocation. Call the new dir intermediate-extracted.
    // 3. Rename the original partition dir to an intermediate dir. Call the
    //    renamed dir intermediate-archive
    // 4. Rename intermediate-extracted to the original partition dir
    // 5. Change the metadata
    // 6. Delete the archived partition files in intermediate-archive

    if (!pathExists(intermediateExtractedDir) &&
        !pathExists(intermediateArchiveDir)) {
      try {

        // Copy the files out of the archive into the temporary directory
        String copySource = sourceDir.toString();
        String copyDest = tmpDir.toString();
        List<String> args = new ArrayList<String>();
        args.add("-cp");
        args.add(copySource);
        args.add(copyDest);

        console.printInfo("Copying " + copySource + " to " + copyDest);
        FsShell fss = new FsShell(conf);
        int ret = 0;
        try {
          ret = ToolRunner.run(fss, args.toArray(new String[0]));
        } catch (Exception e) {
          throw new HiveException(e);
        }
        if (ret != 0) {
          throw new HiveException("Error while copying files from archive");
        }

        console.printInfo("Moving " + tmpDir + " to " + intermediateExtractedDir);
        if (fs.exists(intermediateExtractedDir)) {
          throw new HiveException("Invalid state: the intermediate extracted " +
              "directory already exists.");
        }
        fs.rename(tmpDir, intermediateExtractedDir);
      } catch (Exception e) {
        throw new HiveException(e);
      }
    }

    // At this point, we know that the extracted files are in the intermediate
    // extracted dir, or in the the original directory.

    if (!pathExists(intermediateArchiveDir)) {
      try {
        console.printInfo("Moving " + originalLocation + " to " + intermediateArchiveDir);
        fs.rename(originalLocation, intermediateArchiveDir);
      } catch (IOException e) {
        throw new HiveException(e);
      }
    } else {
      console.printInfo(intermediateArchiveDir + " already exists. " +
      "Assuming it contains the archived version of the partition");
    }

    // If there is a failure from here to until when the metadata is changed,
    // the partition will be empty or throw errors on read.

    // If the original location exists here, then it must be the extracted files
    // because in the previous step, we moved the previous original location
    // (containing the archived version of the files) to intermediateArchiveDir
    if (!pathExists(originalLocation)) {
      try {
        console.printInfo("Moving " + intermediateExtractedDir + " to " + originalLocation);
        fs.rename(intermediateExtractedDir, originalLocation);
      } catch (IOException e) {
        throw new HiveException(e);
      }
    } else {
      console.printInfo(originalLocation + " already exists. " +
      "Assuming it contains the extracted files in the partition");
    }

    setUnArchived(p);
    try {
      db.alterPartition(tblName, p);
    } catch (InvalidOperationException e) {
      throw new HiveException(e);
    }
    // If a failure happens here, the intermediate archive files won't be
    // deleted. The user will need to call unarchive again to clear those up.
    deleteDir(intermediateArchiveDir);

    return 0;
  }

  private void validateAlterTableType(
    Table tbl, AlterTableDesc.AlterTableTypes alterType) throws HiveException {

    validateAlterTableType(tbl, alterType, false);
  }

  private void validateAlterTableType(
    Table tbl, AlterTableDesc.AlterTableTypes alterType,
    boolean expectView) throws HiveException {

    if (tbl.isView()) {
      if (!expectView) {
        throw new HiveException("Cannot alter a view with ALTER TABLE");
      }
      switch (alterType) {
      case ADDPARTITION:
      case DROPPARTITION:
      case ADDPROPS:
      case RENAME:
        // allow this form
        break;
      default:
        throw new HiveException(
          "Cannot use this form of ALTER on a view");
      }
    } else {
      if (expectView) {
        throw new HiveException("Cannot alter a base table with ALTER VIEW");
      }
    }

    if (tbl.isNonNative()) {
      throw new HiveException("Cannot use ALTER TABLE on a non-native table");
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
    CheckResult result = new CheckResult();
    List<String> repairOutput = new ArrayList<String>();
    try {
      HiveMetaStoreChecker checker = new HiveMetaStoreChecker(db);
      Table t = db.newTable(msckDesc.getTableName());
      checker.checkMetastore(t.getDbName(), t.getTableName(), msckDesc.getPartSpecs(), result);
      if (msckDesc.isRepairPartitions()) {
        Table table = db.getTable(msckDesc.getTableName());
        for (CheckResult.PartitionResult part : result.getPartitionsNotInMs()) {
          try {
            db.createPartition(table, Warehouse.makeSpecFromName(part
                .getPartitionName()));
            repairOutput.add("Repair: Added partition to metastore "
                + msckDesc.getTableName() + ':' + part.getPartitionName());
          } catch (Exception e) {
            LOG.warn("Repair error, could not add partition to metastore: ", e);
          }
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
      console.printError("Table " + tabName + " is not a partitioned table");
      return 1;
    }
    if (showParts.getPartSpec() != null) {
      parts = db.getPartitionNames(tbl.getDbName(),
          tbl.getTableName(), showParts.getPartSpec(), (short) -1);
    } else {
      parts = db.getPartitionNames(tbl.getDbName(), tbl.getTableName(), (short) -1);
    }

    // write the results in the file
    DataOutput outStream = null;
    try {
      Path resFile = new Path(showParts.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);
      Iterator<String> iterParts = parts.iterator();

      while (iterParts.hasNext()) {
        // create a row per partition name
        outStream.writeBytes(iterParts.next());
        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
      outStream = null;
    } catch (FileNotFoundException e) {
      LOG.info("show partitions: " + stringifyException(e));
      throw new HiveException(e.toString());
    } catch (IOException e) {
      LOG.info("show partitions: " + stringifyException(e));
      throw new HiveException(e.toString());
    } catch (Exception e) {
      throw new HiveException(e.toString());
    } finally {
      IOUtils.closeStream((FSDataOutputStream) outStream);
    }

    return 0;
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
    DataOutput outStream = null;
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

      ((FSDataOutputStream) outStream).close();
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
      IOUtils.closeStream((FSDataOutputStream) outStream);
    }

    return 0;
  }

  /**
   * Write a list of the available databases to a file.
   *
   * @param showDatabases
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
    DataOutput outStream = null;
    try {
      Path resFile = new Path(showDatabasesDesc.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);

      for (String database : databases) {
        // create a row per database name
        outStream.writeBytes(database);
        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
      outStream = null;
    } catch (FileNotFoundException e) {
      LOG.warn("show databases: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show databases: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    } finally {
      IOUtils.closeStream((FSDataOutputStream) outStream);
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
    // get the tables for the desired pattenn - populate the output stream
    List<String> tbls = null;
    String dbName = showTbls.getDbName();

    if (!db.databaseExists(dbName)) {
      throw new HiveException("ERROR: The database " + dbName + " does not exist.");

    }
    if (showTbls.getPattern() != null) {
      LOG.info("pattern: " + showTbls.getPattern());
      tbls = db.getTablesByPattern(dbName, showTbls.getPattern());
      LOG.info("results : " + tbls.size());
    } else {
      tbls = db.getAllTables(dbName);
    }

    // write the results in the file
    DataOutput outStream = null;
    try {
      Path resFile = new Path(showTbls.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);
      SortedSet<String> sortedTbls = new TreeSet<String>(tbls);
      Iterator<String> iterTbls = sortedTbls.iterator();

      while (iterTbls.hasNext()) {
        // create a row per table name
        outStream.writeBytes(iterTbls.next());
        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
      outStream = null;
    } catch (FileNotFoundException e) {
      LOG.warn("show table: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show table: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    } finally {
      IOUtils.closeStream((FSDataOutputStream) outStream);
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
    // get the tables for the desired pattenn - populate the output stream
    Set<String> funcs = null;
    if (showFuncs.getPattern() != null) {
      LOG.info("pattern: " + showFuncs.getPattern());
      funcs = FunctionRegistry.getFunctionNames(showFuncs.getPattern());
      LOG.info("results : " + funcs.size());
    } else {
      funcs = FunctionRegistry.getFunctionNames();
    }

    // write the results in the file
    DataOutput outStream = null;
    try {
      Path resFile = new Path(showFuncs.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);
      SortedSet<String> sortedFuncs = new TreeSet<String>(funcs);
      Iterator<String> iterFuncs = sortedFuncs.iterator();

      while (iterFuncs.hasNext()) {
        // create a row per table name
        outStream.writeBytes(iterFuncs.next());
        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
      outStream = null;
    } catch (FileNotFoundException e) {
      LOG.warn("show function: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show function: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    } finally {
      IOUtils.closeStream((FSDataOutputStream) outStream);
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
    HiveLockManager lockMgr = ctx.getHiveLockMgr();
    boolean isExt = showLocks.isExt();
    if (lockMgr == null) {
      throw new HiveException("show Locks LockManager not specified");
    }

    // write the results in the file
    DataOutput outStream = null;
    try {
      Path resFile = new Path(showLocks.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);
      List<HiveLock> locks = null;

      if (showLocks.getTableName() == null) {
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
      ((FSDataOutputStream) outStream).close();
      outStream = null;
    } catch (FileNotFoundException e) {
      LOG.warn("show function: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show function: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    } finally {
      IOUtils.closeStream((FSDataOutputStream) outStream);
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
    HiveLockManager lockMgr = ctx.getHiveLockMgr();
    if (lockMgr == null) {
      throw new HiveException("lock Table LockManager not specified");
    }

    HiveLockMode mode = HiveLockMode.valueOf(lockTbl.getMode());
    String tabName = lockTbl.getTableName();
    Table  tbl = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tabName);
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
    HiveLockManager lockMgr = ctx.getHiveLockMgr();
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
  private int describeFunction(DescFunctionDesc descFunc) throws HiveException {
    String funcName = descFunc.getName();

    // write the results in the file
    DataOutput outStream = null;
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
        desc = funcClass.getAnnotation(Description.class);
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

      ((FSDataOutputStream) outStream).close();
      outStream = null;
    } catch (FileNotFoundException e) {
      LOG.warn("describe function: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("describe function: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    } finally {
      IOUtils.closeStream((FSDataOutputStream) outStream);
    }
    return 0;
  }

  private int descDatabase(DescDatabaseDesc descDatabase) throws HiveException {
    DataOutput outStream = null;
    try {
      Path resFile = new Path(descDatabase.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);

      Database database = db.getDatabase(descDatabase.getDatabaseName());

      if (database != null) {
        outStream.writeBytes(database.getName());
        outStream.write(separator);
        if (database.getDescription() != null) {
          outStream.writeBytes(database.getDescription());
        }
        outStream.write(separator);
        if (database.getLocationUri() != null) {
          outStream.writeBytes(database.getLocationUri());
        }

        outStream.write(separator);
        if (descDatabase.isExt() && database.getParametersSize() > 0) {
          Map<String, String> params = database.getParameters();
          outStream.writeBytes(params.toString());
        }

      } else {
        outStream.writeBytes("No such database: " + descDatabase.getDatabaseName());
      }

      outStream.write(terminator);

      ((FSDataOutputStream) outStream).close();
      outStream = null;

    } catch (FileNotFoundException e) {
      LOG.warn("describe database: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("describe database: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    } finally {
      IOUtils.closeStream((FSDataOutputStream) outStream);
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
    // get the tables for the desired pattenn - populate the output stream
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
    DataOutput outStream = null;
    try {
      Path resFile = new Path(showTblStatus.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);

      Iterator<Table> iterTables = tbls.iterator();
      while (iterTables.hasNext()) {
        // create a row per table name
        Table tbl = iterTables.next();
        String tableName = tbl.getTableName();
        String tblLoc = null;
        String inputFormattCls = null;
        String outputFormattCls = null;
        if (part != null) {
          if (par != null) {
            if (par.getLocation() != null) {
              tblLoc = par.getDataLocation().toString();
            }
            inputFormattCls = par.getInputFormatClass().getName();
            outputFormattCls = par.getOutputFormatClass().getName();
          }
        } else {
          if (tbl.getPath() != null) {
            tblLoc = tbl.getDataLocation().toString();
          }
          inputFormattCls = tbl.getInputFormatClass().getName();
          outputFormattCls = tbl.getOutputFormatClass().getName();
        }

        String owner = tbl.getOwner();
        List<FieldSchema> cols = tbl.getCols();
        String ddlCols = MetaStoreUtils.getDDLFromFieldSchema("columns", cols);
        boolean isPartitioned = tbl.isPartitioned();
        String partitionCols = "";
        if (isPartitioned) {
          partitionCols = MetaStoreUtils.getDDLFromFieldSchema(
              "partition_columns", tbl.getPartCols());
        }

        outStream.writeBytes("tableName:" + tableName);
        outStream.write(terminator);
        outStream.writeBytes("owner:" + owner);
        outStream.write(terminator);
        outStream.writeBytes("location:" + tblLoc);
        outStream.write(terminator);
        outStream.writeBytes("inputformat:" + inputFormattCls);
        outStream.write(terminator);
        outStream.writeBytes("outputformat:" + outputFormattCls);
        outStream.write(terminator);
        outStream.writeBytes("columns:" + ddlCols);
        outStream.write(terminator);
        outStream.writeBytes("partitioned:" + isPartitioned);
        outStream.write(terminator);
        outStream.writeBytes("partitionColumns:" + partitionCols);
        outStream.write(terminator);
        // output file system information
        Path tablLoc = tbl.getPath();
        List<Path> locations = new ArrayList<Path>();
        if (isPartitioned) {
          if (par == null) {
            for (Partition curPart : db.getPartitions(tbl)) {
              if (curPart.getLocation() != null) {
                locations.add(new Path(curPart.getLocation()));
              }
            }
          } else {
            if (par.getLocation() != null) {
              locations.add(new Path(par.getLocation()));
            }
          }
        } else {
          if (tablLoc != null) {
            locations.add(tablLoc);
          }
        }
        if (!locations.isEmpty()) {
          writeFileSystemStats(outStream, locations, tablLoc, false, 0);
        }

        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
      outStream = null;
    } catch (FileNotFoundException e) {
      LOG.info("show table status: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.info("show table status: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e);
    } finally {
      IOUtils.closeStream((FSDataOutputStream) outStream);
    }
    return 0;
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
    String colPath = descTbl.getTableName();
    String tableName = colPath.substring(0,
        colPath.indexOf('.') == -1 ? colPath.length() : colPath.indexOf('.'));

    // describe the table - populate the output stream
    Table tbl = db.getTable(tableName, false);
    Partition part = null;
    DataOutput outStream = null;
    try {
      Path resFile = new Path(descTbl.getResFile());
      if (tbl == null) {
        FileSystem fs = resFile.getFileSystem(conf);
        outStream = (DataOutput) fs.open(resFile);
        String errMsg = "Table " + tableName + " does not exist";
        outStream.write(errMsg.getBytes("UTF-8"));
        ((FSDataOutputStream) outStream).close();
        outStream = null;
        return 0;
      }
      if (descTbl.getPartSpec() != null) {
        part = db.getPartition(tbl, descTbl.getPartSpec(), false);
        if (part == null) {
          FileSystem fs = resFile.getFileSystem(conf);
          outStream = (DataOutput) fs.open(resFile);
          String errMsg = "Partition " + descTbl.getPartSpec() + " for table "
              + tableName + " does not exist";
          outStream.write(errMsg.getBytes("UTF-8"));
          ((FSDataOutputStream) outStream).close();
          outStream = null;
          return 0;
        }
        tbl = part.getTable();
      }
    } catch (FileNotFoundException e) {
      LOG.info("describe table: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.info("describe table: " + stringifyException(e));
      return 1;
    } finally {
      IOUtils.closeStream((FSDataOutputStream) outStream);
    }

    try {

      LOG.info("DDLTask: got data for " + tbl.getTableName());

      Path resFile = new Path(descTbl.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);

      if (colPath.equals(tableName)) {
        if (!descTbl.isFormatted()) {
          List<FieldSchema> cols = tbl.getCols();
          if (tableName.equals(colPath)) {
            cols.addAll(tbl.getPartCols());
          }
          outStream.writeBytes(MetaDataFormatUtils.displayColsUnformatted(cols));
        } else {
          outStream.writeBytes(MetaDataFormatUtils.getAllColumnsInformation(tbl));
        }
      } else {
        List<FieldSchema> cols = Hive.getFieldsFromDeserializer(colPath, tbl.getDeserializer());
        if (descTbl.isFormatted()) {
          outStream.writeBytes(MetaDataFormatUtils.getAllColumnsInformation(cols));
        } else {
          outStream.writeBytes(MetaDataFormatUtils.displayColsUnformatted(cols));
        }
      }

      if (tableName.equals(colPath)) {

        if (descTbl.isFormatted()) {
          if (part != null) {
            outStream.writeBytes(MetaDataFormatUtils.getPartitionInformation(part));
          } else {
            outStream.writeBytes(MetaDataFormatUtils.getTableInformation(tbl));
          }
        }

        // if extended desc table then show the complete details of the table
        if (descTbl.isExt()) {
          // add empty line
          outStream.write(terminator);
          if (part != null) {
            // show partition information
            outStream.writeBytes("Detailed Partition Information");
            outStream.write(separator);
            outStream.writeBytes(part.getTPartition().toString());
            outStream.write(separator);
            // comment column is empty
            outStream.write(terminator);
          } else {
            // show table information
            outStream.writeBytes("Detailed Table Information");
            outStream.write(separator);
            outStream.writeBytes(tbl.getTTable().toString());
            outStream.write(separator);
            outStream.write(terminator);
          }
        }
      }

      LOG.info("DDLTask: written data for " + tbl.getTableName());
      ((FSDataOutputStream) outStream).close();
      outStream = null;

    } catch (FileNotFoundException e) {
      LOG.info("describe table: " + stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.info("describe table: " + stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e);
    } finally {
      IOUtils.closeStream((FSDataOutputStream) outStream);
    }

    return 0;
  }

  public static void writeGrantInfo(DataOutput outStream,
      PrincipalType principalType, String principalName, String dbName,
      String tableName, String partName, String columnName,
      PrivilegeGrantInfo grantInfo) throws IOException {

    String privilege = grantInfo.getPrivilege();
    long unixTimestamp = grantInfo.getCreateTime() * 1000L;
    Date createTime = new Date(unixTimestamp);
    String grantor = grantInfo.getGrantor();

    if (dbName != null) {
      writeKeyValuePair(outStream, "database", dbName);
    }
    if (tableName != null) {
      writeKeyValuePair(outStream, "table", tableName);
    }
    if (partName != null) {
      writeKeyValuePair(outStream, "partition", partName);
    }
    if (columnName != null) {
      writeKeyValuePair(outStream, "columnName", columnName);
    }

    writeKeyValuePair(outStream, "principalName", principalName);
    writeKeyValuePair(outStream, "principalType", "" + principalType);
    writeKeyValuePair(outStream, "privilege", privilege);
    writeKeyValuePair(outStream, "grantTime", "" + createTime);
    if (grantor != null) {
      writeKeyValuePair(outStream, "grantor", grantor);
    }
  }

  private static void writeKeyValuePair(DataOutput outStream, String key,
      String value) throws IOException {
    outStream.write(terminator);
    outStream.writeBytes(key);
    outStream.write(separator);
    outStream.writeBytes(value);
    outStream.write(separator);
  }

  private void writeFileSystemStats(DataOutput outStream, List<Path> locations,
      Path tabLoc, boolean partSpecified, int indent) throws IOException {
    long totalFileSize = 0;
    long maxFileSize = 0;
    long minFileSize = Long.MAX_VALUE;
    long lastAccessTime = 0;
    long lastUpdateTime = 0;
    int numOfFiles = 0;

    boolean unknown = false;
    FileSystem fs = tabLoc.getFileSystem(conf);
    // in case all files in locations do not exist
    try {
      FileStatus tmpStatus = fs.getFileStatus(tabLoc);
      lastAccessTime = ShimLoader.getHadoopShims().getAccessTime(tmpStatus);
      lastUpdateTime = tmpStatus.getModificationTime();
      if (partSpecified) {
        // check whether the part exists or not in fs
        tmpStatus = fs.getFileStatus(locations.get(0));
      }
    } catch (IOException e) {
      LOG.warn(
          "Cannot access File System. File System status will be unknown: ", e);
      unknown = true;
    }

    if (!unknown) {
      for (Path loc : locations) {
        try {
          FileStatus status = fs.getFileStatus(tabLoc);
          FileStatus[] files = fs.listStatus(loc);
          long accessTime = ShimLoader.getHadoopShims().getAccessTime(status);
          long updateTime = status.getModificationTime();
          // no matter loc is the table location or part location, it must be a
          // directory.
          if (!status.isDir()) {
            continue;
          }
          if (accessTime > lastAccessTime) {
            lastAccessTime = accessTime;
          }
          if (updateTime > lastUpdateTime) {
            lastUpdateTime = updateTime;
          }
          for (FileStatus currentStatus : files) {
            if (currentStatus.isDir()) {
              continue;
            }
            numOfFiles++;
            long fileLen = currentStatus.getLen();
            totalFileSize += fileLen;
            if (fileLen > maxFileSize) {
              maxFileSize = fileLen;
            }
            if (fileLen < minFileSize) {
              minFileSize = fileLen;
            }
            accessTime = ShimLoader.getHadoopShims().getAccessTime(
                currentStatus);
            updateTime = currentStatus.getModificationTime();
            if (accessTime > lastAccessTime) {
              lastAccessTime = accessTime;
            }
            if (updateTime > lastUpdateTime) {
              lastUpdateTime = updateTime;
            }
          }
        } catch (IOException e) {
          // ignore
        }
      }
    }
    String unknownString = "unknown";

    for (int k = 0; k < indent; k++) {
      outStream.writeBytes(Utilities.INDENT);
    }
    outStream.writeBytes("totalNumberFiles:");
    outStream.writeBytes(unknown ? unknownString : "" + numOfFiles);
    outStream.write(terminator);

    for (int k = 0; k < indent; k++) {
      outStream.writeBytes(Utilities.INDENT);
    }
    outStream.writeBytes("totalFileSize:");
    outStream.writeBytes(unknown ? unknownString : "" + totalFileSize);
    outStream.write(terminator);

    for (int k = 0; k < indent; k++) {
      outStream.writeBytes(Utilities.INDENT);
    }
    outStream.writeBytes("maxFileSize:");
    outStream.writeBytes(unknown ? unknownString : "" + maxFileSize);
    outStream.write(terminator);

    for (int k = 0; k < indent; k++) {
      outStream.writeBytes(Utilities.INDENT);
    }
    outStream.writeBytes("minFileSize:");
    if (numOfFiles > 0) {
      outStream.writeBytes(unknown ? unknownString : "" + minFileSize);
    } else {
      outStream.writeBytes(unknown ? unknownString : "" + 0);
    }
    outStream.write(terminator);

    for (int k = 0; k < indent; k++) {
      outStream.writeBytes(Utilities.INDENT);
    }
    outStream.writeBytes("lastAccessTime:");
    outStream.writeBytes((unknown || lastAccessTime < 0) ? unknownString : ""
        + lastAccessTime);
    outStream.write(terminator);

    for (int k = 0; k < indent; k++) {
      outStream.writeBytes(Utilities.INDENT);
    }
    outStream.writeBytes("lastUpdateTime:");
    outStream.writeBytes(unknown ? unknownString : "" + lastUpdateTime);
    outStream.write(terminator);
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

    Partition part = null;
    if(alterTbl.getPartSpec() != null) {
      part = db.getPartition(tbl, alterTbl.getPartSpec(), false);
      if(part == null) {
        console.printError("Partition : " + alterTbl.getPartSpec().toString()
            + " does not exist.");
        return 1;
      }
    }

    validateAlterTableType(tbl, alterTbl.getOp(), alterTbl.getExpectView());

    Table oldTbl = tbl.copy();

    if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.RENAME) {
      tbl.setTableName(alterTbl.getNewName());
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDCOLS) {
      List<FieldSchema> newCols = alterTbl.getNewCols();
      List<FieldSchema> oldCols = tbl.getCols();
      if (tbl.getSerializationLib().equals(
          "org.apache.hadoop.hive.serde.thrift.columnsetSerDe")) {
        console
            .printInfo("Replacing columns for columnsetSerDe and changing to LazySimpleSerDe");
        tbl.setSerializationLib(LazySimpleSerDe.class.getName());
        tbl.getTTable().getSd().setCols(newCols);
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
              console.printError("Column '" + newColName + "' exists");
              return 1;
            }
          }
          oldCols.add(newCol);
        }
        tbl.getTTable().getSd().setCols(oldCols);
      }
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.RENAMECOLUMN) {
      List<FieldSchema> oldCols = tbl.getCols();
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
          console.printError("Column '" + newName + "' exists");
          return 1;
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
        console.printError("Column '" + oldName + "' does not exist");
        return 1;
      }
      // after column is not null, but we did not find it.
      if ((afterCol != null && !afterCol.trim().equals("")) && position < 0) {
        console.printError("Column '" + afterCol + "' does not exist");
        return 1;
      }

      if (position >= 0) {
        newCols.add(position, column);
      }

      tbl.getTTable().getSd().setCols(newCols);
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.REPLACECOLS) {
      // change SerDe to LazySimpleSerDe if it is columnsetSerDe
      if (tbl.getSerializationLib().equals(
          "org.apache.hadoop.hive.serde.thrift.columnsetSerDe")) {
        console
            .printInfo("Replacing columns for columnsetSerDe and changing to LazySimpleSerDe");
        tbl.setSerializationLib(LazySimpleSerDe.class.getName());
      } else if (!tbl.getSerializationLib().equals(
          MetadataTypedColumnsetSerDe.class.getName())
          && !tbl.getSerializationLib().equals(LazySimpleSerDe.class.getName())
          && !tbl.getSerializationLib().equals(ColumnarSerDe.class.getName())
          && !tbl.getSerializationLib().equals(DynamicSerDe.class.getName())) {
        console.printError("Replace columns is not supported for this table. "
            + "SerDe may be incompatible.");
        return 1;
      }
      tbl.getTTable().getSd().setCols(alterTbl.getNewCols());
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDPROPS) {
      tbl.getTTable().getParameters().putAll(alterTbl.getProps());
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDSERDEPROPS) {
      tbl.getTTable().getSd().getSerdeInfo().getParameters().putAll(
          alterTbl.getProps());
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDSERDE) {
      tbl.setSerializationLib(alterTbl.getSerdeName());
      if ((alterTbl.getProps() != null) && (alterTbl.getProps().size() > 0)) {
        tbl.getTTable().getSd().getSerdeInfo().getParameters().putAll(
            alterTbl.getProps());
      }
      tbl.setFields(Hive.getFieldsFromDeserializer(tbl.getTableName(), tbl
          .getDeserializer()));
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDFILEFORMAT) {
      if(part != null) {
        part.getTPartition().getSd().setInputFormat(alterTbl.getInputFormat());
        part.getTPartition().getSd().setOutputFormat(alterTbl.getOutputFormat());
        if (alterTbl.getSerdeName() != null) {
          part.getTPartition().getSd().getSerdeInfo().setSerializationLib(
              alterTbl.getSerdeName());
        }
      } else {
        tbl.getTTable().getSd().setInputFormat(alterTbl.getInputFormat());
        tbl.getTTable().getSd().setOutputFormat(alterTbl.getOutputFormat());
        if (alterTbl.getSerdeName() != null) {
          tbl.setSerializationLib(alterTbl.getSerdeName());
        }
      }
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ALTERPROTECTMODE) {
      boolean protectModeEnable = alterTbl.isProtectModeEnable();
      AlterTableDesc.ProtectModeType protectMode = alterTbl.getProtectModeType();

      ProtectMode mode = null;
      if(part != null) {
        mode = part.getProtectMode();
      } else {
        mode = tbl.getProtectMode();
      }

      if (protectModeEnable
          && protectMode == AlterTableDesc.ProtectModeType.OFFLINE) {
        mode.offline = true;
      } else if (protectModeEnable
          && protectMode == AlterTableDesc.ProtectModeType.NO_DROP) {
        mode.noDrop = true;
      } else if (!protectModeEnable
          && protectMode == AlterTableDesc.ProtectModeType.OFFLINE) {
        mode.offline = false;
      } else if (!protectModeEnable
          && protectMode == AlterTableDesc.ProtectModeType.NO_DROP) {
        mode.noDrop = false;
      }

      if (part != null) {
        part.setProtectMode(mode);
      } else {
        tbl.setProtectMode(mode);
      }

    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDCLUSTERSORTCOLUMN) {
      // validate sort columns and bucket columns
      List<String> columns = Utilities.getColumnNamesFromFieldSchema(tbl
          .getCols());
      Utilities.validateColumnNames(columns, alterTbl.getBucketColumns());
      if (alterTbl.getSortColumns() != null) {
        Utilities.validateColumnNames(columns, Utilities
            .getColumnNamesFromSortCols(alterTbl.getSortColumns()));
      }

      int numBuckets = -1;
      ArrayList<String> bucketCols = null;
      ArrayList<Order> sortCols = null;

      // -1 buckets means to turn off bucketing
      if (alterTbl.getNumberBuckets() == -1) {
        bucketCols = new ArrayList<String>();
        sortCols = new ArrayList<Order>();
        numBuckets = -1;
      } else {
        bucketCols = alterTbl.getBucketColumns();
        sortCols = alterTbl.getSortColumns();
        numBuckets = alterTbl.getNumberBuckets();
      }
      tbl.getTTable().getSd().setBucketCols(bucketCols);
      tbl.getTTable().getSd().setNumBuckets(numBuckets);
      tbl.getTTable().getSd().setSortCols(sortCols);
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ALTERLOCATION) {
      String newLocation = alterTbl.getNewLocation();
      try {
        URI locURI = new URI(newLocation);
        if (!locURI.isAbsolute() || locURI.getScheme() == null
            || locURI.getScheme().trim().equals("")) {
          throw new HiveException(
              newLocation
                  + " is not absolute or has no scheme information. "
                  + "Please specify a complete absolute uri with scheme information.");
        }
        if (part != null) {
          part.setLocation(newLocation);
        } else {
          tbl.setDataLocation(locURI);
        }
      } catch (URISyntaxException e) {
        throw new HiveException(e);
      }
    } else {
      console.printError("Unsupported Alter commnad");
      return 1;
    }

    if(part == null) {
      if (!updateModifiedParameters(tbl.getTTable().getParameters(), conf)) {
        return 1;
      }
      try {
        tbl.checkValidity();
      } catch (HiveException e) {
        console.printError("Invalid table columns : " + e.getMessage(),
            stringifyException(e));
        return 1;
      }
    } else {
      if (!updateModifiedParameters(part.getParameters(), conf)) {
        return 1;
      }
    }

    try {
      if (part == null) {
        db.alterTable(alterTbl.getOldName(), tbl);
      } else {
        db.alterPartition(tbl.getTableName(), part);
      }
    } catch (InvalidOperationException e) {
      console.printError("Invalid alter operation: " + e.getMessage());
      LOG.info("alter table: " + stringifyException(e));
      return 1;
    } catch (HiveException e) {
      return 1;
    }

    // This is kind of hacky - the read entity contains the old table, whereas
    // the write entity
    // contains the new table. This is needed for rename - both the old and the
    // new table names are
    // passed
    if(part != null) {
      work.getInputs().add(new ReadEntity(part));
      work.getOutputs().add(new WriteEntity(part));
    } else {
      work.getInputs().add(new ReadEntity(oldTbl));
      work.getOutputs().add(new WriteEntity(tbl));
    }
    return 0;
  }

  /**
   * Drop a given table.
   *
   * @param db
   *          The database in question.
   * @param dropTbl
   *          This is the table we're dropping.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int dropTable(Hive db, DropTableDesc dropTbl) throws HiveException {
    // We need to fetch the table before it is dropped so that it can be passed
    // to
    // post-execution hook
    Table tbl = null;
    try {
      tbl = db.getTable(dropTbl.getTableName());
    } catch (InvalidTableException e) {
      // drop table is idempotent
    }

    if (dropTbl.getPartSpecs() == null) {
      // This is a true DROP TABLE
      if (tbl != null) {
        if (tbl.isView()) {
          if (!dropTbl.getExpectView()) {
            if (dropTbl.getIfExists()) {
              return 0;
            }
            throw new HiveException("Cannot drop a view with DROP TABLE");
          }
        } else {
          if (dropTbl.getExpectView()) {
            if (dropTbl.getIfExists()) {
              return 0;
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

      // We should check that all the partitions of the table can be dropped
      if (tbl != null && tbl.isPartitioned()) {
        List<Partition> listPartitions = db.getPartitions(tbl);
        for (Partition p: listPartitions) {
            if (!p.canDrop()) {
              throw new HiveException("Table " + tbl.getTableName() +
                  " Partition" + p.getName() +
                  " is protected from being dropped");
            }
        }
      }

      // drop the table
      db.dropTable(dropTbl.getTableName());
      if (tbl != null) {
        work.getOutputs().add(new WriteEntity(tbl));
      }
    } else {
      // This is actually an ALTER TABLE DROP PARTITION
      if (tbl != null) {
        validateAlterTableType(
          tbl, AlterTableDesc.AlterTableTypes.DROPPARTITION,
          dropTbl.getExpectView());
      }

      List<Partition> partsToDelete = new ArrayList<Partition>();
      for (Map<String, String> partSpec : dropTbl.getPartSpecs()) {
        List<Partition> partitions = db.getPartitions(tbl, partSpec);
        for (Partition p : partitions) {
          if (!p.canDrop()) {
            throw new HiveException("Table " + tbl.getTableName() +
                " Partition " + p.getName() +
                " is protected from being dropped");
          }
          partsToDelete.add(p);
        }
      }

      // drop all existing partitions from the list
      for (Partition partition : partsToDelete) {
        console.printInfo("Dropping the partition " + partition.getName());
        db.dropPartition(dropTbl.getTableName(), partition.getValues(), true);
        work.getOutputs().add(new WriteEntity(partition));
      }
    }

    return 0;
  }

  /**
   * Update last_modified_by and last_modified_time parameters in parameter map.
   *
   * @param params
   *          Parameters.
   * @param user
   *          user that is doing the updating.
   */
  private boolean updateModifiedParameters(Map<String, String> params, HiveConf conf) {
    String user = null;
    try {
      user = conf.getUser();
    } catch (IOException e) {
      console.printError("Unable to get current user: " + e.getMessage(),
          stringifyException(e));
      return false;
    }

    params.put("last_modified_by", user);
    params.put("last_modified_time", Long.toString(System.currentTimeMillis() / 1000));
    return true;
  }

  /**
   * Check if the given serde is valid.
   */
  private void validateSerDe(String serdeName) throws HiveException {
    try {
      Deserializer d = SerDeUtils.lookupDeserializer(serdeName);
      if (d != null) {
        LOG.debug("Found class for " + serdeName);
      }
    } catch (SerDeException e) {
      throw new HiveException("Cannot validate serde: " + serdeName, e);
    }
  }

  /**
   * Create a Database
   * @param db
   * @param crtDb
   * @return Always returns 0
   * @throws HiveException
   * @throws AlreadyExistsException
   */
  private int createDatabase(Hive db, CreateDatabaseDesc crtDb)
      throws HiveException, AlreadyExistsException {
    Database database = new Database();
    database.setName(crtDb.getName());
    database.setDescription(crtDb.getComment());
    database.setLocationUri(crtDb.getLocationUri());
    database.setParameters(crtDb.getDatabaseProperties());

    db.createDatabase(database, crtDb.getIfNotExists());
    return 0;
  }

  /**
   * Drop a Database
   * @param db
   * @param dropDb
   * @return Always returns 0
   * @throws HiveException
   * @throws NoSuchObjectException
   */
  private int dropDatabase(Hive db, DropDatabaseDesc dropDb)
      throws HiveException, NoSuchObjectException {
    db.dropDatabase(dropDb.getDatabaseName(), true, dropDb.getIfExists(), dropDb.isCasdade());
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
      throw new HiveException("ERROR: The database " + dbName + " does not exist.");
    }
    db.setCurrentDatabase(dbName);

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
    Table tbl = db.newTable(crtTbl.getTableName());

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
        org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_STORAGE,
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
      tbl.setSerdeParam(Constants.FIELD_DELIM, crtTbl.getFieldDelim());
      tbl.setSerdeParam(Constants.SERIALIZATION_FORMAT, crtTbl.getFieldDelim());
    }
    if (crtTbl.getFieldEscape() != null) {
      tbl.setSerdeParam(Constants.ESCAPE_CHAR, crtTbl.getFieldEscape());
    }

    if (crtTbl.getCollItemDelim() != null) {
      tbl.setSerdeParam(Constants.COLLECTION_DELIM, crtTbl.getCollItemDelim());
    }
    if (crtTbl.getMapKeyDelim() != null) {
      tbl.setSerdeParam(Constants.MAPKEY_DELIM, crtTbl.getMapKeyDelim());
    }
    if (crtTbl.getLineDelim() != null) {
      tbl.setSerdeParam(Constants.LINE_DELIM, crtTbl.getLineDelim());
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
      tbl.setDataLocation(new Path(crtTbl.getLocation()).toUri());
    }

    tbl.setInputFormatClass(crtTbl.getInputFormat());
    tbl.setOutputFormatClass(crtTbl.getOutputFormat());

    tbl.getTTable().getSd().setInputFormat(
      tbl.getInputFormatClass().getName());
    tbl.getTTable().getSd().setOutputFormat(
      tbl.getOutputFormatClass().getName());

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

    int rc = setGenericTableAttributes(tbl);
    if (rc != 0) {
      return rc;
    }

    // create the table
    db.createTable(tbl, crtTbl.getIfNotExists());
    work.getOutputs().add(new WriteEntity(tbl));
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
  private int createTableLike(Hive db, CreateTableLikeDesc crtTbl) throws HiveException {
    // Get the existing table
    Table oldtbl = db.getTable(crtTbl.getLikeTableName());
    Table tbl;
    if (oldtbl.getTableType() == TableType.VIRTUAL_VIEW) {
      String targetTableName = crtTbl.getTableName();
      tbl=db.newTable(targetTableName);

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
      Table newTable = db.newTable(targetTableName);

      tbl.setDbName(newTable.getDbName());
      tbl.setTableName(newTable.getTableName());

      if (crtTbl.getLocation() != null) {
        tbl.setDataLocation(new Path(crtTbl.getLocation()).toUri());
      } else {
        tbl.unsetDataLocation();
      }

      // we should reset table specific parameters including (stats, lastDDLTime etc.)
      Map<String, String> params = tbl.getParameters();
      params.clear();

      if (crtTbl.isExternal()) {
        tbl.setProperty("EXTERNAL", "TRUE");
        tbl.setTableType(TableType.EXTERNAL_TABLE);
      } else {
        tbl.getParameters().remove("EXTERNAL");
      }
    }

    // reset owner and creation time
    int rc = setGenericTableAttributes(tbl);
    if (rc != 0) {
      return rc;
    }

    // create the table
    db.createTable(tbl, crtTbl.getIfNotExists());
    work.getOutputs().add(new WriteEntity(tbl));
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
      if (!oldview.getTableType().equals(TableType.VIRTUAL_VIEW)) {
        throw new HiveException("Existing table is not a view");
      }

      if (crtView.getPartCols() == null
          || crtView.getPartCols().isEmpty()
          || !crtView.getPartCols().equals(oldview.getPartCols())) {
        // if we are changing partition columns, check that partitions don't exist
        if (!oldview.getPartCols().isEmpty() &&
            !db.getPartitions(oldview).isEmpty()) {
          throw new HiveException(
              "Cannot add or drop partition columns with CREATE OR REPLACE VIEW if partitions currently exist");
        }
      }

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
      work.getOutputs().add(new WriteEntity(oldview));
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

      int rc = setGenericTableAttributes(tbl);
      if (rc != 0) {
        return rc;
      }

      db.createTable(tbl, crtView.getIfNotExists());
      work.getOutputs().add(new WriteEntity(tbl));
    }
    return 0;
  }

  private int setGenericTableAttributes(Table tbl) {
    try {
      tbl.setOwner(conf.getUser());
    } catch (IOException e) {
      console.printError("Unable to get current user: " + e.getMessage(),
          stringifyException(e));
      return 1;
    }
    // set create time
    tbl.setCreateTime((int) (System.currentTimeMillis() / 1000));
    return 0;
  }

  @Override
  public StageType getType() {
    return StageType.DDL;
  }

  @Override
  public String getName() {
    return "DDL";
  }

  @Override
  protected void localizeMRTmpFilesImpl(Context ctx) {
    // no-op
  }
}
