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
package org.apache.hadoop.hive.ql.security.authorization;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePolicyProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveResourceACLs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PrivilegeSynchonizer defines a thread to synchronize privileges from
 * external authorizer to Hive metastore.
 */
public class PrivilegeSynchonizer implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(PrivilegeSynchonizer.class);
  public static final String GRANTOR = "ranger";
  private IMetaStoreClient hiveClient;
  private LeaderLatch privilegeSynchonizerLatch;
  private HiveConf hiveConf;
  private PolicyProviderContainer policyProviderContainer;

  public PrivilegeSynchonizer(LeaderLatch privilegeSynchonizerLatch,
      PolicyProviderContainer policyProviderContainer, HiveConf hiveConf) {
    this.hiveConf = new HiveConf(hiveConf);
    this.hiveConf.set(MetastoreConf.ConfVars.FILTER_HOOK.getVarname(), DefaultMetaStoreFilterHookImpl.class.getName());
    try {
      hiveClient = Hive.get(this.hiveConf).getMSC();
    } catch (Exception e) {
      throw new RuntimeException("Error creating HiveMetastoreClient", e);
    }
    this.privilegeSynchonizerLatch = privilegeSynchonizerLatch;
    this.policyProviderContainer = policyProviderContainer;
    this.hiveConf = hiveConf;
  }

  private void addACLsToBag(
      Map<String, Map<HiveResourceACLs.Privilege, HiveResourceACLs.AccessResult>> principalAclsMap,
      PrivilegeBag privBag, HiveObjectType objectType, String dbName, String tblName, String columnName,
      PrincipalType principalType, String authorizer) {

    for (Map.Entry<String, Map<HiveResourceACLs.Privilege, HiveResourceACLs.AccessResult>> principalAcls
        : principalAclsMap.entrySet()) {
      String principal = principalAcls.getKey();
      for (Map.Entry<HiveResourceACLs.Privilege, HiveResourceACLs.AccessResult> acl : principalAcls.getValue()
          .entrySet()) {
        if (acl.getValue() == HiveResourceACLs.AccessResult.ALLOWED) {
          switch (objectType) {
          case DATABASE:
            privBag.addToPrivileges(
                new HiveObjectPrivilege(new HiveObjectRef(HiveObjectType.DATABASE, dbName, null, null, null), principal,
                    principalType, new PrivilegeGrantInfo(acl.getKey().toString(),
                        (int) (System.currentTimeMillis() / 1000), GRANTOR, PrincipalType.USER, false), authorizer));
            break;
          case TABLE:
            privBag.addToPrivileges(
                new HiveObjectPrivilege(new HiveObjectRef(HiveObjectType.TABLE, dbName, tblName, null, null), principal,
                    principalType, new PrivilegeGrantInfo(acl.getKey().toString(),
                        (int) (System.currentTimeMillis() / 1000), GRANTOR, PrincipalType.USER, false), authorizer));
            break;
          case COLUMN:
            privBag.addToPrivileges(
                new HiveObjectPrivilege(new HiveObjectRef(HiveObjectType.COLUMN, dbName, tblName, null, columnName),
                    principal, principalType, new PrivilegeGrantInfo(acl.getKey().toString(),
                        (int) (System.currentTimeMillis() / 1000), GRANTOR, PrincipalType.USER, false), authorizer));
            break;
          default:
            throw new RuntimeException("Get unknown object type " + objectType);
          }
        }
      }
    }
  }

  private HiveObjectRef getObjToRefresh(HiveObjectType type, String dbName, String tblName) throws Exception {
    HiveObjectRef objToRefresh = null;
    switch (type) {
    case DATABASE:
      objToRefresh = new HiveObjectRef(HiveObjectType.DATABASE, dbName, null, null, null);
      break;
    case TABLE:
      objToRefresh = new HiveObjectRef(HiveObjectType.TABLE, dbName, tblName, null, null);
      break;
    case COLUMN:
      objToRefresh = new HiveObjectRef(HiveObjectType.COLUMN, dbName, tblName, null, null);
      break;
    default:
      throw new RuntimeException("Get unknown object type " + type);
    }
    return objToRefresh;
  }

  private void addGrantPrivilegesToBag(HivePolicyProvider policyProvider, PrivilegeBag privBag, HiveObjectType type,
      String dbName, String tblName, String columnName, String authorizer) throws Exception {

    HiveResourceACLs objectAcls = null;

    switch (type) {
    case DATABASE:
      objectAcls = policyProvider
          .getResourceACLs(new HivePrivilegeObject(HivePrivilegeObjectType.DATABASE, dbName, null));
      break;

    case TABLE:
      objectAcls = policyProvider
          .getResourceACLs(new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, dbName, tblName));
      break;

    case COLUMN:
      objectAcls = policyProvider
          .getResourceACLs(new HivePrivilegeObject(HivePrivilegeObjectType.COLUMN, dbName, tblName, null, columnName));
      break;

    default:
      throw new RuntimeException("Get unknown object type " + type);
    }

    if (objectAcls == null) {
      return;
    }

    addACLsToBag(objectAcls.getUserPermissions(), privBag, type, dbName, tblName, columnName,
        PrincipalType.USER, authorizer);
    addACLsToBag(objectAcls.getGroupPermissions(), privBag, type, dbName, tblName, columnName,
        PrincipalType.GROUP, authorizer);
  }

  @Override
  public void run() {
    while (true) {
      long interval = HiveConf.getTimeVar(hiveConf, ConfVars.HIVE_PRIVILEGE_SYNCHRONIZER_INTERVAL, TimeUnit.SECONDS);
      try {
        for (HivePolicyProvider policyProvider : policyProviderContainer) {
          LOG.info("Start synchronize privilege " + policyProvider.getClass().getName());
          String authorizer = policyProvider.getClass().getSimpleName();
          if (!privilegeSynchonizerLatch.await(interval, TimeUnit.SECONDS)) {
            LOG.info("Not selected as leader, skip");
            continue;
          }
          int numDb = 0, numTbl = 0;
          for (String dbName : hiveClient.getAllDatabases()) {
            numDb++;
            HiveObjectRef dbToRefresh = getObjToRefresh(HiveObjectType.DATABASE, dbName, null);
            PrivilegeBag grantDatabaseBag = new PrivilegeBag();
            addGrantPrivilegesToBag(policyProvider, grantDatabaseBag, HiveObjectType.DATABASE,
                dbName, null, null, authorizer);
            hiveClient.refresh_privileges(dbToRefresh, authorizer, grantDatabaseBag);
            LOG.debug("processing " + dbName);

            for (String tblName : hiveClient.getAllTables(dbName)) {
              numTbl++;
              LOG.debug("processing " + dbName + "." + tblName);
              HiveObjectRef tableToRefresh = getObjToRefresh(HiveObjectType.TABLE, dbName, tblName);
              PrivilegeBag grantTableBag = new PrivilegeBag();
              addGrantPrivilegesToBag(policyProvider, grantTableBag, HiveObjectType.TABLE,
                  dbName, tblName, null, authorizer);
              hiveClient.refresh_privileges(tableToRefresh, authorizer, grantTableBag);

              HiveObjectRef tableOfColumnsToRefresh = getObjToRefresh(HiveObjectType.COLUMN, dbName, tblName);
              PrivilegeBag grantColumnBag = new PrivilegeBag();
              Table tbl = null;
              try {
                tbl = hiveClient.getTable(dbName, tblName);
                for (FieldSchema fs : tbl.getPartitionKeys()) {
                  addGrantPrivilegesToBag(policyProvider, grantColumnBag, HiveObjectType.COLUMN,
                          dbName, tblName, fs.getName(), authorizer);
                }
                for (FieldSchema fs : tbl.getSd().getCols()) {
                  addGrantPrivilegesToBag(policyProvider, grantColumnBag, HiveObjectType.COLUMN,
                          dbName, tblName, fs.getName(), authorizer);
                }
                hiveClient.refresh_privileges(tableOfColumnsToRefresh, authorizer, grantColumnBag);
              } catch (MetaException e) {
                LOG.debug("Unable to synchronize " + tblName + ":" + e.getMessage());
              }
            }
          }
          LOG.info("Success synchronize privilege " + policyProvider.getClass().getName() + ":" + numDb + " databases, "
                  + numTbl + " tables");
        }
        // Wait if no exception happens, otherwise, retry immediately
        LOG.info("Wait for " + interval + " seconds");
        Thread.sleep(interval * 1000);
      } catch (Exception e) {
        LOG.error("Error initializing PrivilegeSynchronizer: " + e.getMessage(), e);
      }
    }
  }
}
