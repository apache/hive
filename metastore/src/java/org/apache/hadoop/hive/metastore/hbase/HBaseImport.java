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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * A tool to take the contents of an RDBMS based Hive metastore and import it into an HBase based
 * one.  To use this the config files for Hive configured to work with the RDBMS (that is,
 * including the JDBC string, etc.) and for HBase must be in the path.  This tool will then
 * handle connecting to the RDBMS via the {@link org.apache.hadoop.hive.metastore.ObjectStore}
 * and HBase via {@link org.apache.hadoop.hive.metastore.hbase.HBaseStore} and transferring the
 * data.
 */
public class HBaseImport {

  static final private Log LOG = LogFactory.getLog(HBaseImport.class.getName());

  public static void main(String[] args) {
    HBaseImport tool = new HBaseImport();
    try {
      tool.run();
    } catch (Exception e) {
      System.err.println("Caught exception " + e.getClass().getName() + " with message <" +
          e.getMessage() + ">");
    }
  }

  private Configuration rdbmsConf;
  private Configuration hbaseConf;
  private RawStore rdbmsStore;
  private RawStore hbaseStore;
  private List<Database> dbs;
  private List<Table> tables;

  @VisibleForTesting
  HBaseImport() {
    dbs = new ArrayList<Database>();
    tables = new ArrayList<Table>();

  }

  @VisibleForTesting
  void run() throws MetaException, InstantiationException, IllegalAccessException,
      NoSuchObjectException, InvalidObjectException {
    // Order here is crucial, as you can't add tables until you've added databases, etc.
    init();
    copyRoles();
    copyDbs();
    copyTables();
    copyPartitions();
    copyFunctions();
  }

  private void init() throws MetaException, IllegalAccessException, InstantiationException {
    if (rdbmsStore != null) {
      // We've been configured for testing, so don't do anything here.
      return;
    }
    rdbmsConf = new HiveConf();  // We're depending on having everything properly in the path
    hbaseConf = new HiveConf();
    HiveConf.setVar(hbaseConf, HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL,
        HBaseStore.class.getName());
    HiveConf.setBoolVar(hbaseConf, HiveConf.ConfVars.METASTORE_FASTPATH, true);

    // First get a connection to the RDBMS based store
    rdbmsStore = new ObjectStore();
    rdbmsStore.setConf(rdbmsConf);

    // Get a connection to the HBase based store
    hbaseStore = new HBaseStore();
    hbaseStore.setConf(hbaseConf);
    // This will go create the tables if they don't exist
    hbaseStore.verifySchema();
  }

  private void copyRoles() throws NoSuchObjectException, InvalidObjectException, MetaException {
    screen("Copying roles");
    for (String roleName : rdbmsStore.listRoleNames()) {
      Role role = rdbmsStore.getRole(roleName);
      screen("Copying role " + roleName);
      hbaseStore.addRole(roleName, role.getOwnerName());
    }
  }

  private void copyDbs() throws MetaException, NoSuchObjectException, InvalidObjectException {
    screen("Copying databases");
    for (String dbName : rdbmsStore.getAllDatabases()) {
      Database db = rdbmsStore.getDatabase(dbName);
      dbs.add(db);
      screen("Copying database " + dbName);
      hbaseStore.createDatabase(db);
    }
  }

  private void copyTables() throws MetaException, InvalidObjectException {
    screen("Copying tables");
    for (Database db : dbs) {
      screen("Coyping tables in database " + db.getName());
      for (String tableName : rdbmsStore.getAllTables(db.getName())) {
        Table table = rdbmsStore.getTable(db.getName(), tableName);
        tables.add(table);
        screen("Copying table " + db.getName() + "." + tableName);
        hbaseStore.createTable(table);
      }
    }
  }

  private void copyPartitions() throws MetaException, NoSuchObjectException,
      InvalidObjectException {
    screen("Copying partitions");
    for (Table table : tables) {
      System.out.print("Copying partitions for table " + table.getDbName() + "." +
          table.getTableName());
      for (Partition part : rdbmsStore.getPartitions(table.getDbName(), table.getTableName(), -1)) {
        LOG.info("Copying " + table.getTableName() + "." + table.getTableName() + "." +
            StringUtils.join(part.getValues(), ':'));
        System.out.print('.');
        hbaseStore.addPartition(part);
      }
      System.out.println();
    }
  }

  private void copyFunctions() throws MetaException, NoSuchObjectException, InvalidObjectException {
    screen("Copying functions");
    for (Database db : dbs) {
      screen("Copying functions in database " + db.getName());
      for (String funcName : rdbmsStore.getFunctions(db.getName(), "*")) {
        Function func = rdbmsStore.getFunction(db.getName(), funcName);
        screen("Copying function " + db.getName() + "." + funcName);
        hbaseStore.createFunction(func);
      }
    }
  }

  private void screen(String msg) {
    LOG.info(msg);
    System.out.println(msg);
  }

  @VisibleForTesting
  HBaseImport setConnections(RawStore rdbms, RawStore hbase) {
    rdbmsStore = rdbms;
    hbaseStore = hbase;

    return new HBaseImport();
  }

}
