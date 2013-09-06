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
package org.apache.hive.hcatalog.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.common.HCatConstants;

public class HCatDriver extends Driver {

  @Override
  public CommandProcessorResponse run(String command) {

    CommandProcessorResponse cpr = null;
    try {
      cpr = super.run(command);
    } catch (CommandNeedRetryException e) {
      return new CommandProcessorResponse(-1, e.toString(), "");
    }

    SessionState ss = SessionState.get();

    if (cpr.getResponseCode() == 0) {
      // Only attempt to do this, if cmd was successful.
      int rc = setFSPermsNGrp(ss);
      cpr = new CommandProcessorResponse(rc);
    }
    // reset conf vars
    ss.getConf().set(HCatConstants.HCAT_CREATE_DB_NAME, "");
    ss.getConf().set(HCatConstants.HCAT_CREATE_TBL_NAME, "");

    return cpr;
  }

  private int setFSPermsNGrp(SessionState ss) {

    Configuration conf = ss.getConf();

    String tblName = conf.get(HCatConstants.HCAT_CREATE_TBL_NAME, "");
    if (tblName.isEmpty()) {
      tblName = conf.get("import.destination.table", "");
      conf.set("import.destination.table", "");
    }
    String dbName = conf.get(HCatConstants.HCAT_CREATE_DB_NAME, "");
    String grp = conf.get(HCatConstants.HCAT_GROUP, null);
    String permsStr = conf.get(HCatConstants.HCAT_PERMS, null);

    if (tblName.isEmpty() && dbName.isEmpty()) {
      // it wasn't create db/table
      return 0;
    }

    if (null == grp && null == permsStr) {
      // there were no grp and perms to begin with.
      return 0;
    }

    FsPermission perms = FsPermission.valueOf(permsStr);

    if (!tblName.isEmpty()) {
      Hive db = null;
      try {
        db = Hive.get();
        Table tbl = db.getTable(tblName);
        Path tblPath = tbl.getPath();

        FileSystem fs = tblPath.getFileSystem(conf);
        if (null != perms) {
          fs.setPermission(tblPath, perms);
        }
        if (null != grp) {
          fs.setOwner(tblPath, null, grp);
        }
        return 0;

      } catch (Exception e) {
        ss.err.println(String.format("Failed to set permissions/groups on TABLE: <%s> %s", tblName, e.getMessage()));
        try {  // We need to drop the table.
          if (null != db) {
            db.dropTable(tblName);
          }
        } catch (HiveException he) {
          ss.err.println(String.format("Failed to drop TABLE <%s> after failing to set permissions/groups on it. %s", tblName, e.getMessage()));
        }
        return 1;
      }
    } else {
      // looks like a db operation
      if (dbName.isEmpty() || dbName.equals(MetaStoreUtils.DEFAULT_DATABASE_NAME)) {
        // We dont set perms or groups for default dir.
        return 0;
      } else {
        try {
          Hive db = Hive.get();
          Path dbPath = new Warehouse(conf).getDatabasePath(db.getDatabase(dbName));
          FileSystem fs = dbPath.getFileSystem(conf);
          if (perms != null) {
            fs.setPermission(dbPath, perms);
          }
          if (null != grp) {
            fs.setOwner(dbPath, null, grp);
          }
          return 0;
        } catch (Exception e) {
          ss.err.println(String.format("Failed to set permissions and/or group on DB: <%s> %s", dbName, e.getMessage()));
          try {
            Hive.get().dropDatabase(dbName);
          } catch (Exception e1) {
            ss.err.println(String.format("Failed to drop DB <%s> after failing to set permissions/group on it. %s", dbName, e1.getMessage()));
          }
          return 1;
        }
      }
    }
  }
}
