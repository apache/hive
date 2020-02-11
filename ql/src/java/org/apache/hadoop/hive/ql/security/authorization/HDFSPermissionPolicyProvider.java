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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePolicyChangeListener;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePolicyProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveResourceACLs;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveResourceACLsImpl;

/**
 * PolicyProvider for storage based authorizer based on hdfs permission string
 */
public class HDFSPermissionPolicyProvider implements HivePolicyProvider {

  private Configuration conf;

  public HDFSPermissionPolicyProvider(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public HiveResourceACLs getResourceACLs(HivePrivilegeObject hiveObject) {
    HiveResourceACLs acls = null;
    try {
      switch (hiveObject.getType()) {
      case DATABASE:
        Database db = Hive.get().getDatabase(hiveObject.getDbname());
        acls = getResourceACLs(new Path(db.getLocationUri()));
        break;
      case TABLE_OR_VIEW:
      case COLUMN:
        Table table = Hive.get().getTable(hiveObject.getDbname(), hiveObject.getObjectName());
        acls = getResourceACLs(new Path(table.getTTable().getSd().getLocation()));
        break;
      default:
        // Shall never happen
        throw new RuntimeException("Unknown request type:" + hiveObject.getType());
      }
    } catch (Exception e) {
    }
    return acls;
  }

  private HiveResourceACLs getResourceACLs(Path path) throws IOException {
    if (path == null) {
      throw new IllegalArgumentException("path is null");
    }

    final FileSystem fs = path.getFileSystem(conf);

    FileStatus pathStatus = FileUtils.getFileStatusOrNull(fs, path);
    if (pathStatus != null) {
      return getResourceACLs(fs, pathStatus);
    } else if (path.getParent() != null) {
      // find the ancestor which exists to check its permissions
      Path par = path.getParent();
      FileStatus parStatus = null;
      while (par != null) {
        parStatus = FileUtils.getFileStatusOrNull(fs, par);
        if (parStatus != null) {
          break;
        }
        par = par.getParent();
      }
      return getResourceACLs(fs, parStatus);
    }
    return null;
  }

  private HiveResourceACLs getResourceACLs(final FileSystem fs, final FileStatus stat) {
    String owner = stat.getOwner();
    String group = stat.getGroup();
    HiveResourceACLsImpl acls = new HiveResourceACLsImpl();
    FsPermission permission = stat.getPermission();
    if (permission.getUserAction().implies(FsAction.READ)) {
      acls.addUserEntry(owner, HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
    }
    if (permission.getGroupAction().implies(FsAction.READ)) {
      acls.addGroupEntry(group, HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
    }
    if (permission.getOtherAction().implies(FsAction.READ)) {
      acls.addGroupEntry("public", HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
    }
    return acls;
  }

  @Override
  public void registerHivePolicyChangeListener(HivePolicyChangeListener listener) {
    // Not implemented
  }

}
