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

package org.apache.hadoop.hive.ql.lockmgr;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

public class HiveLockObject {
  String[] pathNames = null;

  public static class HiveLockObjectData {

    private String queryId; // queryId of the command
    private String lockTime; // time at which lock was acquired
    // mode of the lock: EXPLICIT(lock command)/IMPLICIT(query)
    private String lockMode;
    private String queryStr;
    private String clientIp;

    /**
     * Constructor
     *
     * Note: The parameters are used to uniquely identify a HiveLockObject. 
     * The parameters will be stripped off any ':' characters in order not 
     * to interfere with the way the data is serialized (':' delimited string).
     * The query string might be truncated depending on HIVE_LOCK_QUERY_STRING_MAX_LENGTH
     * @param queryId The query identifier will be added to the object without change
     * @param lockTime The lock time  will be added to the object without change
     * @param lockMode The lock mode  will be added to the object without change
     * @param queryStr The query string might be truncated based on
     *     HIVE_LOCK_QUERY_STRING_MAX_LENGTH conf variable
     * @param conf The hive configuration based on which we decide if we should truncate the query
     *     string or not
     */
    public HiveLockObjectData(String queryId, String lockTime, String lockMode, String queryStr,
        HiveConf conf) {
      this.queryId = removeDelimiter(queryId);
      this.lockTime = StringInternUtils.internIfNotNull(removeDelimiter(lockTime));
      this.lockMode = removeDelimiter(lockMode);
      this.queryStr = StringInternUtils.internIfNotNull(
          queryStr == null ? null : StringUtils.abbreviate(removeDelimiter(queryStr.trim()),
              conf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_QUERY_STRING_MAX_LENGTH)));
    }

    /**
     * Constructor
     * 
     * @param data String of the form "queryId:lockTime:lockMode:queryStr". 
     * No ':' characters are allowed in any of the components.
     */
    public HiveLockObjectData(String data) {
      if (data == null) {
        return;
      }

      String[] elem = data.split(":");
      queryId = elem[0];
      lockTime = StringInternUtils.internIfNotNull(elem[1]);
      lockMode = elem[2];
      queryStr = StringInternUtils.internIfNotNull(elem[3]);
      if (elem.length >= 5) {
        clientIp = elem[4];
      }
    }

    public String getQueryId() {
      return queryId;
    }

    public String getLockTime() {
      return lockTime;
    }

    public String getLockMode() {
      return lockMode;
    }

    public String getQueryStr() {
      return queryStr;
    }

    @Override
    public String toString() {
      return queryId + ":" + lockTime + ":" + lockMode + ":" + queryStr + ":"
          + clientIp;
    }

    public String getClientIp() {
      return this.clientIp;
    }

    public void setClientIp(String clientIp) {
      this.clientIp = clientIp;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof HiveLockObjectData)) {
        return false;
      }

      HiveLockObjectData target = (HiveLockObjectData) o;
      boolean ret = (queryId == null ? target.queryId == null :
          target.queryId != null && queryId.equals(target.queryId));
      ret = ret && (lockTime == null ? target.lockTime == null :
          target.lockTime != null && lockTime.equals(target.lockTime));
      ret = ret && (lockMode == null ? target.lockMode == null :
          target.lockMode != null && lockMode.equals(target.lockMode));
      ret = ret && (queryStr == null ? target.queryStr == null :
          target.queryStr != null && queryStr.equals(target.queryStr));
      ret = ret && (clientIp == null ? target.clientIp == null :
          target.clientIp != null && clientIp.equals(target.clientIp));

      return ret;
    }
 
    @Override
    public int hashCode() {
      HashCodeBuilder builder = new HashCodeBuilder();

      boolean queryId_present = queryId == null;
      builder.append(queryId_present);
      if (queryId_present) {
        builder.append(queryId);
      }

      boolean lockTime_present = lockTime == null;
      builder.append(lockTime);
      if (lockTime_present) {
        builder.append(lockTime);
      }

      boolean lockMode_present = lockMode == null;
      builder.append(lockMode);
      if (lockMode_present) {
        builder.append(lockMode);
      }

      boolean queryStr_present = queryStr == null;
      builder.append(queryStr);
      if (queryStr_present) {
        builder.append(queryStr);
      }

      boolean clienIp_present = clientIp == null;
      builder.append(clientIp);
      if (clienIp_present) {
        builder.append(clientIp);
      }

      return builder.toHashCode();
    }

  }

  /* user supplied data for that object */
  private HiveLockObjectData data;

  public HiveLockObject() {
    this.data = null;
  }

  public HiveLockObject(String path, HiveLockObjectData lockData) {
    this.pathNames = new String[1];
    this.pathNames[0] = StringInternUtils.internIfNotNull(path);
    this.data = lockData;
  }

  public HiveLockObject(String[] paths, HiveLockObjectData lockData) {
    this.pathNames = StringInternUtils.internStringsInArray(paths);
    this.data = lockData;
  }

  public HiveLockObject(Table tbl, HiveLockObjectData lockData) {
    this(new String[] {tbl.getDbName(), org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.encodeTableName(tbl.getTableName())}, lockData);
  }

  public HiveLockObject(Partition par, HiveLockObjectData lockData) {
    this(new String[] {par.getTable().getDbName(),
                       org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.encodeTableName(par.getTable().getTableName()), par.getName()}, lockData);
  }

  public HiveLockObject(DummyPartition par, HiveLockObjectData lockData) {
    this(new String[] {par.getName()}, lockData);
  }

  /**
   * Creates a locking object for a table (when partition spec is not provided)
   * or a table partition
   * @param hiveDB    an object to communicate with the metastore
   * @param tableName the table to create the locking object on
   * @param partSpec  the spec of a partition to create the locking object on
   * @return  the locking object
   * @throws HiveException
   */
  public static HiveLockObject createFrom(Hive hiveDB, String tableName,
      Map<String, String> partSpec) throws HiveException {
    Table  tbl = hiveDB.getTable(tableName);
    if (tbl == null) {
      throw new HiveException("Table " + tableName + " does not exist ");
    }

    HiveLockObject obj = null;

    if  (partSpec == null) {
      obj = new HiveLockObject(tbl, null);
    }
    else {
      Partition par = hiveDB.getPartition(tbl, partSpec, false);
      if (par == null) {
        throw new HiveException("Partition " + partSpec + " for table " +
            tableName + " does not exist");
      }
      obj = new HiveLockObject(par, null);
    }
    return obj;
  }

  public String[] getPaths() {
    return pathNames;
  }

  public String getName() {
    if (pathNames == null) {
      return null;
    }
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < pathNames.length; i++) {
      if (i > 0) {
        builder.append('/');
      }
      builder.append(pathNames[i]);
    }
    return builder.toString();
  }

  public String getDisplayName() {
    if (this.pathNames == null) {
      return null;
    }
    if (pathNames.length == 1) {
      return pathNames[0];
    }
    else if (pathNames.length == 2) {
      return pathNames[0] + "@" + pathNames[1];
    }

    String ret = pathNames[0] + "@" + pathNames[1] + "@";
    boolean first = true;
    for (int i = 2; i < pathNames.length; i++) {
      if (!first) {
        ret = ret + "/";
      } else {
        first = false;
      }
      ret = ret + pathNames[i];
    }
    return ret;
  }

  public HiveLockObjectData getData() {
    return data;
  }

  public void setData(HiveLockObjectData data) {
    this.data = data;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof HiveLockObject)) {
      return false;
    }

    HiveLockObject tgt = (HiveLockObject) o;
    return StringUtils.equals(this.getName(), tgt.getName()) &&
        (data == null ? tgt.getData() == null : data.equals(tgt.getData()));
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();
    builder.append(pathNames);
    boolean data_present = data == null;
    builder.append(data_present);
    if (data_present) {
      builder.append(data);
    }
    return builder.toHashCode();
  }

  private static String removeDelimiter(String in) {
    if (in == null) {
      return null;
    }
    return in.replaceAll(":","");
  }
}
