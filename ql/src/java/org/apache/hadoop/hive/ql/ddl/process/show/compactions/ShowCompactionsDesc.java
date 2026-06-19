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
package org.apache.hadoop.hive.ql.ddl.process.show.compactions;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import java.io.Serializable;
import java.util.Map;

/**
 * DDL task description for SHOW COMPACTIONS commands.
 */
@Explain(displayName = "Show Compactions", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
public class ShowCompactionsDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  // @formatter:off
  public static final String SCHEMA =
      "compactionid,dbname,tabname,partname,type,state,workerhost,workerid,enqueuetime,starttime,duration,hadoopjobid,errormessage,initiatorhost,initiatorid,poolname,txnid,nexttxnid,committime,hightestwriteid#" +
      "string:string:string:string:string:string:string:string:string:string:string:string:string:string:string:string:string:string:string:string";
  // @formatter:on

    private String resFile;
    private long compactionId;
    private final String poolName;
    private final String dbName;
    private final String tbName;
    private final String compactionType;
    private final String compactionStatus;
    private final Map<String, String> partSpec;
    private final short limit;
    private final String orderBy;


    public ShowCompactionsDesc(Path resFile, long compactionId, String dbName, String tbName, String poolName, String compactionType,
                               String compactionStatus, Map<String, String> partSpec, short limit, String orderBy) {
        this.resFile = resFile.toString();
        this.compactionId = compactionId;
        this.poolName = poolName;
        this.dbName = dbName;
        this.tbName = tbName;
        this.compactionType = compactionType;
        this.compactionStatus = compactionStatus;
        this.partSpec = partSpec;
        this.limit = limit;
        this.orderBy = orderBy;
    }

    public String getResFile() {
        return resFile;
    }
    @Explain(displayName = "compactionId", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
    public long getCompactionId() {
        return compactionId;
    }
    @Explain(displayName = "poolName", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
    public String getPoolName() {
        return poolName;
    }
    @Explain(displayName = "dbName", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
    public String getDbName() {
        return dbName;
    }
    @Explain(displayName = "tbName", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
    public String getTbName() {
        return tbName;
    }

    @Explain(displayName = "compactionType", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
    public String getCompactionType() {
        return compactionType;
    }

    @Explain(displayName = "compactionStatus", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
    public String getCompactionStatus() {
        return compactionStatus;
    }

    @Explain(displayName = "partSpec", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
    public Map<String, String> getPartSpec() {
        return partSpec;
    }

    @Explain(displayName = "limit", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
    public short getLimit() {
        return limit;
    }

    @Explain(displayName = "orderBy", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
    public String getOrderBy() {
        return orderBy;
    }


}
