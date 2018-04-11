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

import java.io.Serializable;

import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

@Explain(displayName = "Import Commit", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ImportCommitWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private String dbName, tblName;
  private long writeId;
  private int stmtId;

  public ImportCommitWork(String dbName, String tblName, long writeId, int stmtId) {
    this.writeId = writeId;
    this.stmtId = stmtId;
    this.dbName = dbName;
    this.tblName = tblName;
  }

  public long getWriteId() {
    return writeId;
  }

  public int getStmtId() {
    return stmtId;
  }

  public String getDbName() {
    return dbName;
  }

  public String getTblName() {
    return tblName;
  }
}
