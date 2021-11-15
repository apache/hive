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
package org.apache.hadoop.hive.ql.ddl.process.show.transactions;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.io.Serializable;

/**
 * DDL task description for SHOW TRANSACTIONS commands.
 */
@Explain(displayName = "Show Transactions", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowTransactionsDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  public static final String SCHEMA =
      "txnid,state,startedtime,lastheartbeattime,user,host#string:string:string:string:string:string";

  private final String resFile;

  public ShowTransactionsDesc(Path resFile) {
    this.resFile = resFile.toString();
  }

  public String getResFile() {
    return resFile;
  }
}
