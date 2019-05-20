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
package org.apache.hadoop.hive.ql.parse.repl.dump.log.state;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.parse.repl.ReplState;
import org.codehaus.jackson.annotate.JsonProperty;

public class BootstrapDumpTable extends ReplState {
  @JsonProperty
  private String dbName;

  @JsonProperty
  private String tableName;

  @JsonProperty
  private String tableType;

  @JsonProperty
  private String tablesDumpProgress;

  @JsonProperty
  private Long dumpTime;

  public BootstrapDumpTable(String dbName,
                            String tableName,
                            TableType tableType,
                            long tableSeqNo,
                            long estimatedNumTables) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.tableType = tableType.name();
    this.tablesDumpProgress = new String(new StringBuilder()
                                        .append(tableSeqNo).append("/").append(estimatedNumTables));
    this.dumpTime = System.currentTimeMillis() / 1000;
  }
}
