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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.ReplState;

public class BootstrapDumpEnd extends ReplState {
  @JsonProperty
  private String dbName;

  @JsonProperty
  private DumpType dumpType;

  @JsonProperty
  private Long actualNumTables;

  @JsonProperty
  private Long actualNumFunctions;

  @JsonProperty
  @JsonSerialize(using = ReplUtils.TimeSerializer.class)
  private Long dumpEndTime;

  @JsonProperty
  private String dumpDir;

  @JsonProperty
  private String lastReplId;

  public BootstrapDumpEnd(String dbName,
                          long actualNumTables,
                          long actualNumFunctions,
                          String dumpDir,
                          String lastReplId) {
    this.dbName = dbName;
    this.dumpType = DumpType.BOOTSTRAP;
    this.actualNumTables = actualNumTables;
    this.actualNumFunctions = actualNumFunctions;
    this.dumpEndTime = System.currentTimeMillis() / 1000;
    this.dumpDir = dumpDir;
    this.lastReplId = lastReplId;
  }
}
