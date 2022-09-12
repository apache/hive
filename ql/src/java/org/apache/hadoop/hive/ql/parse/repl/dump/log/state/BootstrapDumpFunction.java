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
import org.apache.hadoop.hive.ql.parse.repl.ReplState;

public class BootstrapDumpFunction extends ReplState {
  @JsonProperty
  private String dbName;

  @JsonProperty
  private String functionName;

  @JsonProperty
  private String functionsDumpProgress;

  @JsonProperty
  @JsonSerialize(using = ReplUtils.TimeSerializer.class)
  private Long dumpTime;

  public BootstrapDumpFunction(String dbName, String funcName,
                               long functionSeqNo, long estimatedNumFunctions) {
    this.dbName = dbName;
    this.functionName = funcName;
    this.functionsDumpProgress = new String(new StringBuilder()
                                  .append(functionSeqNo).append("/").append(estimatedNumFunctions));
    this.dumpTime = System.currentTimeMillis() / 1000;
  }
}
