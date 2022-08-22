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
import org.apache.hive.common.util.SuppressFBWarnings;

/**
 * RangerDumpEnd.
 *
 * ReplState to define Ranger Dump End.
 **/
public class RangerDumpEnd extends ReplState {
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  @JsonProperty
  private String dbName;

  @SuppressFBWarnings("URF_UNREAD_FIELD")
  @JsonProperty
  private Long actualNumPolicies;

  @SuppressFBWarnings("URF_UNREAD_FIELD")
  @JsonProperty
  @JsonSerialize(using = ReplUtils.TimeSerializer.class)
  private Long dumpEndTime;

  @SuppressFBWarnings("URF_UNREAD_FIELD")
  @JsonProperty
  private String dumpDir;

  public RangerDumpEnd(String dbName,
                       long actualNumPolicies,
                       String dumpDir) {
    this.dbName = dbName;
    this.actualNumPolicies = actualNumPolicies;
    this.dumpEndTime = System.currentTimeMillis() / 1000;
    this.dumpDir = dumpDir;
  }
}
