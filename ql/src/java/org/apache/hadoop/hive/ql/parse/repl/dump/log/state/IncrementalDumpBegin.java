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

public class IncrementalDumpBegin extends ReplState {
  @JsonProperty
  private String dbName;

  @JsonProperty
  private DumpType dumpType;

  @JsonProperty
  private Long estimatedNumEvents;

  @JsonProperty
  @JsonSerialize(using = ReplUtils.TimeSerializer.class)
  private Long dumpStartTime;

  @JsonProperty
  private Long fromEventId;

  @JsonProperty
  private Long toEventId;

  @JsonProperty
  private Long maxEvents;

  public IncrementalDumpBegin(String dbName, long estimatedNumEvents, Long fromEventId,
                              Long toEventId, Long maxEvents) {
    this.dbName = dbName;
    this.dumpType = DumpType.INCREMENTAL;
    this.estimatedNumEvents = estimatedNumEvents;
    this.dumpStartTime = System.currentTimeMillis() / 1000;
    this.fromEventId = fromEventId;
    this.toEventId = toEventId;
    this.maxEvents = maxEvents;
  }
}
