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
package org.apache.hadoop.hive.ql.parse.repl.load.log.state;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.apache.hadoop.hive.ql.exec.repl.ReplStatsTracker;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.repl.ReplState;

public class IncrementalLoadEvent extends ReplState {
  @JsonProperty
  private String dbName;

  @JsonProperty
  private String eventId;

  @JsonProperty
  private String eventType;

  @JsonProperty
  private String eventsLoadProgress;

  @JsonProperty
  @JsonSerialize(using = ReplUtils.TimeSerializer.class)
  private Long loadTime;

  @JsonProperty
  private String eventDuration;

  private long loadTimeMillis;

  public IncrementalLoadEvent(String dbName, String eventId, String eventType, long eventSeqNo, long numEvents,
      long previousTimestamp, ReplStatsTracker replStatsTracker) {
    this.dbName = dbName;
    this.eventId = eventId;
    this.eventType = eventType;
    this.eventsLoadProgress = new String(new StringBuilder()
                                            .append(eventSeqNo).append("/").append(numEvents));
    this.loadTimeMillis = System.currentTimeMillis();
    this.loadTime = loadTimeMillis / 1000;
    this.eventDuration = (this.loadTimeMillis - previousTimestamp) + " ms";
    replStatsTracker.addEntry(eventType,eventId,(this.loadTimeMillis - previousTimestamp));
  }

  public long getLoadTimeMillis() {
    return this.loadTimeMillis;
  }
}
