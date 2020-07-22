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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hive.ql.parse.repl.ReplState;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * RangerLoadEnd.
 *
 * ReplState to define Ranger Load End.
 **/
public class RangerLoadEnd extends ReplState {
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  @JsonProperty
  private String sourceDbName;

  @SuppressFBWarnings("URF_UNREAD_FIELD")
  @JsonProperty
  private String targetDbName;

  @SuppressFBWarnings("URF_UNREAD_FIELD")
  @JsonProperty
  private Long actualNumPolicies;

  @SuppressFBWarnings("URF_UNREAD_FIELD")
  @JsonProperty
  private Long loadEndTime;

  @SuppressFBWarnings("URF_UNREAD_FIELD")
  @JsonProperty
  private String dumpDir;

  public RangerLoadEnd(String sourceDbName,
                       String targetDbName,
                       long actualNumPolicies,
                       String dumpDir) {
    this.sourceDbName = sourceDbName;
    this.targetDbName = targetDbName;
    this.actualNumPolicies = actualNumPolicies;
    this.loadEndTime = System.currentTimeMillis() / 1000;
    this.dumpDir = dumpDir;
  }
}
