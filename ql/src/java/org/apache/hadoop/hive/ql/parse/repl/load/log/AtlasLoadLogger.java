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
package org.apache.hadoop.hive.ql.parse.repl.load.log;

import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.ReplState.LogTag;
import org.apache.hadoop.hive.ql.parse.repl.load.log.state.AtlasLoadBegin;
import org.apache.hadoop.hive.ql.parse.repl.load.log.state.AtlasLoadEnd;

/**
 * Repl logger for Atlas metadata load task.
 **/
public class AtlasLoadLogger extends ReplLogger<Integer> {
  private String sourceDbName;
  private String targetDbName;
  private String dumpDir;

  public AtlasLoadLogger(String sourceDbName, String targetDbName, String dumpDir) {
    this.sourceDbName = sourceDbName;
    this.targetDbName = targetDbName;
    this.dumpDir = dumpDir;
  }

  @Override
  public void startLog() {
    new AtlasLoadBegin(sourceDbName, targetDbName).log(LogTag.ATLAS_LOAD_START);
  }

  @Override
  public void endLog(Integer count) {
    new AtlasLoadEnd(sourceDbName, targetDbName, count, dumpDir).log(LogTag.ATLAS_LOAD_END);
  }
}
