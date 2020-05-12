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
package org.apache.hadoop.hive.ql.exec.repl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.Explain;

import java.io.Serializable;

/**
 * RangerDumpWork.
 *
 * Export Ranger authorization policies.
 **/
@Explain(displayName = "Ranger Dump Operator", explainLevels = { Explain.Level.USER,
    Explain.Level.DEFAULT,
    Explain.Level.EXTENDED })
public class RangerDumpWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private Path currentDumpPath;
  private String dbName;

  public RangerDumpWork(Path currentDumpPath, String dbName) {
    this.currentDumpPath = currentDumpPath;
    this.dbName = dbName;
  }

  public Path getCurrentDumpPath() {
    return currentDumpPath;
  }

  public String getDbName() {
    return dbName;
  }
}
