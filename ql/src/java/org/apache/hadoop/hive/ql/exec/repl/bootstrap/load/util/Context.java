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
package org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.LineageState;

public class Context {
  public final HiveConf hiveConf;
  public final Hive hiveDb;
  public final Warehouse warehouse;
  public final PathInfo pathInfo;

  /*
  these are sessionState objects that are copied over to work to allow for parallel execution.
  based on the current use case the methods are selectively synchronized, which might need to be
  taken care when using other methods.
 */
  public final LineageState sessionStateLineageState;
  public final long currentTransactionId;


  public Context(HiveConf hiveConf, Hive hiveDb,
      LineageState lineageState, long currentTransactionId) throws MetaException {
    this.hiveConf = hiveConf;
    this.hiveDb = hiveDb;
    this.warehouse = new Warehouse(hiveConf);
    this.pathInfo = new PathInfo(hiveConf);
    sessionStateLineageState = lineageState;
    this.currentTransactionId = currentTransactionId;
  }
}
