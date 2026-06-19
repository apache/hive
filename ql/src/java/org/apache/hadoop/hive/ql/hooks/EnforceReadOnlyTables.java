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

package org.apache.hadoop.hive.ql.hooks;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Implementation of a pre execute hook that prevents modifications
 * of read-only tables used by the test framework
 */
public class EnforceReadOnlyTables implements ExecuteWithHookContext {

  @Override
  public void run(HookContext hookContext) throws Exception {
    SessionState ss = SessionState.get();
    Set<ReadEntity> inputs = hookContext.getInputs();
    Set<WriteEntity> outputs = hookContext.getOutputs();
    UserGroupInformation ugi = hookContext.getUgi();
    boolean isExplain = hookContext.getQueryPlan().isExplain();
    this.run(ss,inputs,outputs,ugi, isExplain);
  }

  public void run(SessionState sess, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs, UserGroupInformation ugi, boolean isExplain)
    throws Exception {

    // Don't enforce during test driver setup or shutdown.
    if (sess.getConf().getBoolean("hive.test.init.phase", false) ||
        sess.getConf().getBoolean("hive.test.shutdown.phase", false)) {
      return;
    }
    List<String> readOnlyTables = Arrays.asList(System.getProperty("test.src.tables").split(","));

    for (WriteEntity w: outputs) {
      if ((w.getTyp() == WriteEntity.Type.TABLE) ||
          (w.getTyp() == WriteEntity.Type.PARTITION)) {
        Table t = w.getTable();
        if (DEFAULT_DATABASE_NAME.equalsIgnoreCase(t.getDbName())
            && readOnlyTables.contains(t.getTableName()) && !isExplain) {
          throw new RuntimeException ("Cannot overwrite read-only table: " + t.getTableName());
        }
      }
    }
  }
}
