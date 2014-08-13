/**
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

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Implementation of a pre execute hook that prevents modifications
 * of read-only tables used by the test framework
 */
public class EnforceReadOnlyTables implements ExecuteWithHookContext {

  private static final Set<String> READ_ONLY_TABLES = new HashSet<String>();

  static {
    for (String srcTable : System.getProperty("test.src.tables", "").trim().split(",")) {
      srcTable = srcTable.trim();
      if (!srcTable.isEmpty()) {
        READ_ONLY_TABLES.add(srcTable);
      }
    }
    if (READ_ONLY_TABLES.isEmpty()) {
      throw new AssertionError("Source tables cannot be empty");
    }
  }

  @Override
  public void run(HookContext hookContext) throws Exception {
    SessionState ss = SessionState.get();
    Set<ReadEntity> inputs = hookContext.getInputs();
    Set<WriteEntity> outputs = hookContext.getOutputs();
    UserGroupInformation ugi = hookContext.getUgi();
    this.run(ss,inputs,outputs,ugi);
  }

  public void run(SessionState sess, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs, UserGroupInformation ugi)
    throws Exception {

    // Don't enforce during test driver setup or shutdown.
    if (sess.getConf().getBoolean("hive.test.init.phase", false) ||
        sess.getConf().getBoolean("hive.test.shutdown.phase", false)) {
      return;
    }
    for (WriteEntity w: outputs) {
      if ((w.getTyp() == WriteEntity.Type.TABLE) ||
          (w.getTyp() == WriteEntity.Type.PARTITION)) {
        Table t = w.getTable();
        if (DEFAULT_DATABASE_NAME.equalsIgnoreCase(t.getDbName())
            && READ_ONLY_TABLES.contains(t.getTableName())) {
          throw new RuntimeException ("Cannot overwrite read-only table: " + t.getTableName());
        }
      }
    }
  }
}
