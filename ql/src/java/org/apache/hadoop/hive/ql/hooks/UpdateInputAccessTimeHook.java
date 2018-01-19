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

import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Implementation of a pre execute hook that updates the access
 * times for all the inputs.
 */
public class UpdateInputAccessTimeHook {

  private static final String LAST_ACCESS_TIME = "lastAccessTime";

  public static class PreExec implements ExecuteWithHookContext {

    @Override
    public void run(HookContext hookContext) throws Exception {
      HiveConf conf = hookContext.getConf();
      Set<ReadEntity> inputs = hookContext.getQueryPlan().getInputs();

      Hive db;
      try {
        db = Hive.get(conf);
      } catch (HiveException e) {
        // ignore
        db = null;
        return;
      }

      int lastAccessTime = (int) (System.currentTimeMillis()/1000);

      for(ReadEntity re: inputs) {
        // Set the last query time
        ReadEntity.Type typ = re.getType();
        switch(typ) {
        // It is possible that read and write entities contain a old version
        // of the object, before it was modified by StatsTask.
        // Get the latest versions of the object
        case TABLE: {
          Table t = db.getTable(re.getTable().getTableName());
          t.setLastAccessTime(lastAccessTime);
          db.alterTable(t, null);
          break;
        }
        case PARTITION: {
          Partition p = re.getPartition();
          Table t = db.getTable(p.getTable().getTableName());
          p = db.getPartition(t, p.getSpec(), false);
          p.setLastAccessTime(lastAccessTime);
          db.alterPartition(t.getTableName(), p, null);
          t.setLastAccessTime(lastAccessTime);
          db.alterTable(t, null);
          break;
        }
        default:
          // ignore dummy inputs
          break;
        }
      }
    }
  }
}
