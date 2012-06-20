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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.conf.HiveConf;

public class TestMarkPartitionRemote extends TestMarkPartition{

  private static class RunMS implements Runnable {

    @Override
    public void run() {
      try {
        HiveMetaStore.main(new String[] { "29111" });
      } catch (Throwable e) {
        e.printStackTrace(System.err);
        assert false;
      }
    }

  }
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    Thread t = new Thread(new RunMS());
    t.setDaemon(true);
    t.start();
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:29111");
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTRETRIES, 3);
    Thread.sleep(30000);
  }
}
