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
package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

/**
 * Same as TestTxnCommands2 but tests ACID tables with abort cleanup happening explicitly using
 * compaction cycle by default, and having 'transactional_properties' set to 'default'. This
 * specifically tests the abort cleanup done exclusively using compaction cycle for ACID tables.
 */
public class TestTxnCommands2WithAbortCleanupUsingCompactionCycle extends TestTxnCommands2 {
  public TestTxnCommands2WithAbortCleanupUsingCompactionCycle() {
    super();
  }

  @Override
  void initHiveConf() {
    super.initHiveConf();
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER, false);
  }
}
