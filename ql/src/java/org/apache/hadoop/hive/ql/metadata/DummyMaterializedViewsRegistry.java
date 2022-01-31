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

package org.apache.hadoop.hive.ql.metadata;

import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

public class DummyMaterializedViewsRegistry implements MaterializedViewsRegistry {

  @Override
  public void createMaterializedView(HiveConf conf, Table materializedViewTable) {

  }

  @Override
  public void refreshMaterializedView(HiveConf conf, Table materializedViewTable) {

  }

  @Override
  public void refreshMaterializedView(HiveConf conf, Table oldMaterializedViewTable, Table materializedViewTable) {

  }

  @Override
  public void dropMaterializedView(Table materializedViewTable) {

  }

  @Override
  public void dropMaterializedView(String dbName, String tableName) {

  }

  @Override
  public List<HiveRelOptMaterialization> getRewritingMaterializedViews() {
    return Collections.emptyList();
  }

  @Override
  public HiveRelOptMaterialization getRewritingMaterializedView(String dbName, String viewName, EnumSet<HiveRelOptMaterialization.RewriteAlgorithm> scope) {
    return null;
  }

  @Override
  public List<HiveRelOptMaterialization> getRewritingMaterializedViews(String querySql) {
    return Collections.emptyList();
  }

  @Override
  public boolean isEmpty() {
    return true;
  }
}
