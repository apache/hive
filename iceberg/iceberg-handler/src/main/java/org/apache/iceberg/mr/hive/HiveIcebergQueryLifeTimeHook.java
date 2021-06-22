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

package org.apache.iceberg.mr.hive;

import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHook;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookContext;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergQueryLifeTimeHook implements QueryLifeTimeHook {

  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergQueryLifeTimeHook.class);

  @Override
  public void beforeCompile(QueryLifeTimeHookContext ctx) {

  }

  @Override
  public void afterCompile(QueryLifeTimeHookContext ctx, boolean hasError) {
    if (hasError) {
      checkAndRollbackIcebergCTAS(ctx);
    }
  }

  @Override
  public void beforeExecution(QueryLifeTimeHookContext ctx) {

  }

  @Override
  public void afterExecution(QueryLifeTimeHookContext ctx, boolean hasError) {
    if (hasError) {
      checkAndRollbackIcebergCTAS(ctx);
    }
  }

  private void checkAndRollbackIcebergCTAS(QueryLifeTimeHookContext ctx) {
    HiveConf conf = ctx.getHiveConf();
    Optional<String> tableName = SessionStateUtil.getProperty(conf, InputFormatConfig.CTAS_TABLE_NAME);
    if (tableName.isPresent()) {
      LOG.info("Dropping the following CTAS target table as part of rollback: {}", tableName.get());
      Properties props = new Properties();
      props.put(Catalogs.NAME, tableName.get());
      Catalogs.dropTable(conf, props);
    }
  }
}
