/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import static org.apache.hadoop.hive.metastore.metastore.GetHelper.getDirectSqlErrors;

public class DirectSqlConfigurator implements AutoCloseable {
  private final Configuration conf;
  private final boolean origAllowSql;
  private final long directSqlErrors;

  public DirectSqlConfigurator(Configuration configuration, boolean tryDirectSql) {
    this.conf = configuration;
    this.origAllowSql = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.TRY_DIRECT_SQL);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.TRY_DIRECT_SQL, tryDirectSql);
    directSqlErrors = getDirectSqlErrors();
  }

  public void tryDirectSql(boolean tryDirectSql) {
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.TRY_DIRECT_SQL, tryDirectSql);
  }

  @Override
  public void close() throws MetaException {
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.TRY_DIRECT_SQL, origAllowSql);
    if (directSqlErrors != getDirectSqlErrors()) {
      throw new MetaException("An unexpected direct sql error raised behind," +
          " please check the log to see the details");
    }
  }
}
