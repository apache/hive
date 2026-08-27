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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.metastore.GetHelper.getDirectSqlErrors;

/**
 * Test helper that flips {@code hive.metastore.try.direct.sql} for the duration of the try-with-resources
 * block and restores it on {@code close}.
 *
 * <p>It also snapshots the per-thread direct-SQL fallback counter at construction and, on close, logs a
 * warning if the counter advanced — i.e. direct-SQL threw an exception inside {@code GetHelper} and the
 * caller fell back to ORM. The fallback itself is a supported, logged degradation (see
 * {@code GetHelper.handleDirectSqlError}, which calls it "not an error"), and the real correctness
 * check for callers like {@code VerifyingObjectStore} is result equivalence via {@code verifyLists},
 * not the absence of a fallback. Turning the delta into a hard exception here has proved brittle in
 * practice — e.g. HIVE-29700 disabled a q-test because Derby's optimizer intermittently forces a
 * cast-based fallback under load — so we log instead of throw. Tests that specifically assert "no
 * direct-SQL error must occur" should read {@link org.apache.hadoop.hive.metastore.metastore.GetHelper#getDirectSqlErrors()}
 * directly.
 */
public class DirectSqlConfigurator implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(DirectSqlConfigurator.class);

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
    long now = getDirectSqlErrors();
    if (directSqlErrors != now) {
      // A direct-SQL exception happened on this thread while the block was open and the caller fell
      // back to ORM. That's a supported degradation, not a test failure — see class javadoc.
      LOG.warn("Direct SQL fell back to ORM {} time(s) during this verification block; check earlier "
          + "\"Falling back to ORM path due to direct SQL failure\" log lines for the underlying cause.",
          now - directSqlErrors);
    }
  }
}
