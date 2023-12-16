/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.compaction;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context.RewritePolicy;
import org.apache.hadoop.hive.ql.DriverUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorContext;
import org.apache.hadoop.hive.ql.txn.compactor.QueryCompactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergMajorQueryCompactor extends QueryCompactor  {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergMajorQueryCompactor.class.getName());

  @Override
  public boolean run(CompactorContext context) throws IOException, HiveException, InterruptedException {

    String compactTableName = context.getTable().getTableName();
    Map<String, String> tblProperties = context.getTable().getParameters();
    LOG.debug("Initiating compaction for the {} table", compactTableName);

    String compactionQuery = String.format("insert overwrite table %s select * from %s",
        compactTableName, compactTableName);

    SessionState sessionState = setupQueryCompactionSession(context.getConf(),
        context.getCompactionInfo(), tblProperties);
    HiveConf.setVar(context.getConf(), ConfVars.REWRITE_POLICY, RewritePolicy.ALL_PARTITIONS.name());
    try {
      DriverUtils.runOnDriver(context.getConf(), sessionState, compactionQuery);
      LOG.info("Completed compaction for table {}", compactTableName);
    } catch (HiveException e) {
      LOG.error("Error doing query based {} compaction", RewritePolicy.ALL_PARTITIONS.name(), e);
      throw new RuntimeException(e);
    } finally {
      sessionState.setCompaction(false);
    }

    return true;
  }
}
