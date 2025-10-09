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
package org.apache.hadoop.hive.metastore.txn.jdbc.functions;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

public class GenerateCompactionQueueIdFunction implements TransactionalFunction<Long> {

  private static final Logger LOG = LoggerFactory.getLogger(GenerateCompactionQueueIdFunction.class);

  public GenerateCompactionQueueIdFunction() {}

  @Override
  public Long execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException  {
    // Get the id for the next entry in the queue
    String sql = jdbcResource.getSqlGenerator().addForUpdateClause("SELECT \"NCQ_NEXT\" FROM \"NEXT_COMPACTION_QUEUE_ID\"");
    LOG.debug("going to execute SQL <{}>", sql);

    Long allocatedId = jdbcResource.getJdbcTemplate().query(sql, rs -> {
      if (!rs.next()) {
        throw new IllegalStateException("Transaction tables not properly initiated, "
            + "no record found in next_compaction_queue_id");
      }
      long id = rs.getLong(1);

      int count = jdbcResource.getJdbcTemplate().update("UPDATE \"NEXT_COMPACTION_QUEUE_ID\" SET \"NCQ_NEXT\" = :newId WHERE \"NCQ_NEXT\" = :id",
          new MapSqlParameterSource()
              .addValue("id", id)
              .addValue("newId", id + 1));

      if (count != 1) {
        //TODO: Eliminate this id generation by implementing: https://issues.apache.org/jira/browse/HIVE-27121
        LOG.info("The returned compaction ID ({}) already taken, obtaining new", id);
        return null;
      }
      return id;
    });
    if (allocatedId == null) {
      return execute(jdbcResource);
    } else {
      return allocatedId;
    }
  }

}
