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

package org.apache.hadoop.hive.ql.anon.anonymize;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.convert.BodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.BodyConverterFactory;
import org.apache.hadoop.hive.ql.anon.policy.DataErasurePolicy;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureStatement;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_COLUMN_INTERNAL_FORMAT;

public class RowProjector {

  private static final Logger LOG = LoggerFactory.getLogger(RowProjector.class);

  private final DataErasurePolicy policy;
  private final Projector projector;
  private final BodyConverter converter;

  public RowProjector(final Configuration conf, final DataErasurePolicy policy) {
    this.policy = policy;
    final ColumnInternalFormat colFormat =
        ColumnInternalFormat.valueOf(conf.get(ANON_COLUMN_INTERNAL_FORMAT));
    this.projector = ProjectorFactory.getProjector(colFormat);
    this.converter = BodyConverterFactory.getBodyConverter(colFormat);
    LOG.info("RowProjector for column format: {}", colFormat);
  }

  public Map<String, Object> project(final WritableComparable msgId, final Object msg) {
    if (!policy.hasId(msgId)) {
      return Collections.emptyMap();
    }
    final DataErasureStatement statement = policy.getStmt(msgId);
    if (statement == null || statement.rules == null || statement.rules.isEmpty()) {
      return Collections.emptyMap();
    }
    return projector.project(msg, statement);
  }

  public Map<String, Object> project(final WritableComparable msgId, final Writable body) {
    if (!policy.hasId(msgId)) {
      return Collections.emptyMap();
    }
    final DataErasureStatement statement = policy.getStmt(msgId);
    if (statement == null || statement.rules == null || statement.rules.isEmpty()) {
      return Collections.emptyMap();
    }
    final Object msg = converter.convertBody(msgId, body);
    return projector.project(msg, statement);
  }

  public boolean matches(final WritableComparable msgId) {
    return policy.hasId(msgId);
  }
}
