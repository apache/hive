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
import org.apache.hadoop.hive.ql.anon.policy.DataErasureRule;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureStatement;
import org.apache.hadoop.hive.ql.anon.tez.Stats;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_COLUMN_INTERNAL_FORMAT;

public class RowAnonymizer {

  private static final Logger LOG = LoggerFactory.getLogger(RowAnonymizer.class);

  private final DataErasurePolicy policy;
  private final Anonymizer anonymizer;
  private final BodyConverter converter;
  private Stats stats;

  public RowAnonymizer(final Configuration conf, final DataErasurePolicy policy) {
    this.policy = policy;
    final ColumnInternalFormat colFormat = ColumnInternalFormat.valueOf(conf.get(ANON_COLUMN_INTERNAL_FORMAT));
    this.anonymizer = AnonymizerFactory.getAnonymizer(colFormat);
    this.converter = BodyConverterFactory.getBodyConverter(colFormat);
    LOG.info("column format: {}", colFormat);
  }

  public void setStats(final Stats stats) {
    this.stats = stats;
  }

  public Object anonymize(final WritableComparable msgId, final Object msg) {
    if (!policy.hasId(msgId)) {
      return msg;
    }
    final DataErasureStatement statement = policy.getStmt(msgId);
    countMatches(statement);
    final DataErasureStatement bodyMutating = bodyMutatingRulesOnly(statement);
    if (bodyMutating == null) {
      return msg;
    }
    return anonymizer.anonymize(msg, bodyMutating);
  }

  public Writable anonymize(final WritableComparable msgId, final Writable body) {
    if (!policy.hasId(msgId)) {
      return body;
    }
    final DataErasureStatement statement = policy.getStmt(msgId);
    countMatches(statement);
    final DataErasureStatement bodyMutating = bodyMutatingRulesOnly(statement);
    if (bodyMutating == null) {
      return body;
    }
    final Object msg = converter.convertBody(msgId, body);
    final Object out = anonymizer.anonymize(msg, bodyMutating);
    return converter.convertMessage(out);
  }

  public boolean matches(final WritableComparable msgId) {
    return policy.hasId(msgId);
  }

  private void countMatches(final DataErasureStatement statement) {
    if (stats == null || statement == null || statement.rules == null) {
      return;
    }
    for (final DataErasureRule r : statement.rules) {
      switch (r.resolvedAction()) {
        case "INSPECT":
          stats.matchesInspected++;
          break;
        case "FLAG":
          stats.matchesFlagged++;
          break;
        case "ERASE":
        case "REPLACE":
        default:
          stats.matchesRedacted++;
          break;
      }
    }
  }

  private static DataErasureStatement bodyMutatingRulesOnly(final DataErasureStatement statement) {
    if (statement == null || statement.rules == null || statement.rules.isEmpty()) {
      return statement;
    }
    final ArrayList<DataErasureRule> kept = new ArrayList<>(statement.rules.size());
    for (final DataErasureRule r : statement.rules) {
      final String a = r.resolvedAction();
      if ("ERASE".equals(a) || "REPLACE".equals(a)) {
        kept.add(r);
      }
    }
    if (kept.isEmpty()) {
      return null;
    }
    if (kept.size() == statement.rules.size()) {
      return statement;
    }
    final DataErasureStatement filtered = new DataErasureStatement();
    filtered.schemaId = statement.schemaId;
    filtered.rules = kept;
    return filtered;
  }
}
