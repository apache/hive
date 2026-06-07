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
import org.apache.hadoop.hive.ql.anon.convert.JsonBodyConverter;
import org.apache.hadoop.hive.ql.anon.model.Msg3;
import org.apache.hadoop.hive.ql.anon.policy.DataErasurePolicy;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureRule;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureStatement;
import org.apache.hadoop.hive.ql.anon.tez.Stats;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_COLUMN_INTERNAL_FORMAT;
import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.MSG_MSG_3;
import static org.apache.hadoop.hive.ql.anon.utils.MessageUtils.createMsg3;

public class TestRowAnonymizerInspectFlag {

  private static final int SUBJECT_ID = 1;

  private static DataErasureStatement statement(final DataErasureRule... rules) {
    final DataErasureStatement s = new DataErasureStatement();
    s.schemaId = String.valueOf(MSG_MSG_3);
    s.rules = new ArrayList<>();
    for (final DataErasureRule r : rules) {
      s.rules.add(r);
    }
    return s;
  }

  private static DataErasureRule rule(final String path, final String action) {
    final DataErasureRule r = new DataErasureRule();
    r.path = path;
    r.action = action;
    return r;
  }

  private static DataErasureRule replaceRule(final String path, final String value) {
    final DataErasureRule r = new DataErasureRule();
    r.path = path;
    r.action = "REPLACE";
    r.value = value;
    return r;
  }

  private static DataErasurePolicy policyOf(final DataErasureRule... rules) {
    final DataErasurePolicy p = new DataErasurePolicy();
    p.identityFieldName = "userId";
    p.identityFieldType = "int";
    p.schemaFieldType = "int";
    p.statements = new ArrayList<>();
    p.statements.add(statement(rules));
    return p;
  }

  private static Configuration jsonConf() {
    final Configuration c = new Configuration();
    c.set(ANON_COLUMN_INTERNAL_FORMAT, ColumnInternalFormat.JSON.name());
    return c;
  }

  @Test
  public void inspectAndFlagLeaveBodyIntactAndBumpCounters() {
    final DataErasurePolicy policy = policyOf(
        rule("country", "INSPECT"),
        rule("city",    "FLAG"));
    final RowAnonymizer anon = new RowAnonymizer(jsonConf(), policy);
    final Stats stats = new Stats();
    anon.setStats(stats);

    final Msg3 original = createMsg3(SUBJECT_ID, 20);
    final Text body = new Text(JsonBodyConverter.serializeMsg(original));

    final Text out = (Text) anon.anonymize(new IntWritable(MSG_MSG_3), body);
    final Msg3 returned = JsonBodyConverter.convert(out.toString(), Msg3.class);

    Assertions.assertEquals(original.getCountry(), returned.getCountry());
    Assertions.assertEquals(original.getCity(),    returned.getCity());
    Assertions.assertEquals(1, stats.matchesInspected);
    Assertions.assertEquals(1, stats.matchesFlagged);
    Assertions.assertEquals(0, stats.matchesRedacted);
  }

  @Test
  public void mixedRulesApplyEraseAndCountInspect() {
    final DataErasurePolicy policy = policyOf(
        rule("country",   "INSPECT"),
        rule("telephone", "ERASE"));
    final RowAnonymizer anon = new RowAnonymizer(jsonConf(), policy);
    final Stats stats = new Stats();
    anon.setStats(stats);

    final Msg3 original = createMsg3(SUBJECT_ID, 20);
    final Text body = new Text(JsonBodyConverter.serializeMsg(original));

    final Text out = (Text) anon.anonymize(new IntWritable(MSG_MSG_3), body);
    final Msg3 returned = JsonBodyConverter.convert(out.toString(), Msg3.class);

    Assertions.assertEquals(original.getCountry(), returned.getCountry());
    Assertions.assertEquals("", returned.getTelephone());
    Assertions.assertEquals(1, stats.matchesInspected);
    Assertions.assertEquals(0, stats.matchesFlagged);
    Assertions.assertEquals(1, stats.matchesRedacted);
  }

  @Test
  public void replaceWritesLiteralInsteadOfErasing() {
    final DataErasurePolicy policy = policyOf(
        replaceRule("country", "UNKNOWN"),
        replaceRule("city",    "REDACTED"));
    final RowAnonymizer anon = new RowAnonymizer(jsonConf(), policy);
    final Stats stats = new Stats();
    anon.setStats(stats);

    final Msg3 original = createMsg3(SUBJECT_ID, 20);
    final Text body = new Text(JsonBodyConverter.serializeMsg(original));
    final Text out = (Text) anon.anonymize(new IntWritable(MSG_MSG_3), body);
    final Msg3 returned = JsonBodyConverter.convert(out.toString(), Msg3.class);

    Assertions.assertEquals("UNKNOWN",  returned.getCountry());
    Assertions.assertEquals("REDACTED", returned.getCity());
    Assertions.assertEquals(original.getTelephone(), returned.getTelephone());
    Assertions.assertEquals(2, stats.matchesRedacted);
    Assertions.assertEquals(0, stats.matchesInspected);
    Assertions.assertEquals(0, stats.matchesFlagged);
  }

  @Test
  public void replaceOnListFieldFillsEveryElement() {
    final DataErasurePolicy policy = policyOf(
        replaceRule("ipList", "REDACTED"));
    final RowAnonymizer anon = new RowAnonymizer(jsonConf(), policy);

    final Msg3 original = createMsg3(SUBJECT_ID, 20);
    final Text body = new Text(JsonBodyConverter.serializeMsg(original));
    final Text out = (Text) anon.anonymize(new IntWritable(MSG_MSG_3), body);
    final Msg3 returned = JsonBodyConverter.convert(out.toString(), Msg3.class);

    Assertions.assertFalse(returned.getIpList().isEmpty(), "list size preserved");
    for (final String s : returned.getIpList()) {
      Assertions.assertEquals("REDACTED", s);
    }
  }
}
