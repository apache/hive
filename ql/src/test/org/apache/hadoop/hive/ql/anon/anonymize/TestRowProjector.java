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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Map;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_COLUMN_INTERNAL_FORMAT;
import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.MSG_MSG_3;
import static org.apache.hadoop.hive.ql.anon.utils.MessageUtils.createMsg3;

public class TestRowProjector {

  private static final int SUBJECT_ID = 1;

  private static DataErasureRule rule(final String path, final String action) {
    final DataErasureRule r = new DataErasureRule();
    r.path = path;
    r.action = action;
    return r;
  }

  private static DataErasureStatement stmt(final DataErasureRule... rules) {
    final DataErasureStatement s = new DataErasureStatement();
    s.schemaId = String.valueOf(MSG_MSG_3);
    s.rules = new ArrayList<>();
    for (final DataErasureRule r : rules) {
      s.rules.add(r);
    }
    return s;
  }

  private static DataErasurePolicy policyOf(final DataErasureRule... rules) {
    final DataErasurePolicy p = new DataErasurePolicy();
    p.identityFieldName = "userId";
    p.identityFieldType = "int";
    p.schemaFieldType = "int";
    p.statements = new ArrayList<>();
    p.statements.add(stmt(rules));
    return p;
  }

  private static Configuration jsonConf() {
    final Configuration c = new Configuration();
    c.set(ANON_COLUMN_INTERNAL_FORMAT, ColumnInternalFormat.JSON.name());
    return c;
  }


  @Test
  public void projectsBodyAgainstPolicyPaths() {
    final DataErasurePolicy policy = policyOf(
        rule("telephone", "ERASE"),
        rule("country",   "ERASE"));
    final RowProjector projector = new RowProjector(jsonConf(), policy);

    final Msg3 original = createMsg3(SUBJECT_ID, 20);
    final Text body = new Text(JsonBodyConverter.serializeMsg(original));

    final Map<String, Object> projection =
        projector.project(new IntWritable(MSG_MSG_3), body);

    Assertions.assertEquals(2, projection.size(),
        "projection must hold exactly the policy-declared paths");
    Assertions.assertEquals(original.getTelephone(), projection.get("telephone"));
    Assertions.assertEquals(original.getCountry(),   projection.get("country"));
  }


  @Test
  public void unboundSchemaProducesEmptyProjection() {
    final DataErasurePolicy policy = policyOf(rule("telephone", "ERASE"));
    final RowProjector projector = new RowProjector(jsonConf(), policy);

    final Msg3 m = createMsg3(SUBJECT_ID, 20);
    final Text body = new Text(JsonBodyConverter.serializeMsg(m));

    final int unboundSchema = MSG_MSG_3 + 9999;
    final Map<String, Object> projection =
        projector.project(new IntWritable(unboundSchema), body);

    Assertions.assertTrue(projection.isEmpty(),
        "an unbound schema id must surface no projected fields; got: " + projection);
  }


  @Test
  public void projectsAlreadyDeserialisedMessage() {
    final DataErasurePolicy policy = policyOf(rule("city", "ERASE"));
    final RowProjector projector = new RowProjector(jsonConf(), policy);
    final Msg3 m = createMsg3(SUBJECT_ID, 20);

    final Map<String, Object> projection =
        projector.project(new IntWritable(MSG_MSG_3), m);

    Assertions.assertEquals(1, projection.size());
    Assertions.assertEquals(m.getCity(), projection.get("city"));
  }


  @Test
  public void matchesAgreesWithProjectionGate() {
    final DataErasurePolicy policy = policyOf(rule("telephone", "ERASE"));
    final RowProjector projector = new RowProjector(jsonConf(), policy);

    Assertions.assertTrue(projector.matches(new IntWritable(MSG_MSG_3)),
        "schema id declared on the policy must match");
    Assertions.assertFalse(projector.matches(new IntWritable(MSG_MSG_3 + 9999)),
        "schema id not on the policy must not match");
  }
}
