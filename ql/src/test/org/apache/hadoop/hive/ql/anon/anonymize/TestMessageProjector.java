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

import org.apache.hadoop.hive.ql.anon.model.Msg3;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureRule;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.MSG_MSG_3;
import static org.apache.hadoop.hive.ql.anon.utils.MessageUtils.createMsg3;

public class TestMessageProjector {

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


  @Test
  public void projectsDeclaredLeavesAndNothingElse() {
    final MessageProjector projector = new MessageProjector();
    final Msg3 msg = createMsg3(SUBJECT_ID, 20);

    final Map<String, Object> projection = projector.project(msg,
        stmt(rule("telephone", "ERASE"),
             rule("country",   "ERASE")));

    Assertions.assertEquals(2, projection.size(),
        "projection must carry exactly the declared paths; got: " + projection);
    Assertions.assertTrue(projection.containsKey("telephone"));
    Assertions.assertTrue(projection.containsKey("country"));
    Assertions.assertEquals(msg.getTelephone(), projection.get("telephone"));
    Assertions.assertEquals(msg.getCountry(),   projection.get("country"));

    Assertions.assertFalse(projection.containsKey("city"),
        "projection must not include policy-undeclared fields; got: "
        + projection);
  }

  @Test
  public void doesNotMutateTheMessage() {
    final MessageProjector projector = new MessageProjector();
    final Msg3 msg = createMsg3(SUBJECT_ID, 20);
    final String originalTel = msg.getTelephone();
    final String originalCity = msg.getCity();

    projector.project(msg, stmt(rule("telephone", "ERASE")));

    Assertions.assertEquals(originalTel,  msg.getTelephone(),
        "projector must be read-only; telephone must not change");
    Assertions.assertEquals(originalCity, msg.getCity(),
        "projector must be read-only; city must not change");
  }

  @Test
  public void preservesDeclarationOrder() {
    final MessageProjector projector = new MessageProjector();
    final Map<String, Object> first = projector.project(
        createMsg3(SUBJECT_ID, 20),
        stmt(rule("city", "ERASE"),
             rule("telephone", "ERASE"),
             rule("country", "ERASE")));

    final List<String> keys = new ArrayList<>(first.keySet());
    Assertions.assertEquals(List.of("city", "telephone", "country"), keys,
        "projection key order must follow rule declaration order");
  }

  @Test
  public void emptyRuleSetProducesEmptyProjection() {
    final MessageProjector projector = new MessageProjector();
    final Map<String, Object> projection = projector.project(
        createMsg3(SUBJECT_ID, 20), stmt());
    Assertions.assertTrue(projection.isEmpty(),
        "no rules → no projected fields; got: " + projection);
  }


  @Test
  public void listOfStringsLeafReturnsACopiedList() {
    final MessageProjector projector = new MessageProjector();
    final Msg3 msg = createMsg3(SUBJECT_ID, 20);
    final Map<String, Object> projection = projector.project(msg,
        stmt(rule("ipList", "ERASE")));

    Assertions.assertTrue(projection.containsKey("ipList"));
    final Object value = projection.get("ipList");
    Assertions.assertTrue(value instanceof List,
        "list-of-strings leaf must project as a List; got: " + value);
    final List<?> projected = (List<?>) value;
    final List<String> expected = msg.getIpList();
    Assertions.assertEquals(expected.size(), projected.size(),
        "list projection must include every element");
    for (int i = 0; i < expected.size(); i++) {
      Assertions.assertEquals(expected.get(i), projected.get(i));
    }
    Assertions.assertNotSame(expected, projected,
        "projection must hand back a defensive copy, not the original list");
  }


  @Test
  public void rejectsNonBaseMsgInput() {
    final MessageProjector projector = new MessageProjector();
    Assertions.assertThrows(RuntimeException.class,
        () -> projector.project("not a BaseMsg", stmt(rule("country", "ERASE"))),
        "projector must refuse non-BaseMsg input");
  }
}
