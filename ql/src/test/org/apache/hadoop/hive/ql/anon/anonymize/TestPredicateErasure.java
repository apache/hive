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

import org.apache.hadoop.hive.ql.anon.model.BaseMsg;
import org.apache.hadoop.hive.ql.anon.model.Msg3;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureRule;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.ql.anon.utils.MessageUtils.createMsg3;

public class TestPredicateErasure {

  public static final class Session extends BaseMsg {
    private String type;
    private String token;
    public Session() { }
    public Session(String type, String token) { this.type = type; this.token = token; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getToken() { return token; }
    public void setToken(String token) { this.token = token; }
  }

  public static final class Account extends BaseMsg {
    private String owner;
    private List<Session> sessions;
    public Account() { }
    public String getOwner() { return owner; }
    public void setOwner(String owner) { this.owner = owner; }
    public List<Session> getSessions() { return sessions; }
    public void setSessions(List<Session> sessions) { this.sessions = sessions; }
  }

  private static DataErasureStatement erase(String path) {
    final DataErasureStatement stmt = new DataErasureStatement();
    stmt.schemaId = "0";
    final DataErasureRule rule = new DataErasureRule();
    rule.path = path;
    rule.action = "ERASE";
    stmt.rules.add(rule);
    return stmt;
  }

  @Test
  public void indexedScalarListErasesOnlyThatElement() {
    final Msg3 msg = createMsg3(1, 20);
    msg.setIpList(new ArrayList<>(List.of("ip-a", "ip-b", "ip-c")));

    new MessageAnonymizer().anonymize(msg, erase("ipList[1]"));

    Assertions.assertEquals(List.of("ip-a", "", "ip-c"), msg.getIpList());
  }

  @Test
  public void wildcardScalarListErasesEveryElement() {
    final Msg3 msg = createMsg3(1, 20);
    msg.setIpList(new ArrayList<>(List.of("ip-a", "ip-b", "ip-c")));

    new MessageAnonymizer().anonymize(msg, erase("ipList[*]"));

    Assertions.assertEquals(List.of("", "", ""), msg.getIpList());
  }

  @Test
  public void filterSelectsMatchingSubObjectsOnly() {
    final Account acct = new Account();
    acct.setOwner("alice");
    acct.setSessions(new ArrayList<>(List.of(
        new Session("guest", "g-tok-1"),
        new Session("member", "m-tok-1"),
        new Session("guest", "g-tok-2"))));

    new MessageAnonymizer().anonymize(acct, erase("sessions[type='guest']:token"));

    Assertions.assertEquals("", acct.getSessions().get(0).getToken(), "guest token erased");
    Assertions.assertEquals("m-tok-1", acct.getSessions().get(1).getToken(), "member token preserved");
    Assertions.assertEquals("", acct.getSessions().get(2).getToken(), "guest token erased");
    Assertions.assertEquals("guest", acct.getSessions().get(0).getType(), "filter sub-field untouched");
  }

  @Test
  public void plainListPathStillErasesAllSubObjects() {
    final Account acct = new Account();
    acct.setSessions(new ArrayList<>(List.of(
        new Session("guest", "t1"), new Session("member", "t2"))));

    new MessageAnonymizer().anonymize(acct, erase("sessions:token"));

    Assertions.assertEquals("", acct.getSessions().get(0).getToken());
    Assertions.assertEquals("", acct.getSessions().get(1).getToken());
  }
}
