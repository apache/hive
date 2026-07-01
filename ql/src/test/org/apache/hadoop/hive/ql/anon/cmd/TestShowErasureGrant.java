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

package org.apache.hadoop.hive.ql.anon.cmd;

import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.anon.consts.AnonConst;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

public class TestShowErasureGrant extends BaseTest {

  @Test
  public void showAllGrants() throws CommandProcessorException {
    execute("SHOW ERASURE GRANT");
  }

  @Test
  public void showGrantsForPrincipal() throws CommandProcessorException {
    execute("SHOW ERASURE GRANT FOR alice");
  }

  @Test
  public void showGrantsOnPolicy() throws CommandProcessorException {
    try {
      execute("SHOW ERASURE GRANT ON POLICY player_pii");
    } catch (CommandProcessorException expected) {
    }
  }

  @Test
  public void showGrantsForPrincipalOnPolicy() throws CommandProcessorException {
    try {
      execute("SHOW ERASURE GRANT FOR alice ON POLICY player_pii");
    } catch (CommandProcessorException expected) {
    }
  }

  @Test
  public void showSurfacesConfigErasureAdmins() throws CommandProcessorException {
    final String prev = conf.get(AnonConst.ANON_POLICY_GRANT_ADMIN_USERS, "");
    try {
      setAdmins("tsg_admin_x,tsg_admin_y");
      final String all = execute("SHOW ERASURE GRANT").stream()
          .map(Object::toString).collect(Collectors.joining("\n"));
      Assertions.assertTrue(
          all.contains("tsg_admin_x") && all.contains("ERASURE_ADMIN") && all.contains("config"),
          "config erasure-admins must appear as ERASURE_ADMIN/config rows; got:\n" + all);
      Assertions.assertTrue(all.contains("tsg_admin_y"),
          "every config erasure-admin must be listed; got:\n" + all);

      final String forX = execute("SHOW ERASURE GRANT FOR tsg_admin_x").stream()
          .map(Object::toString).collect(Collectors.joining("\n"));
      Assertions.assertTrue(forX.contains("tsg_admin_x"),
          "FOR <admin> must surface that admin; got:\n" + forX);
      Assertions.assertFalse(forX.contains("tsg_admin_y"),
          "FOR <admin> must not surface other admins; got:\n" + forX);
    } finally {
      setAdmins(prev);
    }
  }

  private void setAdmins(final String csv) {
    conf.set(AnonConst.ANON_POLICY_GRANT_ADMIN_USERS, csv);
    if (driver != null) {
      driver.getConf().set(AnonConst.ANON_POLICY_GRANT_ADMIN_USERS, csv);
    }
  }
}
