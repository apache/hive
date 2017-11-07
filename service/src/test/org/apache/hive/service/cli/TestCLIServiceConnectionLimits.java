/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.service.cli;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.session.SessionManager;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Lists;

public class TestCLIServiceConnectionLimits {
  @org.junit.Rule
  public ExpectedException thrown = ExpectedException.none();

  private int limit = 10;
  private HiveConf conf = new HiveConf();

  @Test
  public void testNoLimit() throws HiveSQLException {
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER, 0);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_IPADDRESS, 0);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER_IPADDRESS, 0);
    CLIService service = getService(conf);
    List<SessionHandle> sessionHandles = new ArrayList<>();
    try {
      for (int i = 0; i < limit + 1; i++) {
        SessionHandle session = service.openSession(CLIService.SERVER_VERSION, "foo", "bar", "127.0.0.1", null);
        sessionHandles.add(session);
      }
    } finally {
      for (SessionHandle sessionHandle : sessionHandles) {
        service.closeSession(sessionHandle);
      }
      service.stop();
    }
  }

  @Test
  public void testIncrementAndDecrementConnectionsUser() throws HiveSQLException {
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER, 10);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_IPADDRESS, 0);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER_IPADDRESS, 0);
    CLIService service = getService(conf);
    List<SessionHandle> sessionHandles = new ArrayList<>();
    try {
      // open 5 connections
      for (int i = 0; i < limit / 2; i++) {
        SessionHandle session = service.openSession(CLIService.SERVER_VERSION, "foo", "bar", "127.0.0.1", null);
        sessionHandles.add(session);
      }

      // close them all
      for (SessionHandle sessionHandle : sessionHandles) {
        service.closeSession(sessionHandle);
      }
      sessionHandles.clear();

      // open till limit but not exceed
      for (int i = 0; i < limit; i++) {
        SessionHandle session = service.openSession(CLIService.SERVER_VERSION, "ff", "bar", "127.0.0.1", null);
        sessionHandles.add(session);
      }
    } finally {
      for (SessionHandle sessionHandle : sessionHandles) {
        service.closeSession(sessionHandle);
      }
      service.stop();
    }
  }

  @Test
  public void testInvalidUserName() throws HiveSQLException {
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER, 10);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_IPADDRESS, 0);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER_IPADDRESS, 0);
    CLIService service = getService(conf);
    List<SessionHandle> sessionHandles = new ArrayList<>();
    try {
      for (int i = 0; i < limit + 1; i++) {
        SessionHandle session = service.openSession(CLIService.SERVER_VERSION, null, "bar", "127.0.0.1", null);
        sessionHandles.add(session);
      }

      for (int i = 0; i < limit + 1; i++) {
        SessionHandle session = service.openSession(CLIService.SERVER_VERSION, "", "bar", "127.0.0.1", null);
        sessionHandles.add(session);
      }
    } finally {
      for (SessionHandle sessionHandle : sessionHandles) {
        service.closeSession(sessionHandle);
      }
      service.stop();
    }
  }

  @Test
  public void testInvalidIpaddress() throws HiveSQLException {
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER, 0);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_IPADDRESS, 10);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER_IPADDRESS, 0);
    CLIService service = getService(conf);
    List<SessionHandle> sessionHandles = new ArrayList<>();
    try {
      for (int i = 0; i < limit + 1; i++) {
        SessionHandle session = service.openSession(CLIService.SERVER_VERSION, "foo", "bar", null, null);
        sessionHandles.add(session);
      }

      for (int i = 0; i < limit + 1; i++) {
        SessionHandle session = service.openSession(CLIService.SERVER_VERSION, "foo", "bar", "", null);
        sessionHandles.add(session);
      }
    } finally {
      for (SessionHandle sessionHandle : sessionHandles) {
        service.closeSession(sessionHandle);
      }
      service.stop();
    }
  }

  @Test
  public void testInvalidUserIpaddress() throws HiveSQLException {
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER, 0);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_IPADDRESS, 10);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER_IPADDRESS, 0);
    CLIService service = getService(conf);
    List<SessionHandle> sessionHandles = new ArrayList<>();
    try {
      for (int i = 0; i < limit + 1; i++) {
        SessionHandle session = service.openSession(CLIService.SERVER_VERSION, "   ", "bar", null, null);
        sessionHandles.add(session);
      }
    } finally {
      for (SessionHandle sessionHandle : sessionHandles) {
        service.closeSession(sessionHandle);
      }
      service.stop();
    }
  }

  @Test
  public void testConnectionLimitPerUser() throws HiveSQLException {
    thrown.expect(HiveSQLException.class);
    thrown.expectMessage("Connection limit per user reached (user: foo limit: 10)");

    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER, 10);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_IPADDRESS, 0);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER_IPADDRESS, 0);
    CLIService service = getService(conf);
    List<SessionHandle> sessionHandles = new ArrayList<>();
    try {
      for (int i = 0; i < limit + 1; i++) {
        SessionHandle session = service.openSession(CLIService.SERVER_VERSION, "foo", "bar", "127.0.0.1", null);
        sessionHandles.add(session);
      }

    } finally {
      for (SessionHandle sessionHandle : sessionHandles) {
        service.closeSession(sessionHandle);
      }
      service.stop();
    }
  }

  @Test
  public void testConnectionLimitPerIpAddress() throws HiveSQLException {
    thrown.expect(HiveSQLException.class);
    thrown.expectMessage("Connection limit per ipaddress reached (ipaddress: 127.0.0.1 limit: 10)");

    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER, 0);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_IPADDRESS, 10);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER_IPADDRESS, 0);
    CLIService service = getService(conf);
    List<SessionHandle> sessionHandles = new ArrayList<>();
    try {
      for (int i = 0; i < limit + 1; i++) {
        SessionHandle session = service.openSession(CLIService.SERVER_VERSION, "foo", "bar", "127.0.0.1", null);
        sessionHandles.add(session);
      }

    } finally {
      for (SessionHandle sessionHandle : sessionHandles) {
        service.closeSession(sessionHandle);
      }
      service.stop();
    }
  }

  @Test
  public void testConnectionLimitPerUserIpAddress() throws HiveSQLException {
    thrown.expect(HiveSQLException.class);
    thrown.expectMessage("Connection limit per user:ipaddress reached (user:ipaddress: foo:127.0.0.1 limit: 10)");

    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER, 0);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_IPADDRESS, 0);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER_IPADDRESS, 10);
    CLIService service = getService(conf);
    List<SessionHandle> sessionHandles = new ArrayList<>();
    try {
      for (int i = 0; i < limit + 1; i++) {
        SessionHandle session = service.openSession(CLIService.SERVER_VERSION, "foo", "bar", "127.0.0.1", null);
        sessionHandles.add(session);
      }

    } finally {
      for (SessionHandle sessionHandle : sessionHandles) {
        service.closeSession(sessionHandle);
      }
      service.stop();
    }
  }

  @Test
  public void testConnectionMultipleLimitsUserAndIP() throws HiveSQLException {
    thrown.expect(HiveSQLException.class);
    thrown.expectMessage("Connection limit per user reached (user: foo limit: 5)");

    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER, 5);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_IPADDRESS, 10);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER_IPADDRESS, 0);
    CLIService service = getService(conf);
    List<SessionHandle> sessionHandles = new ArrayList<>();
    try {
      for (int i = 0; i < limit + 1; i++) {
        SessionHandle session = service.openSession(CLIService.SERVER_VERSION, "foo", "bar", "127.0.0.1", null);
        sessionHandles.add(session);
      }

    } finally {
      for (SessionHandle sessionHandle : sessionHandles) {
        service.closeSession(sessionHandle);
      }
      service.stop();
    }
  }

  @Test
  public void testConnectionMultipleLimitsIPAndUserIP() throws HiveSQLException {
    thrown.expect(HiveSQLException.class);
    thrown.expectMessage("Connection limit per ipaddress reached (ipaddress: 127.0.0.1 limit: 5)");

    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER, 0);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_IPADDRESS, 5);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER_IPADDRESS, 10);
    CLIService service = getService(conf);
    List<SessionHandle> sessionHandles = new ArrayList<>();
    try {
      for (int i = 0; i < limit + 1; i++) {
        SessionHandle session = service.openSession(CLIService.SERVER_VERSION, "foo", "bar", "127.0.0.1", null);
        sessionHandles.add(session);
      }

    } finally {
      for (SessionHandle sessionHandle : sessionHandles) {
        service.closeSession(sessionHandle);
      }
      service.stop();
    }
  }

  @Test
  public void testConnectionMultipleLimitsUserIPAndUser() throws HiveSQLException {
    thrown.expect(HiveSQLException.class);
    thrown.expectMessage("Connection limit per user:ipaddress reached (user:ipaddress: foo:127.0.0.1 limit: 10)");

    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER, 15);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_IPADDRESS, 0);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER_IPADDRESS, 10);
    CLIService service = getService(conf);
    List<SessionHandle> sessionHandles = new ArrayList<>();
    try {
      for (int i = 0; i < limit + 1; i++) {
        SessionHandle session = service.openSession(CLIService.SERVER_VERSION, "foo", "bar", "127.0.0.1", null);
        sessionHandles.add(session);
      }

    } finally {
      for (SessionHandle sessionHandle : sessionHandles) {
        service.closeSession(sessionHandle);
      }
      service.stop();
    }
  }

  @Test
  public void testConnectionForwardedIpAddresses() throws HiveSQLException {
    thrown.expect(HiveSQLException.class);
    thrown.expectMessage("Connection limit per ipaddress reached (ipaddress: 194.167.0.3 limit: 10)");

    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER, 0);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_IPADDRESS, 10);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER_IPADDRESS, 10);
    CLIService service = getService(conf);
    SessionManager.setForwardedAddresses(Lists.newArrayList("194.167.0.3", "194.167.0.2", "194.167.0.1"));
    List<SessionHandle> sessionHandles = new ArrayList<>();
    try {
      for (int i = 0; i < limit + 1; i++) {
        SessionHandle session = service.openSession(CLIService.SERVER_VERSION, "foo", "bar", "194.167.0.1", null);
        sessionHandles.add(session);
      }

    } finally {
      SessionManager.setForwardedAddresses(Collections.emptyList());
      for (SessionHandle sessionHandle : sessionHandles) {
        service.closeSession(sessionHandle);
      }
      service.stop();
    }
  }

  private CLIService getService(HiveConf conf) {
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
      "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    CLIService service = new CLIService(null);
    service.init(conf);
    service.start();
    return service;
  }
}
