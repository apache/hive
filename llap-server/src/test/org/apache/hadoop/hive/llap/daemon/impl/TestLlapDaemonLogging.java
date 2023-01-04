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
package org.apache.hadoop.hive.llap.daemon.impl;

import org.apache.hadoop.hive.llap.daemon.LlapDaemonExtension;
import org.apache.hadoop.hive.llap.daemon.LlapDaemonTestUtils;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hive.testutils.junit.extensions.DoNothingTCPServer;
import org.apache.hive.testutils.junit.extensions.DoNothingTCPServerExtension;
import org.apache.logging.log4j.junit.LoggerContextSource;
import org.apache.tez.common.security.TokenCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the log4j configuration of the LLAP daemons.
 */
@LoggerContextSource("llap-daemon-routing-log4j2.properties")
public class TestLlapDaemonLogging {

  @Test
  @ExtendWith(LlapDaemonExtension.class)
  @ExtendWith(DoNothingTCPServerExtension.class)
  void testQueryRoutingNoLeakFileDescriptors(LlapDaemon daemon, DoNothingTCPServer amMockServer)
      throws IOException, InterruptedException {
    final int amPort = amMockServer.port();
    Credentials credentials = validSessionCredentials();
    String appId = "application_1540489363818_0021";
    for (int i = 0; i < 10; i++) {
      String queryId = "query" + i;
      int dagId = 1000 + i;
      daemon.registerDag(LlapDaemonTestUtils.buildRegisterDagRequest(appId, dagId, credentials));
      daemon.submitWork(LlapDaemonTestUtils.buildSubmitProtoRequest(appId, dagId, queryId, amPort, credentials));
      daemon.queryComplete(LlapDaemonTestUtils.buildQueryCompleteRequest(appId, dagId));
    }
    // Busy wait till all daemon tasks are treated 
    while (!daemon.getExecutorsStatus().isEmpty()) {
      Thread.sleep(100);
    }
    // The IdlePurgePolicy used should close appenders after 5 seconds of inactivity.
    // Wait for 8 sec to give some margin. 
    Thread.sleep(8000);
    Pattern pn = Pattern.compile("query\\d++-dag_1540489363818_0021_\\d{4}\\.log");
    assertEquals(0, findOpenFileDescriptors(pn).count());
  }

  @Test
  @ExtendWith(LlapDaemonExtension.class)
  @ExtendWith(DoNothingTCPServerExtension.class)
  void testQueryRoutingLogFileNameOnIncompleteQuery(LlapDaemon daemon, DoNothingTCPServer amMockServer)
      throws IOException, InterruptedException {
    final int amPort = amMockServer.port();
    Credentials credentials = validSessionCredentials();
    String appId = "application_2500489363818_0021";
    String queryId = "query0";
    int dagId = 2000;
    daemon.registerDag(LlapDaemonTestUtils.buildRegisterDagRequest(appId, dagId, credentials));
    daemon.submitWork(LlapDaemonTestUtils.buildSubmitProtoRequest(appId, dagId, queryId, amPort, credentials));
    // Busy wait till all daemon tasks are treated 
    while (!daemon.getExecutorsStatus().isEmpty()) {
      Thread.sleep(100);
    }
    // The IdlePurgePolicy used should close appenders after 5 seconds of inactivity.
    // Wait for 8 sec to give some margin. 
    Thread.sleep(8000);
    assertFileExists("query0-dag_2500489363818_0021_2000.log");
  }

  @Test
  @ExtendWith(LlapDaemonExtension.class)
  @ExtendWith(DoNothingTCPServerExtension.class)
  void testQueryRoutingLogFileNameOnCompleteQuery(LlapDaemon daemon, DoNothingTCPServer amMockServer)
      throws IOException, InterruptedException {
    final int amPort = amMockServer.port();
    Credentials credentials = validSessionCredentials();
    String appId = "application_3500489363818_0021";
    String queryId = "query0";
    int dagId = 3000;
    daemon.registerDag(LlapDaemonTestUtils.buildRegisterDagRequest(appId, dagId, credentials));
    daemon.submitWork(LlapDaemonTestUtils.buildSubmitProtoRequest(appId, dagId, queryId, amPort, credentials));
    daemon.queryComplete(LlapDaemonTestUtils.buildQueryCompleteRequest(appId, dagId));
    // Busy wait till all daemon tasks are treated 
    while (!daemon.getExecutorsStatus().isEmpty()) {
      Thread.sleep(100);
    }
    // The IdlePurgePolicy used should close appenders after 5 seconds of inactivity.
    // Wait for 8 sec to give some margin. 
    Thread.sleep(8000);
    // A complete query should always have a corresponding log file with ".done" suffix
    assertFileExists("query0-dag_3500489363818_0021_3000.log.done");
  }

  private static Credentials validSessionCredentials() {
    Credentials credentials = new Credentials();
    Token<LlapTokenIdentifier> sessionToken =
        new Token<>("foo".getBytes(), "bar".getBytes(), new Text("kind"), new Text("service"));
    TokenCache.setSessionToken(sessionToken, credentials);
    return credentials;
  }

  private static Stream<Path> findOpenFileDescriptors(Pattern searchPattern) throws IOException {
    return Files.walk(Paths.get("/proc/self/fd")).filter(p -> {
      Path resolved = p;
      if (Files.isSymbolicLink(p)) {
        try {
          resolved = Files.readSymbolicLink(p);
        } catch (IOException exception) {
          throw new UncheckedIOException(exception);
        }
      }
      return searchPattern.matcher(resolved.toString()).find();
    });
  }

  private static void assertFileExists(String fileName) throws IOException {
    boolean found = Files.walk(Paths.get(System.getProperty("java.io.tmpdir"))).anyMatch(p -> p.endsWith(fileName));
    assertTrue(found, "File " + fileName + " was not found under " + System.getProperty("java.io.tmpdir"));
  }

}
