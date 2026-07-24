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
package org.apache.hive.tez.yarn;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.Container.ExecResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Smoke test for TezYarnClusterContainer. Verifies that a real HDFS+YARN
 * cluster comes up cleanly via Testcontainers and that the wrapper's helper
 * methods return usable addresses and can perform HDFS I/O.
 *
 * Run with: mvn -Pitests,tez-yarn -pl itests/tez-yarn-it -Dtest=TezYarnClusterContainerTest test
 */
public class TezYarnClusterContainerTest {

  private static TezYarnClusterContainer cluster;

  @BeforeClass
  public static void startCluster() {
    cluster = new TezYarnClusterContainer();
    cluster.start();
  }

  @AfterClass
  public static void stopCluster() {
    if (cluster != null) {
      cluster.stop();
    }
  }

  @Test
  public void testHdfsUriFormat() {
    String uri = cluster.getHdfsUri();
    Assert.assertNotNull("HDFS URI must not be null", uri);
    Assert.assertTrue("Expected hdfs:// URI, got: " + uri, uri.startsWith("hdfs://"));
  }

  @Test
  public void testResourceManagerAddressFormat() {
    String addr = cluster.getResourceManagerAddress();
    Assert.assertNotNull("RM address must not be null", addr);
    Assert.assertTrue("Expected host:port, got: " + addr, addr.contains(":"));
  }

  @Test
  public void testHdfsWriteAndRead() throws IOException, InterruptedException {
    ExecResult mkdir = cluster.namenodeContainer()
        .execInContainer("hdfs", "dfs", "-mkdir", "-p", "/tmp/smoke-test");
    Assert.assertEquals("hdfs mkdir failed:\n" + mkdir.getStderr(), 0, mkdir.getExitCode());

    ExecResult ls = cluster.namenodeContainer()
        .execInContainer("hdfs", "dfs", "-ls", "/tmp");
    Assert.assertTrue("Expected /tmp/smoke-test in HDFS listing:\n" + ls.getStdout(),
        ls.getStdout().contains("smoke-test"));
  }

  @Test
  public void testYarnNodeManagerRegistered() throws IOException, InterruptedException {
    ExecResult result = cluster.resourceManagerContainer()
        .execInContainer("yarn", "node", "-list");
    String out = result.getStdout();
    Assert.assertTrue("Expected 'Total Nodes:' in yarn node -list output:\n" + out,
        out.contains("Total Nodes:"));
    Assert.assertFalse("Expected at least one active NodeManager:\n" + out,
        out.contains("Total Nodes:0"));
  }

  @Test
  public void testUploadJarToHdfs() throws IOException, InterruptedException {
    Path tempJar = Files.createTempFile("smoke-upload", ".jar");
    try {
      Files.write(tempJar, "placeholder-jar-content".getBytes());

      String hdfsPath = cluster.uploadJarToHdfs(tempJar);
      Assert.assertNotNull("uploadJarToHdfs must return a non-null HDFS path", hdfsPath);

      ExecResult ls = cluster.namenodeContainer()
          .execInContainer("hdfs", "dfs", "-ls", "/tmp/hive-29483-jars");
      Assert.assertTrue("Uploaded jar not visible in HDFS:\n" + ls.getStdout(),
          ls.getStdout().contains(tempJar.getFileName().toString()));
    } finally {
      Files.deleteIfExists(tempJar);
    }
  }
}
