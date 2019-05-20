/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.execution.conf;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import org.junit.Assert;
import org.apache.hive.ptest.execution.PTest;
import org.apache.hive.ptest.execution.context.ExecutionContext;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class TestTestConfiguration {

  public TemporaryFolder baseDir = new TemporaryFolder();
  private static final Logger LOG = LoggerFactory
      .getLogger(TestTestConfiguration.class);

  @Test
  public void testGettersSetters() throws Exception {
    Context ctx = Context.fromInputStream(
        Resources.getResource("test-configuration.properties").openStream());
    ExecutionContextConfiguration execConf = ExecutionContextConfiguration.withContext(ctx);
    TestConfiguration conf = TestConfiguration.withContext(ctx, LOG);
    Set<Host> expectedHosts = Sets.newHashSet(new Host("localhost", "hiveptest", new String[]{"/home/hiveptest"}, 2));
    ExecutionContext executionContext = execConf.getExecutionContextProvider().createExecutionContext();
    Assert.assertEquals(expectedHosts, executionContext.getHosts());
    Assert.assertEquals("/tmp/hive-ptest-units/working/dir", execConf.getWorkingDirectory());
    Assert.assertEquals("/etc/hiveptest/conf", execConf.getProfileDirectory());
    Assert.assertEquals("/tmp/hive-ptest-units/working/dir/logs", execConf.getGlobalLogDirectory());
    Assert.assertEquals("/home/brock/.ssh/id_rsa", executionContext.getPrivateKey());
    Assert.assertEquals("git://github.com/apache/hive.git", conf.getRepository());
    Assert.assertEquals("apache-github", conf.getRepositoryName());
    Assert.assertEquals("trunk", conf.getBranch());
    Assert.assertEquals("/tmp/hive-ptest-units/working/dir/working", executionContext.getLocalWorkingDirectory());
    Assert.assertEquals("-Dtest.continue.on.failure=true -Dtest.silent=false", conf.getAntArgs());
    Assert.assertEquals("hadoop-1,hadoop-2", conf.getAdditionalProfiles());
    Assert.assertNotNull(conf.toString());

    Assert.assertEquals("", conf.getPatch());
    conf.setPatch("Patch");
    Assert.assertEquals("Patch", conf.getPatch());

    conf.setRepository("Repository");
    Assert.assertEquals("Repository", conf.getRepository());

    conf.setRepositoryName("RepositoryName");
    Assert.assertEquals("RepositoryName", conf.getRepositoryName());

    conf.setBranch("Branch");
    Assert.assertEquals("Branch", conf.getBranch());

    conf.setAntArgs("AntArgs");
    Assert.assertEquals("AntArgs", conf.getAntArgs());
  }
  @Test
  public void testContext() throws Exception {
    Properties properties = new Properties();
    properties.load(Resources.getResource("test-configuration.properties").openStream());
    Context context = new Context(Maps.fromProperties(properties));
    Assert.assertEquals(context.getParameters(), (new TestConfiguration(context, LOG)).getContext().getParameters());

  }

  @Test
  public void testPTest() throws Exception {
    Host testHost = new Host("test", "test", new String[1], 1);
    Set<Host> testHosts = new HashSet<Host>();
    testHosts.add(testHost);

    Context ctx = Context.fromInputStream(Resources.getResource("test-configuration.properties").openStream());
    TestConfiguration conf = TestConfiguration.withContext(ctx, LOG);
    ExecutionContext execContext = new ExecutionContext(null, testHosts, "test", null);
    PTest.Builder mPTestBuilder = new PTest.Builder();
    PTest ptest = mPTestBuilder.build(conf, execContext, "1234", baseDir.newFolder(), null, null, null, null);
    Map<String, String> templateDefaults = ptest.getTemplateDefaults();
    Assert.assertEquals("git://github.com/apache/hive.git", templateDefaults.get("repository"));
    Assert.assertEquals("apache-github", templateDefaults.get("repositoryName"));
    Assert.assertEquals("trunk", templateDefaults.get("branch"));
    Assert.assertEquals("-Dtest.continue.on.failure=true -Dtest.silent=false", templateDefaults.get("antArgs"));
    Assert.assertEquals("hadoop-1,hadoop-2", templateDefaults.get("additionalProfiles"));
  }
}
