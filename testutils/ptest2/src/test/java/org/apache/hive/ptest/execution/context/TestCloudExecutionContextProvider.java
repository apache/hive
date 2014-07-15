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
package org.apache.hive.ptest.execution.context;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hive.ptest.execution.MockSSHCommandExecutor;
import org.apache.hive.ptest.execution.conf.Host;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.jclouds.domain.Location;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class TestCloudExecutionContextProvider {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestCloudExecutionContextProvider.class);
  private static final String PRIVATE_KEY = "mykey";
  private static final String USER = "user";
  private static final String[] SLAVE_DIRS = {"/tmp/hive-ptest"};
  private static final int NUM_NODES = 2;
  @Rule
  public TemporaryFolder baseDir = new TemporaryFolder();

  private String dataDir;
  private CloudComputeService cloudComputeService;
  private MockSSHCommandExecutor sshCommandExecutor;
  private String workingDir;
  private Template template;
  private NodeMetadata node1;
  private NodeMetadata node2;
  private NodeMetadata node3;
  private RunNodesException runNodesException;

  @Before
  public void setup() throws Exception {
    dataDir = baseDir.newFolder().getAbsolutePath();
    workingDir = baseDir.newFolder().getAbsolutePath();
    cloudComputeService = mock(CloudComputeService.class);
    sshCommandExecutor = new MockSSHCommandExecutor(LOG);
    node1 = mock(NodeMetadata.class);
    node2 = mock(NodeMetadata.class);
    node3 = mock(NodeMetadata.class);
    template = mock(Template.class);
    when(template.getLocation()).thenReturn(mock(Location.class));
    when(template.getImage()).thenReturn(mock(Image.class));
    when(template.getHardware()).thenReturn(mock(Hardware.class));
    when(node1.getHostname()).thenReturn("node1");
    when(node1.getPublicAddresses()).thenReturn(Collections.singleton("1.1.1.1"));
    when(node2.getHostname()).thenReturn("node2");
    when(node2.getPublicAddresses()).thenReturn(Collections.singleton("1.1.1.2"));
    when(node3.getHostname()).thenReturn("node3");
    when(node3.getPublicAddresses()).thenReturn(Collections.singleton("1.1.1.3"));
    runNodesException = new RunNodesException("", 2, template,
        Collections.singleton(node1), Collections.<String, Exception>emptyMap(),
        Collections.singletonMap(node2, new Exception("For testing")));
  }
  @After
  public void teardown() {

  }

  @org.junit.Test
  public void testRetry() throws Exception {
    when(cloudComputeService.createNodes(anyInt())).then(new Answer<Set<NodeMetadata>>() {
      int count = 0;
      @Override
      public Set<NodeMetadata> answer(InvocationOnMock invocation)
          throws Throwable {
        if(count++ == 0) {
          throw runNodesException;
        }
        return Collections.singleton(node3);
      }
    });
    CloudExecutionContextProvider provider = new CloudExecutionContextProvider(dataDir, NUM_NODES,
        cloudComputeService, sshCommandExecutor, workingDir, PRIVATE_KEY, USER, SLAVE_DIRS, 1, 0, 1);
    ExecutionContext executionContext = provider.createExecutionContext();
    Set<String> hosts = Sets.newHashSet();
    for(Host host : executionContext.getHosts()) {
      hosts.add(host.getName());
    }
    Assert.assertEquals(Sets.newHashSet("1.1.1.1", "1.1.1.3"), hosts);
  }
}
