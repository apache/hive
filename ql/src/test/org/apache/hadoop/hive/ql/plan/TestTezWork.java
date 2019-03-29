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
package org.apache.hadoop.hive.ql.plan;

import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;

import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestTezWork {

  private static final String MR_JAR_PROPERTY = "tmpjars";
  private List<BaseWork> nodes;
  private TezWork work;

  @Before
  public void setup() throws Exception {
    nodes = new LinkedList<BaseWork>();
    work = new TezWork("", null);
    addWork(5);
  }

  private void addWork(int n) {
    for (int i = 0; i < n; ++i) {
      BaseWork w = new MapWork();
      nodes.add(w);
      work.add(w);
    }
  }

  @Test
  public void testAdd() throws Exception {
    Assert.assertEquals(work.getAllWork().size(), nodes.size());
    Assert.assertEquals(work.getRoots().size(), nodes.size());
    Assert.assertEquals(work.getLeaves().size(), nodes.size());
    for (BaseWork w: nodes) {
      Assert.assertEquals(work.getParents(w).size(), 0);
      Assert.assertEquals(work.getChildren(w).size(), 0);
    }
  }

  @Test
  public void testConnect() throws Exception {
    BaseWork parent = nodes.get(0);
    BaseWork child = nodes.get(1);

    TezEdgeProperty edgeProp = new TezEdgeProperty(EdgeType.SIMPLE_EDGE);
    work.connect(parent, child, edgeProp);

    Assert.assertEquals(work.getParents(child).size(), 1);
    Assert.assertEquals(work.getChildren(parent).size(), 1);
    Assert.assertEquals(work.getChildren(parent).get(0), child);
    Assert.assertEquals(work.getParents(child).get(0), parent);
    Assert.assertTrue(work.getRoots().contains(parent) && !work.getRoots().contains(child));
    Assert.assertTrue(!work.getLeaves().contains(parent) && work.getLeaves().contains(child));
    for (BaseWork w: nodes) {
      if (w == parent || w == child) {
        continue;
      }
      Assert.assertEquals(work.getParents(w).size(), 0);
      Assert.assertEquals(work.getChildren(w).size(), 0);
    }

    Assert.assertEquals(work.getEdgeProperty(parent, child).getEdgeType(), EdgeType.SIMPLE_EDGE);
  }

  @Test
  public void testBroadcastConnect() throws Exception {
    BaseWork parent = nodes.get(0);
    BaseWork child = nodes.get(1);

    TezEdgeProperty edgeProp = new TezEdgeProperty(EdgeType.BROADCAST_EDGE);
    work.connect(parent, child, edgeProp);

    Assert.assertEquals(work.getParents(child).size(), 1);
    Assert.assertEquals(work.getChildren(parent).size(), 1);
    Assert.assertEquals(work.getChildren(parent).get(0), child);
    Assert.assertEquals(work.getParents(child).get(0), parent);
    Assert.assertTrue(work.getRoots().contains(parent) && !work.getRoots().contains(child));
    Assert.assertTrue(!work.getLeaves().contains(parent) && work.getLeaves().contains(child));
    for (BaseWork w: nodes) {
      if (w == parent || w == child) {
        continue;
      }
      Assert.assertEquals(work.getParents(w).size(), 0);
      Assert.assertEquals(work.getChildren(w).size(), 0);
    }

    Assert.assertEquals(work.getEdgeProperty(parent, child).getEdgeType(), EdgeType.BROADCAST_EDGE);
  }

  @Test
  public void testDisconnect() throws Exception {
    BaseWork parent = nodes.get(0);
    BaseWork children[] = {nodes.get(1), nodes.get(2)};

    TezEdgeProperty edgeProp = new TezEdgeProperty(EdgeType.SIMPLE_EDGE);
    work.connect(parent, children[0], edgeProp);
    work.connect(parent, children[1], edgeProp);

    work.disconnect(parent, children[0]);

    Assert.assertTrue(work.getChildren(parent).contains(children[1]));
    Assert.assertTrue(!work.getChildren(parent).contains(children[0]));
    Assert.assertTrue(work.getRoots().contains(parent) && work.getRoots().contains(children[0])
                      && !work.getRoots().contains(children[1]));
    Assert.assertTrue(!work.getLeaves().contains(parent) && work.getLeaves().contains(children[0])
                      && work.getLeaves().contains(children[1]));
  }

  @Test
  public void testRemove() throws Exception {
    BaseWork parent = nodes.get(0);
    BaseWork children[] = {nodes.get(1), nodes.get(2)};

    TezEdgeProperty edgeProp = new TezEdgeProperty(EdgeType.SIMPLE_EDGE);
    work.connect(parent, children[0], edgeProp);
    work.connect(parent, children[1], edgeProp);

    work.remove(parent);

    Assert.assertEquals(work.getParents(children[0]).size(), 0);
    Assert.assertEquals(work.getParents(children[1]).size(), 0);
    Assert.assertEquals(work.getAllWork().size(), nodes.size()-1);
    Assert.assertEquals(work.getRoots().size(), nodes.size()-1);
    Assert.assertEquals(work.getLeaves().size(), nodes.size()-1);
  }

  @Test
  public void testGetAllWork() throws Exception {
    TezEdgeProperty edgeProp = new TezEdgeProperty(EdgeType.SIMPLE_EDGE);
    for (int i = 4; i > 0; --i) {
      work.connect(nodes.get(i), nodes.get(i-1), edgeProp);
    }

    List<BaseWork> sorted = work.getAllWork();
    for (int i = 0; i < 5; ++i) {
      Assert.assertEquals(sorted.get(i), nodes.get(4-i));
    }
  }

  @Test
  public void testConfigureJars() throws Exception {
    final JobConf conf = new JobConf();
    conf.set(MR_JAR_PROPERTY, "file:///tmp/foo1.jar");
    BaseWork baseWork = Mockito.mock(BaseWork.class);
    Mockito.doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        conf.set(MR_JAR_PROPERTY, "file:///tmp/foo2.jar");
        return null;
      }

    }).when(baseWork).configureJobConf(conf);

    work.add(baseWork);
    work.configureJobConfAndExtractJars(conf);
    Assert.assertEquals("file:///tmp/foo1.jar,file:///tmp/foo2.jar", conf.get(MR_JAR_PROPERTY));
  }

  @Test
  public void testConfigureJarsNoExtraJars() throws Exception {
    final JobConf conf = new JobConf();
    conf.set(MR_JAR_PROPERTY, "file:///tmp/foo1.jar");
    BaseWork baseWork = Mockito.mock(BaseWork.class);

    work.add(baseWork);
    work.configureJobConfAndExtractJars(conf);
    Assert.assertEquals("file:///tmp/foo1.jar", conf.get(MR_JAR_PROPERTY));
  }

  @Test
  public void testConfigureJarsWithNull() throws Exception {
    final JobConf conf = new JobConf();
    conf.set(MR_JAR_PROPERTY, "file:///tmp/foo1.jar");
    BaseWork baseWork = Mockito.mock(BaseWork.class);
    Mockito.doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        conf.unset(MR_JAR_PROPERTY);
        return null;
      }

    }).when(baseWork).configureJobConf(conf);

    work.add(baseWork);
    work.configureJobConfAndExtractJars(conf);
    Assert.assertEquals("file:///tmp/foo1.jar", conf.get(MR_JAR_PROPERTY));
  }

  @Test
  public void testConfigureJarsStartingWithNull() throws Exception {
    final JobConf conf = new JobConf();
    conf.unset(MR_JAR_PROPERTY);
    BaseWork baseWork = Mockito.mock(BaseWork.class);
    Mockito.doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        conf.setStrings(MR_JAR_PROPERTY, "file:///tmp/foo1.jar", "file:///tmp/foo2.jar");
        return null;
      }

    }).when(baseWork).configureJobConf(conf);

    work.add(baseWork);
    work.configureJobConfAndExtractJars(conf);
    Assert.assertEquals("file:///tmp/foo1.jar,file:///tmp/foo2.jar", conf.get(MR_JAR_PROPERTY));
  }
}
