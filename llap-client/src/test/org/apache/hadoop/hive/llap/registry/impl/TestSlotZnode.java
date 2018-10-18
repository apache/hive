/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.llap.registry.impl;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;

import org.apache.curator.test.KillSession;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.DebugUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/** See SlotZNode; some tests (and the setup) are c/p-ed or modified from Curator. */
public class TestSlotZnode {
  private static final String DIR = "/test";
  private static final String PATH = ZKPaths.makePath(DIR, "/foo");
  private final Collection<CuratorFramework> curatorInstances = Lists.newArrayList();
  private final Collection<SlotZnode> createdNodes = Lists.newArrayList();
  private TestingServer server;

  private static Logger LOG = LoggerFactory.getLogger(TestSlotZnode.class);

  @Before
  public void setUp() throws Exception {
    System.setProperty(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES, "true");
    while (server == null) {
      try {
        server = new TestingServer();
      } catch (BindException e) {
        LOG.warn("Getting bind exception - retrying to allocate server");
        server = null;
      }
    }
  }

  @After
  public void teardown() throws Exception {
    for (SlotZnode node : createdNodes) {
      node.close();
    }
    for (CuratorFramework curator : curatorInstances) {
      curator.close();
    }
    server.close();
    server = null;
  }

  @Test
  public void testDeletesNodeWhenClosed() throws Exception {
    CuratorFramework curator = newCurator();
    SlotZnode node = createZnode(curator);
    assertTrue(node.start(30, TimeUnit.SECONDS));
    String path = null;
    try {
      path = node.getActualPath();
      assertNodeExists(curator, path);
    } finally {
      node.close(); // After closing the path is set to null...
    }
    assertNodeDoesNotExist(curator, path);
  }

  @Test
  public void testDeletedAndRecreatedNodeWhenSessionReconnects() throws Exception {
    CuratorFramework curator = newCurator();
    CuratorFramework observer = newCurator();
    SlotZnode node = createZnode(curator);
    assertTrue(node.start(30, TimeUnit.SECONDS));
    String originalPath = node.getActualPath();
    assertNodeExists(observer, originalPath);
    Trigger deletedTrigger = Trigger.deleted();
    observer.checkExists().usingWatcher(deletedTrigger).forPath(node.getActualPath());
    killSession(curator);
    // Make sure the node got deleted.
    assertTrue(deletedTrigger.firedWithin(30, TimeUnit.SECONDS));
    // Check for it to be recreated.
    Trigger createdTrigger = Trigger.created();
    Stat stat = observer.checkExists().usingWatcher(createdTrigger).forPath(originalPath);
    assertTrue(stat != null || createdTrigger.firedWithin(30, TimeUnit.SECONDS));
  }

  @Test
  public void testRecreatesNodeWhenItGetsDeleted() throws Exception {
    CuratorFramework curator = newCurator();
    SlotZnode node = createZnode(curator);
    assertTrue(node.start(30, TimeUnit.SECONDS));
    String originalNode = node.getActualPath();
    assertNodeExists(curator, originalNode);
    // Delete the original node...
    curator.delete().forPath(originalNode);
    Trigger createdWatchTrigger = Trigger.created();
    Stat stat = curator.checkExists().usingWatcher(createdWatchTrigger).forPath(originalNode);
    assertTrue(stat != null || createdWatchTrigger.firedWithin(30, TimeUnit.SECONDS));
  }

  @Test
  public void testPathUsage() throws Exception {
    CuratorFramework curator = newCurator();
    SlotZnode node1 = createZnode(curator),
        node2 = createZnode(curator), node3 = createZnode(curator);
    assertTrue(node1.start(30, TimeUnit.SECONDS));
    String path1 = node1.getActualPath();
    assertTrue(node2.start(30, TimeUnit.SECONDS));
    String path2 = node2.getActualPath();
    assertFalse(path1.equals(path2));
    node1.close();
    // Path must be reused.
    assertTrue(node3.start(30, TimeUnit.SECONDS));
    assertTrue(path1.equals(node3.getActualPath()));
  }

  private void syncThreadStart(final CountDownLatch cdlIn, final CountDownLatch cdlOut) {
    cdlIn.countDown();
    try {
      cdlOut.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testConcurrencyAndFallback() throws Exception {
    concurrencyTest(100, true);
  }

  @Test
  public void testConcurrencyNoFallback() throws Exception {
    concurrencyTest(100, false);
  }

  private void concurrencyTest(final int nodeCount, boolean isFallback) throws Exception {
    final CuratorFramework curator = newCurator();
    ExecutorService executor = Executors.newFixedThreadPool(nodeCount);
    final CountDownLatch cdlIn = new CountDownLatch(nodeCount), cdlOut = new CountDownLatch(1);
    @SuppressWarnings("unchecked")
    FutureTask<SlotZnode>[] startTasks = new FutureTask[nodeCount];
    for (int i = 0; i < nodeCount; ++i) {
      if (isFallback) {
        curator.create().creatingParentsIfNeeded().forPath(PATH + "/worker-" + i);
      }
      final int ix = i;
      startTasks[i] = new FutureTask<SlotZnode>(new Callable<SlotZnode>() {
        SlotZnode node = createZnode(curator);
        public SlotZnode call() throws Exception {
          syncThreadStart(cdlIn, cdlOut);
          int id = System.identityHashCode(node);
          LOG.info("Starting the node " + id + " from task #" + ix);
          boolean result = node.start(30, TimeUnit.SECONDS);
          LOG.info((result ? "Started" : "Failed to start") + " the node from task #" + ix);
          return result ? node : null;
        }
      });
    }
    for (int i = 0; i < startTasks.length; ++i) {
      executor.execute(startTasks[i]);
    }
    cdlIn.await();
    cdlOut.countDown();
    boolean[] found = new boolean[nodeCount];
    int totalFallbackCount = 0;
    for (int i = 0; i < startTasks.length; ++i) {
      SlotZnode node = startTasks[i].get();
      assertNotNull("failed to start the node from task #" + i, node);
      totalFallbackCount += node.getFallbackCount();
      int slot = node.getCurrentSlot();
      assertTrue(slot < found.length);
      assertFalse(found[slot]); // Given these 2 lines we don't need to double check later.
      found[slot] = true;
    }
    if (isFallback) {
      LOG.info("Total fallback count " + totalFallbackCount);
      assertTrue(totalFallbackCount > 0);
    }
  }

  private CuratorFramework newCurator() throws IOException {
    CuratorFramework client = CuratorFrameworkFactory.newClient(
        server.getConnectString(), 10000, 10000, new RetryOneTime(1));
    client.start();
    curatorInstances.add(client);
    return client;
  }

  private SlotZnode createZnode(CuratorFramework curator) throws Exception {
    if (curator.checkExists().forPath(PATH) == null) {
      curator.create().creatingParentsIfNeeded().forPath(PATH);
    }
    SlotZnode result = new SlotZnode(curator, PATH, "slot-", "worker-", "");
    createdNodes.add(result);
    return result;
  }

  private void assertNodeExists(CuratorFramework curator, String path) throws Exception {
    assertNotNull(path);
    assertTrue(curator.checkExists().forPath(path) != null);
  }

  private void assertNodeDoesNotExist(CuratorFramework curator, String path) throws Exception {
    assertTrue(curator.checkExists().forPath(path) == null);
  }

  public void killSession(CuratorFramework curator) throws Exception {
    KillSession.kill(curator.getZookeeperClient().getZooKeeper(), curator.getZookeeperClient().getCurrentConnectionString());
  }

  private static final class Trigger implements Watcher {
    private final Event.EventType type;
    private final CountDownLatch latch;
    public Trigger(Event.EventType type) {
      assertNotNull(type);
      this.type = type;
      this.latch = new CountDownLatch(1);
    }
    @Override
    public void process(WatchedEvent event) {
      if (type == event.getType()) {
        latch.countDown();
      }
    }
    public boolean firedWithin(long duration, TimeUnit unit) {
      try {
        return latch.await(duration, unit);
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }
    private static Trigger created() {
      return new Trigger(Event.EventType.NodeCreated);
    }
    private static Trigger deleted() {
      return new Trigger(Event.EventType.NodeDeleted);
    }
  }
}
