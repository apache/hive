  /**
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * We would have used the curator ephemeral node with some extra logic but it doesn't handle
 * the EXISTS condition, which is crucial here; so we c/p parts of Curator and add our logic.
 */
public class SlotZnode implements Closeable {
  static final Charset CHARSET = StandardCharsets.UTF_8;
  private final static Logger LOG = LoggerFactory.getLogger(SlotZnode.class);

  private final AtomicReference<CountDownLatch> initialCreateLatch =
      new AtomicReference<CountDownLatch>(new CountDownLatch(1));
  private final AtomicReference<String> nodePath = new AtomicReference<String>(null);
  private final Random rdm = new Random();
  private final CuratorFramework client;
  private final String basePath, prefix, workerPrefix, dataStr;
  private final byte[] data;
  private int currentSlot;
  private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
  private int fallbackCount = 0; // Test-only counter.
  private final BackgroundCallback backgroundCallback = new BackgroundCallback() {
    @Override
    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
      processCreateResult(client, event);
    }
  };
  private final Watcher watcher = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      processWatchedEvent(event);
    }
  };
  private final BackgroundCallback checkExistsCallback = new BackgroundCallback() {
    @Override
    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
      processWatchResult(event);
    }
  };
  private final ConnectionStateListener connectionStateListener = new ConnectionStateListener() {
    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
      processConnectionState(newState);
    }
  };

  private enum State {
    LATENT,
    INITIAL_SELECTION,
    AFTER_SELECTION,
    CLOSED
  }

  public SlotZnode(
      CuratorFramework client, String basePath, String prefix, String workerPrefix, String data) {
    this.client = Preconditions.checkNotNull(client, "client cannot be null");
    this.basePath = Preconditions.checkNotNull(basePath, "basePath cannot be null");
    this.prefix = Preconditions.checkNotNull(prefix, "prefix cannot be null");
    this.workerPrefix = workerPrefix;
    Preconditions.checkNotNull(data, "data cannot be null");
    this.dataStr = data;
    this.data = data.getBytes(CHARSET);
  }

  @VisibleForTesting
  public int getFallbackCount() {
    return fallbackCount;
  }

  private void chooseSlotToTake() throws Exception {
    int slotToTake = -1;
    while (true) {
      List<String> allChildNodes;
      try {
        allChildNodes = client.getChildren().forPath(basePath);
      } catch (Exception e) {
        LOG.error("Cannot list nodes to get slots; failing", e);
        throw e;
      }
      TreeSet<Integer> slots = new TreeSet<>();
      int approxWorkerCount = 0;
      for (String child : allChildNodes) {
        if (!child.startsWith(prefix)) {
          if (child.startsWith(workerPrefix)) {
            ++approxWorkerCount;
          }
        } else {
          slots.add(Integer.parseInt(child.substring(prefix.length())));
        }
      }
      Iterator<Integer> slotIter = slots.iterator();
      slotToTake = 0;
      while (slotIter.hasNext()) {
        int nextTaken = slotIter.next();
        if (slotToTake < nextTaken) break;
        slotToTake = nextTaken + 1;
      }
      // There may be a race for this slot so re-query after a delay with some probability.
      if (slotToTake != currentSlot || !shouldFallBackOnCollision(approxWorkerCount)) break;
      ++fallbackCount;
      Thread.sleep(rdm.nextInt(200)); // arbitrary
    }

    currentSlot = slotToTake;
    LOG.info("Will attempt to take slot " + currentSlot);
  }


  private boolean shouldFallBackOnCollision(int approxWorkerCount) {
    // Ideally, we'd want 1 worker to try for every slot; e.g. if there are 4 workers we want 3
    // to re-read, i.e. probability of falling back = 0.75, or 1/4 < random([0,1)). However, we
    // make it slightly more probable (2.0x) to avoid too much re-reading. This is hand-wavy.
    if (approxWorkerCount == 0) return false;
    return (2.0f / approxWorkerCount) <= rdm.nextDouble();
  }

  private String getSlotPath(int slot) {
    return String.format("%s/%s%010d", basePath, prefix, slot);
  }

  public boolean start(long timeout, TimeUnit unit) throws Exception {
    Preconditions.checkState(state.compareAndSet(State.LATENT, State.INITIAL_SELECTION), "Already started");
    CountDownLatch localLatch = initialCreateLatch.get();
    client.getConnectionStateListenable().addListener(connectionStateListener);
    chooseSlotToTake();
    startCreateCurrentNode();
    return localLatch.await(timeout, unit);
  }

  @Override
  public void close() throws IOException {
    State currentState = state.getAndSet(State.CLOSED);
    if (currentState == State.CLOSED || currentState == State.LATENT) return;
    client.getConnectionStateListenable().removeListener(connectionStateListener);
    String localNodePath = nodePath.getAndSet(null);
    if (localNodePath == null) return;
    try {
      client.delete().guaranteed().forPath(localNodePath);
    } catch (KeeperException.NoNodeException ignore) {
    } catch (Exception e) {
      LOG.error("Deleting node: " + localNodePath, e);
      throw new IOException(e);
    }
  }

  public int getCurrentSlot() {
    assert isActive();
    return currentSlot;
  }

  private void startCreateCurrentNode() {
    if (!isActive()) return;
    String createPath = null;
    try {
      createPath = getSlotPath(currentSlot);
      LOG.info("Attempting to create " + createPath);
      client.create().withMode(CreateMode.EPHEMERAL).inBackground(backgroundCallback)
        .forPath(createPath, data);
    } catch (Exception e) {
      LOG.error("Creating node. Path: " + createPath, e);
      throw new RuntimeException(e);
    }
  }

  private void watchNode() throws Exception {
    if (!isActive()) return;
    String localNodePath = nodePath.get();
    if (localNodePath == null) return;
    try {
      client.checkExists().usingWatcher(watcher).inBackground(
          checkExistsCallback).forPath(localNodePath);
    } catch (Exception e) {
      LOG.error("Watching node: " + localNodePath, e);
      throw e;
    }
  }

  private boolean isActive() {
    State localState = state.get();
    return (localState != State.LATENT && localState != State.CLOSED);
  }

  private void processWatchResult(CuratorEvent event) throws Exception {
    if (event.getResultCode() != KeeperException.Code.NONODE.intValue()) return;
    LOG.info("Trying to reacquire because of the NONODE event");
    startCreateCurrentNode();
  }


  private void processConnectionState(ConnectionState newState) {
    if (newState != ConnectionState.RECONNECTED) return;
    LOG.info("Trying to reacquire because of the RECONNECTED event");
    startCreateCurrentNode();
  }


  private void processWatchedEvent(WatchedEvent event) {
    if (event.getType() != EventType.NodeDeleted) return;
    String localPath = nodePath.get();
    if (localPath == null) return;
    if (!localPath.equals(event.getPath())) {
      LOG.info("Ignoring the NodeDeleted event for " + event.getPath());
      return;
    }
    LOG.info("Trying to reacquire because of the NodeDeleted event");
    startCreateCurrentNode();
  }


  private void processCreateResult(CuratorFramework client, CuratorEvent event) throws Exception {
    boolean doesExist = event.getResultCode() == KeeperException.Code.NODEEXISTS.intValue();
    if (!doesExist && event.getResultCode() != KeeperException.Code.OK.intValue()) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Trying to reacquire due to create error: " + event);
      }
      startCreateCurrentNode(); // TODO: a pattern from Curator. Better error handling?
      return;
    }
    State localState = state.get();
    switch (localState) {
    case CLOSED:
    case LATENT:
      return;
    case INITIAL_SELECTION:
      if (doesExist) {
        LOG.info("Slot " + currentSlot + " was occupied");
        chooseSlotToTake(); // Try another slot.
        startCreateCurrentNode();
      } else {
        handleCreatedNode(event.getName());
      }
      break;
    case AFTER_SELECTION:
      if (doesExist) {
        processExistsFromCreate(client, event.getPath());
      } else {
        handleCreatedNode(event.getName());
      }
      break;
    default:
      throw new AssertionError("Unknown state " + localState);
    }
  }


  private void processExistsFromCreate(CuratorFramework client, String path) throws Exception {
    byte[] actual;
    try {
      actual = client.getData().forPath(path);
    } catch (Exception ex) {
      LOG.error("Error getting data for the node; will retry creating", ex);
      startCreateCurrentNode();
      return;
    }
    if (Arrays.equals(actual, data)) {
      handleCreatedNode(path);
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info("Data at {} is from a different node: {} (we are {})",
            path, new String(actual, CHARSET), dataStr);
      }
      nodePath.getAndSet(null);
      chooseSlotToTake(); // Try another slot.
      startCreateCurrentNode();
    }
  }

  private void handleCreatedNode(String path) throws Exception {
    while (true) {
      State localState = state.get();
      if (localState == State.CLOSED || localState == State.LATENT) return;
      if (state.compareAndSet(localState, State.AFTER_SELECTION)) break;
    }
    nodePath.set(path);
    watchNode();
    CountDownLatch localLatch = initialCreateLatch.getAndSet(null);
    if (localLatch != null) {
      localLatch.countDown();
    }
    LOG.info("Acquired the slot znode {}{}", path,
        localLatch != null ? "; this is the initial assignment" : "");
  }

  @VisibleForTesting
  public String getActualPath() {
    return nodePath.get();
  }
}
