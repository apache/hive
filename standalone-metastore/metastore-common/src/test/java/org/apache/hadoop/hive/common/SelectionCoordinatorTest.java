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

package org.apache.hadoop.hive.common;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public class SelectionCoordinatorTest {

  private static final String SELECTION_PATH = "/mutex/select/leader/for/test";
  private final int BASE_SLEEP_TIME_MS = 1000;
  private final int MAX_RETRIES = 3;
  private final AtomicInteger leadershipChangeCount = new AtomicInteger();
  private final List<CuratorFramework> clients = new ArrayList<>();
  private TestingServer server = null;

  @Before public void setUp() throws Exception {
    server = new TestingServer();
  }

  @Test public void testLeadershipTransfer() throws InterruptedException {
    CountDownLatch uberLatch = new CountDownLatch(3);
    try {
      simulateCrash(uberLatch);
      simulateRelinquishingLeadership(uberLatch);
      simulateAnotherThread(uberLatch);
      uberLatch.await(1, TimeUnit.MINUTES);
    } finally {
      for (CuratorFramework client : clients)
        CloseableUtils.closeQuietly(client);
    }
    assertEquals(3, leadershipChangeCount.get());
  }

  private void simulateAnotherThread(CountDownLatch uberLatch) {
    new Thread(() -> {
      CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(),
          new ExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_RETRIES));
      client.start();
      clients.add(client);

      LeaderSelector selector = null;
      CountDownLatch latch = new CountDownLatch(1);

      try {
        SelectionCoordinator coordinator = new SelectionCoordinator(client);
        selector = coordinator.participateInSelection(SELECTION_PATH, new Participant() {
          @Override public String getParticipantName() {
            return "Client-3";
          }

          @Override public Consumer<CuratorFramework> getResponsibility() {
            return curatorFramework -> {
              System.out.printf("[Thread %s] %s is now the leader. Going to execute a long running task.%n",
                  Thread.currentThread().getName(), this.getParticipantName());
              System.out.printf("Number of leader selection so far %d%n", leadershipChangeCount.incrementAndGet());

              takeAPause(2, TimeUnit.SECONDS);

              latch.countDown();

            };
          }

          @Override public BiConsumer<CuratorFramework, ConnectionState> getHowToReactToStateChange() {
            return (curatorFramework, connectionState) -> System.out.println(
                this.getParticipantName() + " - current state after state change: [" + connectionState + "]");
          }
        });
        latch.await();
      } catch (Exception e) {
        System.err.println("Error in simulateAnotherThread: " + e.getMessage());
      } finally {
        CloseableUtils.closeQuietly(selector);
        uberLatch.countDown();
      }
    }).start();
  }

  private void simulateRelinquishingLeadership(CountDownLatch uberLatch) {
    new Thread(() -> {
      CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(),
          new ExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_RETRIES));
      client.start();
      clients.add(client);

      LeaderSelector selector = null;
      CountDownLatch latch = new CountDownLatch(1);

      try {
        SelectionCoordinator coordinator = new SelectionCoordinator(client);
        Participant participant = new Participant() {
          private volatile boolean shouldRelinquishResponsibility = false;

          @Override public String getParticipantName() {
            return "Client-2";
          }

          @Override public Consumer<CuratorFramework> getResponsibility() {
            return curatorFramework -> {
              System.out.printf("%n[Thread %s] %s is now the leader. Going to execute the leadership responsibility.%n",
                  Thread.currentThread().getName(), this.getParticipantName());
              System.out.printf("%nNumber of leader selection so far %d%n", leadershipChangeCount.incrementAndGet());

              stopMeAfterTimeout(100, TimeUnit.MILLISECONDS, this);

              String[] words = new String[] { "Quick", "Brown", "Fox", "Dog", "Hello", "World" };
              Random r = new Random();

              while (!shouldRelinquishResponsibility) {
                System.out.printf("[Thread %s] Current Process: %s, Word: %s%n", Thread.currentThread(),
                    this.getParticipantName(), words[r.nextInt(6)]);
              }

              latch.countDown();

            };
          }

          @Override public BiConsumer<CuratorFramework, ConnectionState> getHowToReactToStateChange() {
            return (curatorFramework, connectionState) -> System.out.println(
                this.getParticipantName() + " - current state after state change: [" + connectionState + "]");
          }

          @Override public synchronized void relinquishLeadershipResponsibility() {
            this.shouldRelinquishResponsibility = true;
          }
        };

        selector = coordinator.participateInSelection(SELECTION_PATH, participant);
        latch.await();

      } catch (Exception e) {
        System.err.println("Error in simulateRelinquishingLeadership: " + e.getMessage());
      } finally {
        CloseableUtils.closeQuietly(selector);
        uberLatch.countDown();
      }
    }).start();
  }

  private void simulateCrash(CountDownLatch uberLatch) {
    new Thread(() -> {
      CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(),
          new ExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_RETRIES));
      client.start();
      clients.add(client);

      LeaderSelector selector = null;
      CountDownLatch latch = new CountDownLatch(1);

      try {
        SelectionCoordinator coordinator = new SelectionCoordinator(client);
        Participant participant = new Participant() {
          @Override public String getParticipantName() {
            return "Client-1";
          }

          @Override public Consumer<CuratorFramework> getResponsibility() {
            return curatorFramework -> {
              System.out.printf("%n[Thread %s] %s is now the leader. Going to perform a long running task.%n",
                  Thread.currentThread().getName(), this.getParticipantName());
              System.out.printf("%nNumber of leader selection so far %d%n", leadershipChangeCount.incrementAndGet());

              try {
                interruptMeAfterTimeout(500, TimeUnit.MILLISECONDS, Thread.currentThread());
                takeAPause(2, TimeUnit.MINUTES);
              } finally {
                latch.countDown();
              }

            };
          }

          @Override public BiConsumer<CuratorFramework, ConnectionState> getHowToReactToStateChange() {
            return (curatorFramework, connectionState) -> System.out.println(
                this.getParticipantName() + " - current state after state change: [" + connectionState + "]");
          }

        };

        selector = coordinator.participateInSelection(SELECTION_PATH, participant);
        latch.await();
        throw new RuntimeException("Emulating a crash");
      } catch (Exception e) {
        System.err.println("Error in simulateCrash: " + e.getMessage());

      } finally {
        CloseableUtils.closeQuietly(selector);
        uberLatch.countDown();
      }

    }).start();
  }

  private void takeAPause(long timeout, TimeUnit timeUnit) {
    try {
      timeUnit.sleep(timeout);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void stopMeAfterTimeout(long timeout, TimeUnit timeUnit, Participant participant) {
    new Thread(() -> {
      takeAPause(timeout, timeUnit);
      participant.relinquishLeadershipResponsibility();
    }).start();
  }

  private void interruptMeAfterTimeout(long timeout, TimeUnit timeUnit, Thread thread) {
    new Thread(() -> {
      takeAPause(timeout, timeUnit);
      thread.interrupt();
    }).start();
  }

}