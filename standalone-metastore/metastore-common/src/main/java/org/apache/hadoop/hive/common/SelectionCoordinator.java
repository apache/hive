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
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;

import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * This is an abstraction over @{@link LeaderSelector} that enables participants to participate in 'Leader Selection'
 * process in a distributed environment where there could be multiple similar contenders. Essentially it depends on
 * a fault-tolerant distributed consensus algorithm or Atomic Broadcast. Here we are relying on Zookeeper's Atomic
 * Broadcast protocol under through.
 * <p>
 * There are well defined <a href="https://zookeeper.apache.org/doc/current/recipes.html#sc_leaderElection">recipes</a>
 * to use Zookeeper to implement higher order functions, i.e. Leader Election. Apache Curator has already implemented
 * all the recipes, please refer here: https://curator.apache.org/curator-recipes/index.html
 * </p>
 * <p>
 * Under the hood we are relying on Apache Curator's implementation of Leader Election recipe. In this implementation,
 * once a leader is selected, the next election won't happen until the leader relinquishes its responsibility or the
 * leader process is crashed or killed. Please note that this algorithm is 'fair', each participant will become leader
 * in the same order Zookeeper received the participation requests.
 * </p>
 * <p>
 * In a distributed environment, we often need to select one node as leader to coordinate the execution of a
 * complex job.
 * </p>
 */
public class SelectionCoordinator {
  private final CuratorFramework client;

  /**
   * Instantiates a Selection Coordinator
   *
   * @param client this is the client to interact with Zookeeper
   */
  public SelectionCoordinator(CuratorFramework client) {
    this.client = client;
  }

  /**
   * The {@code executorService} defaults to null here.
   * Otherwise, it is just another overloaded version of
   * {@link SelectionCoordinator#participateInSelection(String, Participant, ExecutorService)}.
   *
   * @see SelectionCoordinator#participateInSelection(String, Participant, ExecutorService)
   */
  public LeaderSelector participateInSelection(String atPath, Participant participant) {
    return participateInSelection(atPath, participant, null);
  }

  /**
   * Enables a {@link Participant} to contest for an election at the given path.
   *
   * @param atPath the path for this leadership group, please note that each selection process should have a
   *               dedicated path. This represents a node in Zookeeper's storage.
   * @param participant a concrete implementation of {@link Participant}
   * @param executorService the {@link ExecutorService} that would be used to execute the callbacks or change
   *                        notifications, if null, the executor service would be managed by Curator
   * @return the {@link LeaderSelector} object
   */
  public LeaderSelector participateInSelection(String atPath, Participant participant,
      ExecutorService executorService) {
    LeaderSelectorListener listener = new LeaderSelectorListener() {
      @Override public void takeLeadership(CuratorFramework client) {
        Consumer<CuratorFramework> responsibility = participant.getResponsibility();
        responsibility.accept(client);
      }

      @Override public void stateChanged(CuratorFramework client, ConnectionState newState) {
        BiConsumer<CuratorFramework, ConnectionState> reactor = participant.getHowToReactToStateChange();
        reactor.accept(client, newState);
      }
    };

    LeaderSelector leaderSelector =
        (executorService != null) ? new LeaderSelector(client, atPath, executorService, listener) : new LeaderSelector(
            client, atPath, listener);

    leaderSelector.start();

    return leaderSelector;
  }
}