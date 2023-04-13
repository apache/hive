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
import org.apache.curator.framework.state.ConnectionState;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * This represents a participant or contestant for a Leader Selection at a given path, i.e. Leadership Group.
 * <p>
 * This also defines a bunch of important aspects of a participant, i.e., name of the participant, responsibilities
 * if elected as the Leader, how to react to state change, how to relinquish leadership after being selected as
 * a leader.
 * </p>
 * <p>
 * Please note, if the assumed responsibilities are short/transient, the leadership is automatically forgone once the
 * responsibilities are taken care of, or if the process/thread (aka participant) crashes or is killed.
 * <br> If the responsibilities are long running or the leader does not willingly relinquish the power, external
 * entities can request for the same, although it's upto the individual Leader whether to consider such requests.
 * </p>
 *
 */
public interface Participant {
  /**
   * This returns the Participant's name or identifier
   *
   * @return participant's name or identifier
   */
  String getParticipantName();

  /**
   * Defines the responsibilities if the participant is elected as the Leader.
   *
   * @return Responsibilities or the tasks to be executed if elected as the leader, we need to define a {@link Consumer}
   * that has the {@link CuratorFramework} at its disposal, it should define the tasks
   */
  Consumer<CuratorFramework> getResponsibility();

  /**
   * This defines how to react to a connection state change notification.
   *
   * @return A {@link BiConsumer} implementation that accepts the {@link CuratorFramework} instance and the
   * new {@link ConnectionState} and decides how to react to this change
   */
  BiConsumer<CuratorFramework, ConnectionState> getHowToReactToStateChange();

  /**
   * This sends a signal or request to the Leader to relinquish leadership.
   * <br>It's upto the leader whether to honour such requests.
   */
  default void relinquishLeadershipResponsibility() {
    throw new UnsupportedOperationException();
  }

}
