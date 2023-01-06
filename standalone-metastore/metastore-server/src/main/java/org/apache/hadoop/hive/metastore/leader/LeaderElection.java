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

package org.apache.hadoop.hive.metastore.leader;

import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;

/**
 * Interface for leader election.
 * As a result of leader election, an instance is changed to either leader
 * or follower(non-leader). Leader means that the instance has won the leader
 * election. To receive leadership change event, please register {@link LeadershipStateListener}
 * before election happens.
 * @param <T> the type of mutex
 */
public interface LeaderElection<T> extends Closeable {

  /**
   * Place where election happens
   * @param conf configuration
   * @param mutex a global mutex for leader election.
   *              It can be a path in Zookeeper, or a table that going to be locked.
   * @throws LeaderException
   */
  public void tryBeLeader(Configuration conf, T mutex)
      throws LeaderException;

  /**
   * Getting the result of election.
   * @return true if wins the election, false otherwise.
   */
  public boolean isLeader();

  /**
   * Register listeners to get notified when leadership changes.
   * @param listener The listener to be added
   */
  public void addStateListener(LeadershipStateListener listener);

  /**
   * Set the name of this leader candidate
   * @param name the name
   */
  public void setName(String name);

  /**
   * Get the name of this leader candidate
   */
  public String getName();

  public interface LeadershipStateListener {

    /**
     * Invoked when won the election.
     * @param election the election where happens.
     */
    public void takeLeadership(LeaderElection election) throws Exception;

    /**
     * Invoked when lost the election
     * @param election the election where happens.
     */
    public void lossLeadership(LeaderElection election) throws Exception;

  }

}
