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

package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * UpdatePartitionColumnStatEventBatch
 * Event generated for a batch of partition column stat update event.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class UpdatePartitionColumnStatEventBatch extends ListenerEvent {
  private List<UpdatePartitionColumnStatEvent> eventList = new ArrayList<>();

  /**
   * @param eventList List of events to update the partition column statistic.
   * @param handler handler that is firing the event
   */
  public UpdatePartitionColumnStatEventBatch(List<UpdatePartitionColumnStatEvent> eventList, IHMSHandler handler) {
    super(true, handler);
    this.eventList.addAll(eventList);
  }

  /**
   * @param handler handler that is firing the event
   */
  public UpdatePartitionColumnStatEventBatch(IHMSHandler handler) {
    super(true, handler);
  }

  public void addPartColStatEvent(UpdatePartitionColumnStatEvent event) {
    eventList.add(event);
  }

  public UpdatePartitionColumnStatEvent getPartColStatEvent(int idx) {
    return eventList.get(idx);
  }

  public int getNumEntries() {
    return eventList.size();
  }
}
