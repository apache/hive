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

import java.util.List;
import java.util.Map;

/**
 * UpdatePartitionColumnStatEvent
 * Event generated for partition column stat update event.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class UpdatePartitionColumnStatEvent extends ListenerEvent {
  private ColumnStatistics partColStats;
  private long writeId;
  private Map<String, String> parameters;
  private List<String> partVals;
  private Table tableObj;

  /**
   * @param statsObj Columns statistics Info.
   * @param partVals partition names
   * @param parameters table parameters to be updated after stats are updated.
   * @param tableObj table object
   * @param writeId writeId for the query.
   * @param handler handler that is firing the event
   */
  public UpdatePartitionColumnStatEvent(ColumnStatistics statsObj, List<String> partVals, Map<String, String> parameters,
                                        Table tableObj, long writeId, IHMSHandler handler) {
    super(true, handler);
    this.partColStats = statsObj;
    this.writeId = writeId;
    this.parameters = parameters;
    this.partVals = partVals;
    this.tableObj = tableObj;
  }

  /**
   * @param statsObj Columns statistics Info.
   * @param partVals partition names
   * @param handler handler that is firing the event
   */
  public UpdatePartitionColumnStatEvent(ColumnStatistics statsObj, List<String> partVals,
                                        Table tableObj, IHMSHandler handler) {
    super(true, handler);
    this.partColStats = statsObj;
    this.partVals = partVals;
    this.writeId = 0;
    this.parameters = null;
    this.tableObj = tableObj;
  }

  public ColumnStatistics getPartColStats() {
    return partColStats;
  }

  public long getWriteId() {
    return writeId;
  }

  public Map<String, String> getPartParameters() {
    return parameters;
  }

  public List<String> getPartVals() {
    return partVals;
  }

  public Table getTableObj() { return tableObj; }
}
