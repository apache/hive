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

import java.io.Serializable;
import java.util.Set;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;

/**
 * Client-side stats aggregator task.
 */
public class BasicStatsNoJobWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private TableSpec tableSpecs;
  private boolean statsReliable;

  private Set<Partition> pp;

  public BasicStatsNoJobWork(TableSpec tableSpecs) {
    this.tableSpecs = tableSpecs;
  }

  public TableSpec getTableSpecs() {
    return tableSpecs;
  }

  public void setStatsReliable(boolean s1) {
    statsReliable = s1;
  }

  public boolean isStatsReliable() {
    return statsReliable;
  }

  public Set<Partition> getPartitions() {
    return pp;
  }

  public void setPartitions(Set<Partition> partitions) {
    pp = partitions;

  }

}
