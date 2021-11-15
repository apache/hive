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
package org.apache.hadoop.hive.upgrade.acid;

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.List;

/**
 * Store result of database and table scan: compaction commands and meta info.
 */
public final class CompactTablesState {

  public static CompactTablesState empty() {
    return new CompactTablesState(emptyList(), new CompactionMetaInfo());
  }

  public static CompactTablesState compactions(List<String> compactionCommands, CompactionMetaInfo compactionMetaInfo) {
    return new CompactTablesState(compactionCommands, compactionMetaInfo);
  }

  private final List<String> compactionCommands;
  private final CompactionMetaInfo compactionMetaInfo;

  private CompactTablesState(List<String> compactionCommands, CompactionMetaInfo compactionMetaInfo) {
    this.compactionCommands = compactionCommands;
    this.compactionMetaInfo = compactionMetaInfo;
  }

  public List<String> getCompactionCommands() {
    return compactionCommands;
  }

  public CompactionMetaInfo getMetaInfo() {
    return compactionMetaInfo;
  }

  public CompactTablesState merge(CompactTablesState other) {
    List<String> compactionCommands = new ArrayList<>(this.compactionCommands);
    compactionCommands.addAll(other.compactionCommands);
    return new CompactTablesState(compactionCommands, this.compactionMetaInfo.merge(other.compactionMetaInfo));
  }
}
