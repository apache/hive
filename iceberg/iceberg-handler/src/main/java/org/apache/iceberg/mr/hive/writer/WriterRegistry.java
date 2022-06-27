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

package org.apache.iceberg.mr.hive.writer;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class WriterRegistry {
  private static final Map<TaskAttemptID, Map<String, List<HiveIcebergWriter>>> writers = Maps.newConcurrentMap();

  private WriterRegistry() {
  }

  public static Map<String, List<HiveIcebergWriter>> removeWriters(TaskAttemptID taskAttemptID) {
    return writers.remove(taskAttemptID);
  }

  public static void registerWriter(TaskAttemptID taskAttemptID, String tableName, HiveIcebergWriter writer) {
    writers.putIfAbsent(taskAttemptID, Maps.newConcurrentMap());

    Map<String, List<HiveIcebergWriter>> writersOfTableMap = writers.get(taskAttemptID);
    writersOfTableMap.putIfAbsent(tableName, Lists.newArrayList());

    List<HiveIcebergWriter> writerList = writersOfTableMap.get(tableName);
    writerList.add(writer);
  }

  public static Map<String, List<HiveIcebergWriter>> writers(TaskAttemptID taskAttemptID) {
    return writers.get(taskAttemptID);
  }
}
