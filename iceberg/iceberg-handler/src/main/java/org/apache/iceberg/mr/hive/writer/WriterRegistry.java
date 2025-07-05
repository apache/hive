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
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class WriterRegistry {
  private static final Map<WriterKey, Map<String, List<HiveIcebergWriter>>> writers = Maps.newConcurrentMap();

  private WriterRegistry() {
  }

  public static Map<String, List<HiveIcebergWriter>> removeWriters(TaskAttemptID taskAttemptID, String outputId) {
    return writers.remove(new WriterKey(taskAttemptID, outputId));
  }

  public static void registerWriter(TaskAttemptID taskAttemptID,
                                    String outputId,
                                    String tableName,
                                    HiveIcebergWriter writer) {
    WriterKey key = new WriterKey(taskAttemptID, outputId);
    writers
        .computeIfAbsent(key, k -> Maps.newConcurrentMap())
        .computeIfAbsent(tableName, k -> new CopyOnWriteArrayList<>())
        .add(writer);
  }

  public static Map<String, List<HiveIcebergWriter>> writers(TaskAttemptID taskAttemptID, String outputId) {
    return writers.get(new WriterKey(taskAttemptID, outputId));
  }

  static class WriterKey {
    private final TaskAttemptID taskAttemptID;
    private final String outputId;

    WriterKey(TaskAttemptID taskAttemptID, String outputId) {
      this.taskAttemptID = taskAttemptID;
      this.outputId = outputId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      WriterKey writerKey = (WriterKey) o;
      return taskAttemptID.equals(writerKey.taskAttemptID) && Objects.equals(outputId, writerKey.outputId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(taskAttemptID, outputId);
    }
  }
}
