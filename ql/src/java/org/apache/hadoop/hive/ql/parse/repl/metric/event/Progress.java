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
package org.apache.hadoop.hive.ql.parse.repl.metric.event;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class for defining the progress info for replication metrics.
 */
public class Progress {

  @JsonProperty()
  private Status status;

  @JsonProperty("stages")
  private Map<String, Stage> stages = new ConcurrentHashMap<>();

  public Progress() {

  }

  public Progress(Progress progress) {
    this.status = progress.status;
    for (Stage value : progress.stages.values()) {
      Stage stage = new Stage(value);
      this.stages.put(stage.getName(), stage);
    }
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public void addStage(Stage stage) {
    stages.putIfAbsent(stage.getName(), stage);
  }

  public Stage getStageByName(String stageName) {
    return stages.get(stageName);
  }

  public List<Stage> getStages() {
    return new ArrayList<>(stages.values());
  }

}
