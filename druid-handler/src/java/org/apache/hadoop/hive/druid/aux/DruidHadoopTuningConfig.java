/**
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
package org.apache.hadoop.hive.druid.aux;

import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableMap;

import io.druid.indexer.HadoopTuningConfig;
import io.druid.indexer.HadoopyShardSpec;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.segment.IndexSpec;

/**
 * We create our own tuning config class because there is a conflict with
 * the variable names [maxRowsInMemory, rowFlushBoundary], and otherwise
 * there is a Jackson conflict while creating the JSON
 */
@JsonTypeName("hadoop")
public class DruidHadoopTuningConfig extends HadoopTuningConfig {

  @JsonCreator
  private DruidHadoopTuningConfig(final @JsonProperty("workingPath") String workingPath,
          final @JsonProperty("version") String version,
          final @JsonProperty("partitionsSpec") PartitionsSpec partitionsSpec,
          final @JsonProperty("shardSpecs") Map<DateTime, List<HadoopyShardSpec>> shardSpecs,
          final @JsonProperty("indexSpec") IndexSpec indexSpec,
          final @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
          final @JsonProperty("leaveIntermediate") boolean leaveIntermediate,
          final @JsonProperty("cleanupOnFailure") Boolean cleanupOnFailure,
          final @JsonProperty("overwriteFiles") boolean overwriteFiles,
          final @JsonProperty("ignoreInvalidRows") boolean ignoreInvalidRows,
          final @JsonProperty("jobProperties") Map<String, String> jobProperties,
          final @JsonProperty("combineText") boolean combineText,
          final @JsonProperty("useCombiner") Boolean useCombiner,
          final @JsonProperty("buildV9Directly") Boolean buildV9Directly,
          final @JsonProperty("numBackgroundPersistThreads") Integer numBackgroundPersistThreads) {
    super (workingPath, version, partitionsSpec, shardSpecs, indexSpec, maxRowsInMemory,
            leaveIntermediate, cleanupOnFailure, overwriteFiles, ignoreInvalidRows,
            jobProperties, combineText, useCombiner, null, buildV9Directly, numBackgroundPersistThreads);
  }

  public static DruidHadoopTuningConfig makeDefaultTuningConfig() {
    HadoopTuningConfig conf = HadoopTuningConfig.makeDefaultTuningConfig();
    // Important to avoid compatibility problems with Jackson versions
    Map<String,String> jobProperties =
            ImmutableMap.<String, String>of("mapreduce.job.user.classpath.first", "true");
    return new DruidHadoopTuningConfig(conf.getWorkingPath(), conf.getVersion(),
            conf.getPartitionsSpec(), conf.getShardSpecs(), conf.getIndexSpec(),
            conf.getRowFlushBoundary(), conf.isLeaveIntermediate(), conf.isCleanupOnFailure(),
            conf.isOverwriteFiles(), conf.isIgnoreInvalidRows(), jobProperties,
            conf.isCombineText(), conf.getUseCombiner(), conf.getBuildV9Directly(),
            conf.getNumBackgroundPersistThreads());
  }

}
