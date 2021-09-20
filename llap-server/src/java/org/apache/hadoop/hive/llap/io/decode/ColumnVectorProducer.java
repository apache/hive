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

package org.apache.hadoop.hive.llap.io.decode;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.SchemaEvolution;

/**
 * Entry point used by LlapInputFormat to create read pipeline to get data.
 */
public interface ColumnVectorProducer {
  public interface SchemaEvolutionFactory {
    SchemaEvolution createSchemaEvolution(TypeDescription fileSchema);
  }

  public interface Includes {
    boolean[] generateFileIncludes(TypeDescription fileSchema);
    List<Integer> getPhysicalColumnIds();
    List<Integer> getReaderLogicalColumnIds();
    TypeDescription[] getBatchReaderTypes(TypeDescription fileSchema);
    String[] getOriginalColumnNames(TypeDescription fileSchema);
    String getQueryId();
    boolean isProbeDecodeEnabled();
    byte getProbeMjSmallTablePos();
    String getProbeCacheKey();
    String getProbeColName();
    int getProbeColIdx();
    List<Integer> getLogicalOrderedColumnIds();
  }

  ReadPipeline createReadPipeline(Consumer<ColumnVectorBatch> consumer, FileSplit split,
      Includes includes, SearchArgument sarg, QueryFragmentCounters counters,
      SchemaEvolutionFactory sef, InputFormat<?, ?> sourceInputFormat, Deserializer sourceSerDe,
      Reporter reporter, JobConf job, Map<Path, PartitionDesc> parts) throws IOException;
}
