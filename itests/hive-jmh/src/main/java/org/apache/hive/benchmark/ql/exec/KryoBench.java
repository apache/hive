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
package org.apache.hive.benchmark.ql.exec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.TestInputOutputFormat.BigRowInspector;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorPartitionDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.google.common.io.ByteStreams;

@State(Scope.Benchmark)
public class KryoBench {

  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @State(Scope.Thread)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class BaseBench {

    private MapWork mapWork;

    @Setup
    public void setup() throws Exception {
      mapWork = KryoBench.mockMapWork("my_table", 1000, new BigRowInspector());
    }

    @Benchmark
    @Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
    public void testSerializeMapWork() {
      SerializationUtilities.serializePlan(mapWork, ByteStreams.nullOutputStream());
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder().include(".*" + KryoBench.class.getSimpleName() + ".*").build();
    new Runner(opt).run();
  }

  public static MapWork mockMapWork(String tableName, int partitions,
      ObjectInspector objectInspector) throws Exception {
    Path root = new Path("/warehouse", tableName);

    String[] partPath = new String[partitions];
    StringBuilder buffer = new StringBuilder();
    for (int p = 0; p < partitions; ++p) {
      partPath[p] = new Path(root, "p=" + p).toString();
      if (p != 0) {
        buffer.append(',');
      }
      buffer.append(partPath[p]);
    }
    StringBuilder columnIds = new StringBuilder();
    StringBuilder columnNames = new StringBuilder();
    StringBuilder columnTypes = new StringBuilder();
    StructObjectInspector structOI = (StructObjectInspector) objectInspector;
    List<? extends StructField> fields = structOI.getAllStructFieldRefs();
    int numCols = fields.size();
    for (int i = 0; i < numCols; ++i) {
      if (i != 0) {
        columnIds.append(',');
        columnNames.append(',');
        columnTypes.append(',');
      }
      columnIds.append(i);
      columnNames.append(fields.get(i).getFieldName());
      columnTypes.append(fields.get(i).getFieldObjectInspector().getTypeName());
    }

    Properties tblProps = new Properties();
    tblProps.put("name", tableName);
    tblProps.put("serialization.lib", OrcSerde.class.getName());
    tblProps.put("columns", columnNames.toString());
    tblProps.put("columns.types", columnTypes.toString());
    TableDesc tbl = new TableDesc(OrcInputFormat.class, OrcOutputFormat.class, tblProps);

    MapWork mapWork = new MapWork();
    mapWork.setVectorMode(true);

    VectorizedRowBatchCtx vectorizedRowBatchCtx = new VectorizedRowBatchCtx();
    vectorizedRowBatchCtx.init(structOI, new String[0]);
    mapWork.setVectorizedRowBatchCtx(vectorizedRowBatchCtx);

    mapWork.setUseBucketizedHiveInputFormat(false);
    Map<Path, List<String>> aliasMap = new LinkedHashMap<>();
    List<String> aliases = new ArrayList<String>();
    aliases.add(tableName);
    LinkedHashMap<Path, PartitionDesc> partMap = new LinkedHashMap<>();
    for (int p = 0; p < partitions; ++p) {
      Path path = new Path(partPath[p]);
      aliasMap.put(path, aliases);
      LinkedHashMap<String, String> partSpec = new LinkedHashMap<String, String>();
      PartitionDesc part = new PartitionDesc(tbl, partSpec);
      part.setVectorPartitionDesc(VectorPartitionDesc
          .createVectorizedInputFileFormat("MockInputFileFormatClassName", false, null));
      partMap.put(path, part);
    }
    mapWork.setPathToAliases(aliasMap);
    mapWork.setPathToPartitionInfo(partMap);

    return mapWork;
  }
}
