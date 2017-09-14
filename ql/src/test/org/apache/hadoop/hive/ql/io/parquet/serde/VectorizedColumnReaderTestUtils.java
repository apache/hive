/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.io.parquet.serde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;
import java.util.List;

import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;

public class VectorizedColumnReaderTestUtils {
  public static VectorizedParquetRecordReader createParquetReader(
    String schemaString,
    Configuration conf,
    Path file) throws IOException, InterruptedException, HiveException {
    conf.set(PARQUET_READ_SCHEMA, schemaString);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, "//tmp");

    Job vectorJob = new Job(conf, "read vector");
    ParquetInputFormat.setInputPaths(vectorJob, file);
    ParquetInputFormat parquetInputFormat = new ParquetInputFormat(GroupReadSupport.class);
    InputSplit split = (InputSplit) parquetInputFormat.getSplits(vectorJob).get(0);
    initialVectorizedRowBatchCtx(conf);
    return new VectorizedParquetRecordReader(split, new JobConf(conf));
  }

  public static void initialVectorizedRowBatchCtx(Configuration conf) throws HiveException {
    MapWork mapWork = new MapWork();
    VectorizedRowBatchCtx rbCtx = new VectorizedRowBatchCtx();
    rbCtx.init(createStructObjectInspector(conf), new String[0]);
    mapWork.setVectorMode(true);
    mapWork.setVectorizedRowBatchCtx(rbCtx);
    Utilities.setMapWork(conf, mapWork);
  }

  private static StructObjectInspector createStructObjectInspector(Configuration conf) {
    // Create row related objects
    String columnNames = conf.get(IOConstants.COLUMNS);
    List<String> columnNamesList = DataWritableReadSupport.getColumnNames(columnNames);
    String columnTypes = conf.get(IOConstants.COLUMNS_TYPES);
    List<TypeInfo> columnTypesList = DataWritableReadSupport.getColumnTypes(columnTypes);
    TypeInfo rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNamesList, columnTypesList);
    return new ArrayWritableObjectInspector((StructTypeInfo) rowTypeInfo);
  }
}
