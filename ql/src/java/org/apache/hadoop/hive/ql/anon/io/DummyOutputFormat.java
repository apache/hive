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

package org.apache.hadoop.hive.ql.anon.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

@Deprecated
public class DummyOutputFormat extends FileOutputFormat<NullWritable, OrcStruct> {

  private static final Logger LOG = LoggerFactory.getLogger(DummyOutputFormat.class);

  @Override
  public RecordWriter<NullWritable, OrcStruct> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
    Configuration conf = job.getConfiguration();
    String path = conf.get("test.prop2");

    LOG.info("path from config: {}", path);
    path = path + "/tmp.orc." + new Random().nextInt(Integer.MAX_VALUE);
    String colNamesProp = conf.get(IOConstants.COLUMNS);
    String colTypesProp = conf.get(IOConstants.COLUMNS_TYPES);

    List<String> colNames = Arrays.stream(colNamesProp.split(",")).collect(Collectors.toList());
    TypeDescription schema = TypeDescription.createStruct();
    List<TypeInfo> typeInfos = TypeInfoUtils.getTypeInfosFromTypeString(colTypesProp);
    List<ObjectInspector> fois = new ArrayList<>();
    for (int i = 0; i < colNames.size(); ++i) {
      schema.addField(colNames.get(i), OrcInputFormat.convertTypeInfo(typeInfos.get(i)));
      fois.add(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfos.get(i)));
    }

    OrcFile.WriterOptions options = OrcFile.writerOptions(null, conf);
    options.setSchema(schema);

    StructObjectInspector soi = ObjectInspectorFactory.getStandardStructObjectInspector(colNames, fois);

    return new DummyRecordWriter(new Path(path), options, soi);
  }
}
