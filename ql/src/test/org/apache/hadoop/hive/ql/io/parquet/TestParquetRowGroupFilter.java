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

package org.apache.hadoop.hive.ql.io.parquet;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestParquetRowGroupFilter extends AbstractTestParquetDirect {

  JobConf conf;
  String columnNames;
  String columnTypes;

  @Before
  public void initConf() throws Exception {
    conf = new JobConf();

  }

  @Test
  public void testRowGroupFilterTakeEffect() throws Exception {
    // define schema
    columnNames = "intCol";
    columnTypes = "int";
    StructObjectInspector inspector = getObjectInspector(columnNames, columnTypes);
    MessageType fileSchema = MessageTypeParser.parseMessageType(
        "message hive_schema {\n"
            + "  optional int32 intCol;\n"
            + "}\n"
    );

    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "intCol");
    conf.set("columns", "intCol");
    conf.set("columns.types", "int");

    // create Parquet file with specific data
    Path testPath = writeDirect("RowGroupFilterTakeEffect", fileSchema,
        new DirectWriter() {
          @Override
          public void write(RecordConsumer consumer) {
            for(int i = 0; i < 100; i++) {
              consumer.startMessage();
              consumer.startField("int", 0);
              consumer.addInteger(i);
              consumer.endField("int", 0);
              consumer.endMessage();
            }
          }
        });

    // > 50
    GenericUDF udf = new GenericUDFOPGreaterThan();
    List<ExprNodeDesc> children = Lists.newArrayList();
    ExprNodeColumnDesc columnDesc = new ExprNodeColumnDesc(Integer.class, "intCol", "T", false);
    ExprNodeConstantDesc constantDesc = new ExprNodeConstantDesc(50);
    children.add(columnDesc);
    children.add(constantDesc);
    ExprNodeGenericFuncDesc genericFuncDesc = new ExprNodeGenericFuncDesc(inspector, udf, children);
    String searchArgumentStr = Utilities.serializeExpression(genericFuncDesc);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, searchArgumentStr);

    ParquetRecordReaderWrapper recordReader = (ParquetRecordReaderWrapper)
        new MapredParquetInputFormat().getRecordReader(
        new FileSplit(testPath, 0, fileLength(testPath), (String[]) null), conf, null);

    Assert.assertEquals("row group is not filtered correctly", 1, recordReader.getFiltedBlocks().size());

    // > 100
    constantDesc = new ExprNodeConstantDesc(100);
    children.set(1, constantDesc);
    genericFuncDesc = new ExprNodeGenericFuncDesc(inspector, udf, children);
    searchArgumentStr = Utilities.serializeExpression(genericFuncDesc);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, searchArgumentStr);

    recordReader = (ParquetRecordReaderWrapper)
        new MapredParquetInputFormat().getRecordReader(
            new FileSplit(testPath, 0, fileLength(testPath), (String[]) null), conf, null);

    Assert.assertEquals("row group is not filtered correctly", 0, recordReader.getFiltedBlocks().size());
  }

  private ArrayWritableObjectInspector getObjectInspector(final String columnNames, final String columnTypes) {
    List<TypeInfo> columnTypeList = createHiveTypeInfoFrom(columnTypes);
    List<String> columnNameList = createHiveColumnsFrom(columnNames);
    StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNameList, columnTypeList);

    return new ArrayWritableObjectInspector(rowTypeInfo);
  }

  private List<String> createHiveColumnsFrom(final String columnNamesStr) {
    List<String> columnNames;
    if (columnNamesStr.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNamesStr.split(","));
    }

    return columnNames;
  }

  private List<TypeInfo> createHiveTypeInfoFrom(final String columnsTypeStr) {
    List<TypeInfo> columnTypes;

    if (columnsTypeStr.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnsTypeStr);
    }

    return columnTypes;
  }
}
