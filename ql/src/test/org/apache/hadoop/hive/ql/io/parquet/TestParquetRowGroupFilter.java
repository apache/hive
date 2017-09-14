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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetFilterPredicateConverter;
import org.apache.hadoop.hive.ql.io.parquet.serde.VectorizedColumnReaderTestUtils;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestParquetRowGroupFilter extends AbstractTestParquetDirect {

  JobConf conf;
  String columnNames;
  String columnTypes;
  Path testPath;
  String schemaStr;
  MessageType fileSchema;
  StructObjectInspector inspector;

  @Before
  public void initConf() throws Exception {
    conf = new JobConf();
    // define schema
    columnNames = "intCol";
    columnTypes = "int";
    schemaStr =
      "message hive_schema {\n"
      + "  optional int32 intCol;\n"
      + "}\n";
    fileSchema = MessageTypeParser.parseMessageType(
      schemaStr
    );
    inspector = getObjectInspector(columnNames, columnTypes);

    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "intCol");
    conf.set("columns", "intCol");
    conf.set("columns.types", "int");

    // create Parquet file with specific data
    testPath = writeDirect("RowGroupFilterTakeEffect", fileSchema, new DirectWriter() {
        @Override
        public void write(RecordConsumer consumer) {
          for (int i = 0; i < 100; i++) {
            consumer.startMessage();
            consumer.startField("int", 0);
            consumer.addInteger(i);
            consumer.endField("int", 0);
            consumer.endMessage();
          }
        }
      });
  }

  @Test
  public void testRowGroupFilterTakeEffect() throws Exception {
    // > 50
    String searchArgumentStr = buildSearchArgumentStr(50);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, searchArgumentStr);

    ParquetRecordReaderWrapper recordReader = (ParquetRecordReaderWrapper)
        new MapredParquetInputFormat().getRecordReader(
        new FileSplit(testPath, 0, fileLength(testPath), (String[]) null), conf, null);

    Assert.assertEquals("row group is not filtered correctly", 1, recordReader.getFiltedBlocks().size());

    // > 100
    searchArgumentStr = buildSearchArgumentStr(100);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, searchArgumentStr);

    recordReader = (ParquetRecordReaderWrapper)
        new MapredParquetInputFormat().getRecordReader(
            new FileSplit(testPath, 0, fileLength(testPath), (String[]) null), conf, null);

    Assert.assertEquals("row group is not filtered correctly", 0, recordReader.getFiltedBlocks().size());
  }

  @Test
  public void testRowGroupFilterTakeEffectForVectorization() throws Exception {
    // > 50
    String searchArgumentStr = buildSearchArgumentStr(50);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, searchArgumentStr);
    SearchArgument sarg = ConvertAstToSearchArg.createFromConf(conf);
    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, fileSchema);

    ParquetInputFormat.setFilterPredicate(conf, p);

    VectorizedParquetRecordReader reader =
      VectorizedColumnReaderTestUtils.createParquetReader(schemaStr, conf, testPath);
    VectorizedRowBatch previous = reader.createValue();

    try {
      boolean hasValue =  reader.next(NullWritable.get(), previous);
      Assert.assertTrue("No row groups should be filtered.", hasValue);
    } finally {
      reader.close();
    }

    // > 100
    searchArgumentStr = buildSearchArgumentStr(100);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, searchArgumentStr);
    sarg = ConvertAstToSearchArg.createFromConf(conf);
    p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, fileSchema);
    ParquetInputFormat.setFilterPredicate(conf, p);

     reader =
       VectorizedColumnReaderTestUtils.createParquetReader(schemaStr, conf, testPath);
     previous = reader.createValue();
    try {
      boolean hasValue =  reader.next(NullWritable.get(), previous);
      Assert.assertFalse("Row groups should be filtered.", hasValue);
    } finally {
      reader.close();
    }
  }


  /**
   * Build the search argument string great than constant
   *
   * @return searchArgumentStr
   */
  private String buildSearchArgumentStr(int constant){
    GenericUDF udf = new GenericUDFOPGreaterThan();
    List<ExprNodeDesc> children = Lists.newArrayList();
    ExprNodeColumnDesc columnDesc = new ExprNodeColumnDesc(Integer.class, "intCol", "T", false);
    ExprNodeConstantDesc constantDesc = new ExprNodeConstantDesc(constant);
    children.add(columnDesc);
    children.add(constantDesc);
    ExprNodeGenericFuncDesc genericFuncDesc = new ExprNodeGenericFuncDesc(inspector, udf, children);
    return SerializationUtilities.serializeExpression(genericFuncDesc);
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
