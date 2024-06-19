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

package org.apache.iceberg.mr.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.HiveIcebergStorageHandler;
import org.apache.iceberg.mr.hive.IcebergAcidUtil;

public abstract class AbstractIcebergRecordReader<T> extends RecordReader<Void, T> {

  private TaskAttemptContext context;
  private Configuration conf;
  private Table table;
  private Schema expectedSchema;
  private String nameMapping;
  private boolean reuseContainers;
  private boolean caseSensitive;
  private InputFormatConfig.InMemoryDataModel inMemoryDataModel;
  private boolean fetchVirtualColumns;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext newContext) {
    // For now IcebergInputFormat does its own split planning and does not accept FileSplit instances
    this.context = newContext;
    this.conf = newContext.getConfiguration();
    this.table = HiveIcebergStorageHandler.table(conf, conf.get(InputFormatConfig.TABLE_IDENTIFIER));
    HiveIcebergStorageHandler.checkAndSetIoConfig(conf, table);
    this.nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    this.caseSensitive = conf.getBoolean(InputFormatConfig.CASE_SENSITIVE, InputFormatConfig.CASE_SENSITIVE_DEFAULT);
    this.expectedSchema = readSchema(conf, table, caseSensitive);
    this.reuseContainers = conf.getBoolean(InputFormatConfig.REUSE_CONTAINERS, false);
    this.inMemoryDataModel = conf.getEnum(InputFormatConfig.IN_MEMORY_DATA_MODEL,
      InputFormatConfig.InMemoryDataModel.GENERIC);
    this.fetchVirtualColumns = InputFormatConfig.fetchVirtualColumns(conf);
  }

  private static Schema readSchema(Configuration conf, Table table, boolean caseSensitive) {
    Schema readSchema = InputFormatConfig.readSchema(conf);

    if (readSchema != null) {
      return readSchema;
    }

    String[] selectedColumns = InputFormatConfig.selectedColumns(conf);
    readSchema = table.schema();

    if (selectedColumns != null) {
      readSchema =
        caseSensitive ? readSchema.select(selectedColumns) : readSchema.caseInsensitiveSelect(selectedColumns);
    }

    if (InputFormatConfig.fetchVirtualColumns(conf)) {
      return IcebergAcidUtil.createFileReadSchemaWithVirtualColums(readSchema.columns(), table);
    }

    return readSchema;
  }

  CloseableIterable<T> applyResidualFiltering(CloseableIterable<T> iter, Expression residual,
                                              Schema readSchema) {
    boolean applyResidual = !getContext().getConfiguration()
        .getBoolean(InputFormatConfig.SKIP_RESIDUAL_FILTERING, false);

    if (applyResidual && residual != null && residual != Expressions.alwaysTrue()) {
      // Date and timestamp values are not the correct type for Evaluator.
      // Wrapping to return the expected type.
      InternalRecordWrapper wrapper = new InternalRecordWrapper(readSchema.asStruct());
      Evaluator filter = new Evaluator(readSchema.asStruct(), residual, caseSensitive);
      return CloseableIterable.filter(iter, record -> filter.eval(wrapper.wrap((StructLike) record)));
    } else {
      return iter;
    }
  }

  public TaskAttemptContext getContext() {
    return context;
  }

  public Configuration getConf() {
    return conf;
  }

  public boolean isReuseContainers() {
    return reuseContainers;
  }

  public Schema getExpectedSchema() {
    return expectedSchema;
  }

  public String getNameMapping() {
    return nameMapping;
  }

  public Table getTable() {
    return table;
  }

  public InputFormatConfig.InMemoryDataModel getInMemoryDataModel() {
    return inMemoryDataModel;
  }

  public boolean isFetchVirtualColumns() {
    return fetchVirtualColumns;
  }

  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  @Override
  public Void getCurrentKey() {
    return null;
  }

  @Override
  public float getProgress() {
    // TODO: We could give a more accurate progress based on records read from the file. Context.getProgress does not
    // have enough information to give an accurate progress value. This isn't that easy, since we don't know how much
    // of the input split has been processed and we are pushing filters into Parquet and ORC. But we do know when a
    // file is opened and could count the number of rows returned, so we can estimate. And we could also add a row
    // count to the readers so that we can get an accurate count of rows that have been either returned or filtered
    // out.
    return getContext().getProgress();
  }
}
