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
package org.apache.hadoop.hive.druid;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.aux.DruidHadoopTuningConfig;
import org.apache.hadoop.hive.druid.serde.DruidSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.optimizer.calcite.druid.DruidTable;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;

import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.indexer.HadoopIOConfig;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexing.common.task.HadoopIndexTask;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.column.Column;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import io.druid.segment.indexing.granularity.GranularitySpec;

/**
 * DruidStorageHandler provides a HiveStorageHandler implementation for Druid.
 */
@SuppressWarnings({"deprecation","rawtypes"})
public class DruidStorageHandler extends DefaultStorageHandler implements HiveMetaHook {

  protected static final Logger LOG = LoggerFactory.getLogger(DruidStorageHandler.class);

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveDruidQueryBasedInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    final String name = HiveConf.getVar(getConf(),
            HiveConf.ConfVars.HIVE_DRUID_OUTPUT_FORMAT);
    if (name.equalsIgnoreCase(IOConstants.ORC)) {
      return OrcOutputFormat.class; // ORC
    }
    return HiveIgnoreKeyTextOutputFormat.class; // Textfile
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return DruidSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public void preCreateTable(Table table) throws MetaException {
    // Do safety checks
    if (MetaStoreUtils.isExternalTable(table) && !StringUtils.isEmpty(table.getSd().getLocation())) {
      throw new MetaException("LOCATION may not be specified for Druid existing sources");
    }
    if (table.getPartitionKeysSize() != 0) {
      throw new MetaException("PARTITIONED BY may not be specified for Druid");
    }
    if (table.getSd().getBucketColsSize() != 0) {
      throw new MetaException("CLUSTERED BY may not be specified for Druid");
    }
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    // Remove results from Hive
    final Warehouse wh = new Warehouse(getConf());
    final String location = table.getSd().getLocation();
    boolean success = false;
    if (location != null) {
      success = wh.deleteDir(new Path(location), true);
    }
    if (!success) {
      if (LOG.isErrorEnabled()) {
        LOG.error("Could not remove Hive results for Druid indexing");
      }
    } else {
      if (LOG.isErrorEnabled()) {
        LOG.error("Rollback due to error; removed Hive results for Druid indexing");
      }
    }
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    if (MetaStoreUtils.isExternalTable(table)) {
      // Nothing to do
      return;
    }
    final Warehouse wh = new Warehouse(getConf());
    final String location = table.getSd().getLocation();
    try {
      if (StringUtils.isEmpty(location) || wh.isEmpty(new Path(location))) {
        if (LOG.isInfoEnabled()) {
          LOG.info("No need to move data to Druid");
        }
        return;
      }
    } catch (IOException e) {
      throw new MetaException(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }

    // Take results and format
    final String dataSource = table.getParameters().get(Constants.DRUID_DATA_SOURCE);
    if (dataSource == null) {
      throw new MetaException("Druid data source not specified; use " +
              Constants.DRUID_DATA_SOURCE +
              " in table properties");
    }
    final String name = HiveConf.getVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_OUTPUT_FORMAT);

    // Create indexing task
    final HadoopIndexTask indexTask = createHadoopIndexTask(table, dataSource, location, name);

    // Submit indexing task to Druid and retrieve results
    String address = HiveConf.getVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_OVERLORD_DEFAULT_ADDRESS);
    if (org.apache.commons.lang3.StringUtils.isEmpty(address)) {
      throw new MetaException("Druid overlord address not specified in configuration");
    }
    HttpClient client = HttpClientInit.createClient(HttpClientConfig.builder().build(), new Lifecycle());
    InputStream response;
    String taskId;
    try {
      response = DruidStorageHandlerUtils.submitRequest(client,
              DruidStorageHandlerUtils.createTaskRequest(address, indexTask));
      taskId = DruidStorageHandlerUtils.JSON_MAPPER.readValue(response, JsonNode.class)
              .get("task").textValue();
    } catch (IOException e) {
      throw new MetaException(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
    if (taskId == null) {
      throw new MetaException("Connected to Druid but task ID is null");
    }

    // Monitor the indexing task
    final long initialSleepTime = HiveConf.getLongVar(getConf(),
            HiveConf.ConfVars.HIVE_DRUID_INDEXING_INITIAL_SLEEP_TIME);
    final long timeout = HiveConf.getLongVar(getConf(),
            HiveConf.ConfVars.HIVE_DRUID_INDEXING_TIMEOUT);
    final long sleepTime = HiveConf.getLongVar(getConf(),
            HiveConf.ConfVars.HIVE_DRUID_INDEXING_SLEEP_TIME);
    final LogHelper console = new LogHelper(LOG);
    console.printInfo("Indexing data in Druid - TaskId: " + taskId);
    JsonNode statusResponse;
    try {
      statusResponse = DruidStorageHandlerUtils.monitorTask(client, address, taskId,
              initialSleepTime, timeout, sleepTime);
    } catch (Exception e) {
      if (LOG.isErrorEnabled()) {
        LOG.error("Exception in Druid indexing task");
      }
      throw new MetaException(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }

    // Provide feedback
    if (DruidStorageHandlerUtils.isComplete(statusResponse)) {
      // It finished: either it succeeded or failed
      if (DruidStorageHandlerUtils.isSuccess(statusResponse)) {
        // Success
        console.printInfo("Finished indexing data in Druid - TaskId: " + taskId +
                ", duration: " + DruidStorageHandlerUtils.extractDurationFromResponse(statusResponse) + "ms");
      } else {
        // Fail
        throw new MetaException("Error storing data in Druid - TaskId: " + taskId +
                ". Check Druid logs for more information");
      }
    } else {
      // Still running, we hit the timeout, shutdown the task and bail out
      if (LOG.isWarnEnabled()) {
        LOG.warn("Timeout exceeeded: shutting down Druid indexing task...");
      }
      try {
        DruidStorageHandlerUtils.submitRequest(client,
                DruidStorageHandlerUtils.createTaskShutdownRequest(address, taskId));
      } catch (IOException e) {
        throw new MetaException(org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
      throw new MetaException("Timeout exceeeded - TaskId: " + taskId +
              ". Data storage operation in Druid not completed");
    }

    // Remove results from Hive
    boolean success = wh.deleteDir(new Path(location), true);
    if (!success) {
      if (LOG.isErrorEnabled()) {
        LOG.error("Could not remove Hive results for Druid indexing");
      }
    }
  }

  private HadoopIndexTask createHadoopIndexTask(Table table, String dataSource, String location,
          String name) throws MetaException {
    // Create data schema specification (including parser specification)
    final Map<String, Object> parserMap;
    if (name.equalsIgnoreCase(IOConstants.ORC)) { // ORC
      throw new UnsupportedOperationException("Currently reading from ORC is not supported;"
              + "Druid version needs to be upgraded to 0.9.2");
    } else { // CSV
      // Default
      TimestampSpec timestampSpec = new TimestampSpec(Column.TIME_COLUMN_NAME,
          "yyyy-MM-dd HH:mm:ss", null);
      // Default, all columns that are not metrics or timestamp, are treated as dimensions
      DimensionsSpec dimensionsSpec = new DimensionsSpec(null, null, null);
      // Add column names for the Spec, so it knows which column corresponds to each name
      List<String> columns = new ArrayList<>();
      for (FieldSchema f : table.getSd().getCols()) {
        columns.add(f.getName());
      }
      ParseSpec parseSpec = new CSVParseSpec(timestampSpec, dimensionsSpec, null, columns);
      InputRowParser parser = new StringInputRowParser(parseSpec, null);
      try {
        parserMap = DruidStorageHandlerUtils.JSON_MAPPER.readValue(
                DruidStorageHandlerUtils.JSON_MAPPER.writeValueAsString(parser),
                new TypeReference<Map<String, Object>>(){});
      } catch (Exception e) {
        throw new MetaException(org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    }
    // Metrics
    final List<AggregatorFactory> aggregators = new ArrayList<>();
    for (FieldSchema f : table.getSd().getCols()) {
      AggregatorFactory af;
      switch (f.getType()) {
      case serdeConstants.TINYINT_TYPE_NAME:
      case serdeConstants.SMALLINT_TYPE_NAME:
      case serdeConstants.INT_TYPE_NAME:
      case serdeConstants.BIGINT_TYPE_NAME:
        af = new LongSumAggregatorFactory(f.getName(), f.getName());
        break;
      case serdeConstants.FLOAT_TYPE_NAME:
      case serdeConstants.DOUBLE_TYPE_NAME:
        af = new DoubleSumAggregatorFactory(f.getName(), f.getName());
        break;
      default:
        // Dimension or timestamp
        continue;
      }
      aggregators.add(af);
    }
    final GranularitySpec granularitySpec =
            new ArbitraryGranularitySpec(QueryGranularity.fromString("NONE"),
                    ImmutableList.of(DruidTable.DEFAULT_INTERVAL));
    final DataSchema dataSchema = new DataSchema(dataSource, parserMap,
            aggregators.toArray(new AggregatorFactory[aggregators.size()]),
            granularitySpec, DruidStorageHandlerUtils.JSON_MAPPER);
    Map<String, Object> pathSpec = new HashMap<>();
    pathSpec.put("type", "static");
    pathSpec.put("paths", location);
    if (name.equalsIgnoreCase(IOConstants.ORC)) {
      pathSpec.put("inputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat");
    }
    HadoopIOConfig ioConfig = new HadoopIOConfig(pathSpec, null, null);
    HadoopIngestionSpec spec = new HadoopIngestionSpec(dataSchema, ioConfig,
            DruidHadoopTuningConfig.makeDefaultTuningConfig(), null);
    return new HadoopIndexTask(null, spec, null, null, null, DruidStorageHandlerUtils.JSON_MAPPER, null);
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    // Nothing to do
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // Nothing to do
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
    // Nothing to do
  }

  @Override
  public String toString() {
    return Constants.DRUID_HIVE_STORAGE_HANDLER_ID;
  }

}
