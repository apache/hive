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
package org.apache.hadoop.hive.metastore.handler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.PreReadTableEvent;
import org.apache.hadoop.hive.metastore.utils.FilterUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.ExceptionHandler.handleException;
import static org.apache.hadoop.hive.metastore.HMSHandler.PARTITION_NUMBER_EXCEED_LIMIT_MSG;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

// Collect get partitions APIs together
@SuppressWarnings({"unchecked", "rawtypes"})
@RequestHandler(requestBody = GetPartitionsHandler.GetPartitionsRequest.class)
public class GetPartitionsHandler<T> extends AbstractRequestHandler<GetPartitionsHandler.GetPartitionsRequest,
    GetPartitionsHandler.GetPartitionsResult<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(GetPartitionsHandler.class);
  private static final String NO_FILTER_STRING = "";
  private RawStore rs;
  private String catName;
  private String dbName;
  private String tblName;
  private GetPartitionsArgs args;
  private Table table;
  private Configuration conf;
  private GetPartitionsMethod getMethod;
  private MetaStoreFilterHook filterHook;
  private boolean isServerFilterEnabled;

  enum GetPartitionsMethod {
    EXPR, NAMES, FILTER, PART_VALS, ALL, VALUES
  }

  GetPartitionsHandler(IHMSHandler handler, GetPartitionsRequest request) {
    super(handler, false, request);
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    this.args = request.getGetPartitionsArgs();
    if (request.isGetPartitionValues()) {
      getMethod = GetPartitionsMethod.VALUES;
    } else if (args.getExpr() != null) {
      getMethod = GetPartitionsMethod.EXPR;
    } else if (args.getFilter() != null) {
      getMethod = GetPartitionsMethod.FILTER;
    } else if (args.getPartNames() != null) {
      getMethod = GetPartitionsMethod.NAMES;
    } else if (args.getPart_vals() != null) {
      getMethod = GetPartitionsMethod.PART_VALS;
    } else {
      getMethod = GetPartitionsMethod.ALL;
    }
    
    this.catName = normalizeIdentifier(request.getTableName().getCat());
    this.dbName = normalizeIdentifier(request.getTableName().getDb());
    this.tblName = normalizeIdentifier(request.getTableName().getTable());
    this.conf = handler.getConf();
    this.rs = handler.getMS();
    this.filterHook = handler.getMetaFilterHook();
    this.isServerFilterEnabled = filterHook != null;
    GetTableRequest getTableRequest = new GetTableRequest(dbName, tblName);
    getTableRequest.setCatName(catName);
    this.table = handler.get_table_core(getTableRequest);
    ((HMSHandler) handler).firePreEvent(new PreReadTableEvent(table, handler));
    authorizeTableForPartitionMetadata();

    LOG.info("Starting to get {} of {} using {}", request.isFetchPartNames() ? "partition names" : "partitions",
        TableName.getQualified(catName, dbName, tblName), getMethod);
  }

  @Override
  protected GetPartitionsResult<T> execute() throws TException, IOException {
    return (GetPartitionsResult<T>) switch (getMethod) {
      case EXPR -> getPartitionsByExpr();
      case FILTER -> getPartitionsByFilter();
      case NAMES -> getPartitionsByNames();
      case PART_VALS -> getPartitionsByVals();
      case ALL -> getPartitions();
      case VALUES -> getPartitionValues();
    };
  }

  private GetPartitionsResult getPartitionsByVals() throws TException {
    if (request.isFetchPartNames()) {
      List<String> ret = rs.listPartitionNamesPs(catName, dbName, tblName,
          args.getPart_vals(), (short) args.getMax());
      ret = FilterUtils.filterPartitionNamesIfEnabled(isServerFilterEnabled,
          filterHook, catName, dbName, tblName, ret);
      return new GetPartitionsResult<>(ret, true);
    } else {
      List<Partition> ret;
      if (args.getPart_vals() != null) {
        checkLimitNumberOfPartitionsByPs(args.getPart_vals(), args.getMax());
      } else {
        checkLimitNumberOfPartitionsByFilter(NO_FILTER_STRING, args.getMax());
      }
      ret = rs.listPartitionsPsWithAuth(catName, dbName, tblName, args);
      return new GetPartitionsResult(ret, true);
    }
  }

  private GetPartitionsResult getPartitionValues() throws MetaException {
    PartitionValuesResponse resp = rs.listPartitionValues(catName, dbName, tblName, request.getPartitionKeys(),
        request.isApplyDistinct(), args.getFilter(), request.isAscending(),
        request.getPartitionOrders(), args.getMax());
    return new GetPartitionsResult<>(Arrays.asList(resp), true);
  }

  private void checkLimitNumberOfPartitionsByPs(List<String> partVals, int requestMax)
      throws TException {
    if (exceedsPartitionFetchLimit(requestMax)) {
      checkLimitNumberOfPartitions(tblName, rs.getNumPartitionsByPs(catName, dbName, tblName,
          partVals));
    }
  }

  private GetPartitionsResult<Partition> getPartitionsByFilter() throws TException {
    List<Partition> ret = null;
    if (exceedsPartitionFetchLimit(args.getMax())) {
      // Since partition limit is configured, we need fetch at most (limit + 1) partition names
      int max = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.LIMIT_PARTITION_REQUEST) + 1;
      args = new GetPartitionsArgs.GetPartitionsArgsBuilder(args).max(max).build();
      List<String> partNames = rs.listPartitionNamesByFilter(catName, dbName, tblName, args);
      checkLimitNumberOfPartitions(tblName, partNames.size());
      ret = rs.getPartitionsByNames(catName, dbName, tblName,
          new GetPartitionsArgs.GetPartitionsArgsBuilder(args).partNames(partNames).build());
    } else {
      ret = rs.getPartitionsByFilter(catName, dbName, tblName, args);
    }

    ret = FilterUtils.filterPartitionsIfEnabled(isServerFilterEnabled, filterHook, ret);
    return new GetPartitionsResult<>(ret, true);
  }

  /**
   * Check if user can access the table associated with the partition. If not, then throw exception
   * so user cannot access partitions associated with this table
   * We are not calling Pre event listener for authorization because it requires getting the
   * table object from DB, more overhead. Instead ,we call filter hook to filter out table if user
   * has no access. Filter hook only requires table name, not table object. That saves DB access for
   * table object, and still achieve the same purpose: checking if user can access the specified
   * table
   *
   * @throws NoSuchObjectException
   * @throws MetaException
   */
  private void authorizeTableForPartitionMetadata()
      throws NoSuchObjectException, MetaException {
    FilterUtils.checkDbAndTableFilters(
        isServerFilterEnabled, filterHook, catName, dbName, tblName);
  }

  private GetPartitionsResult<Partition> getPartitionsByNames() throws TException {
    List<Partition> ret = null;
    boolean success = false;
    rs.openTransaction();
    try {
      checkLimitNumberOfPartitions(tblName, args.getPartNames().size());
      ret = rs.getPartitionsByNames(catName, dbName, tblName, args);
      ret = FilterUtils.filterPartitionsIfEnabled(isServerFilterEnabled, filterHook, ret);

      // If requested add column statistics in each of the partition objects
      if (request.isGetColStats()) {
        // Since each partition may have stats collected for different set of columns, we
        // request them separately.
        for (Partition part: ret) {
          String partName = Warehouse.makePartName(table.getPartitionKeys(), part.getValues());
          List<ColumnStatistics> partColStatsList =
              rs.getPartitionColumnStatistics(catName, dbName, tblName,
                  Collections.singletonList(partName),
                  StatsSetupConst.getColumnsHavingStats(part.getParameters()),
                  request.getEngine());
          if (partColStatsList != null && !partColStatsList.isEmpty()) {
            ColumnStatistics partColStats = partColStatsList.get(0);
            if (partColStats != null) {
              part.setColStats(partColStats);
            }
          }
        }
      }

      List<String> processorCapabilities = request.getProcessorCapabilities();
      if (processorCapabilities == null || processorCapabilities.isEmpty() ||
          processorCapabilities.contains("MANAGERAWMETADATA")) {
        LOG.info("Skipping translation for processor with {}", request.getProcessorId());
      } else {
        if (handler.getMetadataTransformer() != null) {
          ret = handler.getMetadataTransformer().transformPartitions(ret, table,
              processorCapabilities, request.getProcessorId());
        }
      }
      success = rs.commitTransaction();
    } finally {
      if (!success) {
        rs.rollbackTransaction();
      }
    }
    return new GetPartitionsResult<>(ret, success);
  }

  private GetPartitionsResult getPartitions() throws TException {
    if (request.isFetchPartNames()) {
      List<String> ret = rs.listPartitionNames(catName, dbName, tblName, (short) args.getMax());
      ret = FilterUtils.filterPartitionNamesIfEnabled(isServerFilterEnabled,
          filterHook, catName, dbName, tblName, ret);
      return new GetPartitionsResult<>(ret, true);
    } else {
      List<Partition> ret;
      checkLimitNumberOfPartitionsByFilter(NO_FILTER_STRING, args.getMax());
      ret = rs.listPartitionsPsWithAuth(catName, dbName, tblName, args);
      ret = FilterUtils.filterPartitionsIfEnabled(isServerFilterEnabled, filterHook, ret);
      return new GetPartitionsResult<>(ret, true);
    }
  }

  private void checkLimitNumberOfPartitionsByFilter(String filterString, int requestMax) throws TException {
    if (exceedsPartitionFetchLimit(requestMax)) {
      checkLimitNumberOfPartitions(tblName, rs.getNumPartitionsByFilter(catName, dbName, tblName, filterString));
    }
  }

  private GetPartitionsResult getPartitionsByExpr() throws TException {
    if (request.isFetchPartNames()) {
      List<String> ret = rs.listPartitionNames(catName, dbName, tblName,
          args.getDefaultPartName(), args.getExpr(), args.getOrder(), args.getMax());
      ret = FilterUtils.filterPartitionNamesIfEnabled(isServerFilterEnabled,
          filterHook, catName, dbName, tblName, ret);
      return new GetPartitionsResult(ret, true);
    } else {
      List<Partition> partitions = new LinkedList<>();
      boolean hasUnknownPartitions = false;
      if (exceedsPartitionFetchLimit(args.getMax())) {
        // Since partition limit is configured, we need fetch at most (limit + 1) partition names
        int max = MetastoreConf.getIntVar(handler.getConf(), MetastoreConf.ConfVars.LIMIT_PARTITION_REQUEST) + 1;
        List<String> partNames = rs.listPartitionNames(catName, dbName, tblName, args.getDefaultPartName(),
            args.getExpr(), null, max);
        checkLimitNumberOfPartitions(tblName, partNames.size());
        partitions = rs.getPartitionsByNames(catName, dbName, tblName,
            new GetPartitionsArgs.GetPartitionsArgsBuilder(args).partNames(partNames).build());
      } else {
        hasUnknownPartitions = rs.getPartitionsByExpr(catName, dbName, tblName, partitions, args);
      }
      return new GetPartitionsResult<>(partitions, hasUnknownPartitions);
    }
  }

  // Check input count exceeding partition limit iff:
  //  1. partition limit is enabled.
  //  2. input count is greater than the limit.
  private boolean exceedsPartitionFetchLimit(int count) {
    int partitionLimit = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.LIMIT_PARTITION_REQUEST);
    return partitionLimit > -1 && (count < 0 || count > partitionLimit);
  }

  private void checkLimitNumberOfPartitions(String tblName, int numPartitions) throws MetaException {
    if (exceedsPartitionFetchLimit(numPartitions)) {
      int partitionLimit = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.LIMIT_PARTITION_REQUEST);
      String configName = MetastoreConf.ConfVars.LIMIT_PARTITION_REQUEST.toString();
      throw new MetaException(String.format(PARTITION_NUMBER_EXCEED_LIMIT_MSG, numPartitions,
          tblName, partitionLimit, configName));
    }
  }

  @Override
  protected String getMessagePrefix() {
    return "GetPartitionsHandler [" + id + "] -  Get partitions from " +
        TableName.getQualified(catName, dbName, tblName) + ":";
  }

  public record GetPartitionsResult<T>(List<T> result, boolean success) implements Result {

  }

  public static class GetPartitionsRequest extends TAbstractBase {
    private final TableName tableName;
    private final GetPartitionsArgs getPartitionsArgs;
    private final boolean fetchPartNames;
    private String engine;
    private boolean getColStats;
    private List<String> processorCapabilities;
    private String processorId;

    private List<FieldSchema> partitionOrders;
    private List<FieldSchema> partitionKeys;
    private boolean applyDistinct;
    private boolean isAscending;
    private boolean getPartitionValues;

    public GetPartitionsRequest(TableName tableName,
        GetPartitionsArgs getPartitionsArgs,
        boolean fetchPartNames) {
      this.getPartitionsArgs = getPartitionsArgs;
      this.tableName = tableName;
      this.fetchPartNames = fetchPartNames;
    }

    public GetPartitionsRequest(TableName tableName,
        GetPartitionsArgs getPartitionsArgs) {
      this(tableName, getPartitionsArgs, false);
    }

    public String getEngine() {
      return engine;
    }

    public void setEngine(String engine) {
      this.engine = engine;
    }

    public boolean isGetColStats() {
      return getColStats;
    }

    public void setGetColStats(boolean getColStats) {
      this.getColStats = getColStats;
    }

    public List<String> getProcessorCapabilities() {
      return processorCapabilities;
    }

    public void setProcessorCapabilities(List<String> processorCapabilities) {
      this.processorCapabilities = processorCapabilities;
    }

    public String getProcessorId() {
      return processorId;
    }

    public void setProcessorId(String processorId) {
      this.processorId = processorId;
    }

    public boolean isAscending() {
      return isAscending;
    }

    public void setAscending(boolean ascending) {
      isAscending = ascending;
    }

    public boolean isApplyDistinct() {
      return applyDistinct;
    }

    public void setApplyDistinct(boolean applyDistinct) {
      this.applyDistinct = applyDistinct;
    }

    public List<FieldSchema> getPartitionKeys() {
      return partitionKeys;
    }

    public void setPartitionKeys(List<FieldSchema> partitionKeys) {
      this.partitionKeys = partitionKeys;
    }

    public List<FieldSchema> getPartitionOrders() {
      return partitionOrders;
    }

    public void setPartitionOrders(List<FieldSchema> partitionOrders) {
      this.partitionOrders = partitionOrders;
    }

    public boolean isGetPartitionValues() {
      return getPartitionValues;
    }

    public void setGetPartitionValues(boolean getPartitionValues) {
      this.getPartitionValues = getPartitionValues;
    }

    public TableName getTableName() {
      return tableName;
    }

    public GetPartitionsArgs getGetPartitionsArgs() {
      return getPartitionsArgs;
    }

    public boolean isFetchPartNames() {
      return fetchPartNames;
    }
  }

  public static List<Partition> getPartitions(Consumer<TableName> preHook,
      Consumer<Exception> postHook, IHMSHandler handler, TableName tableName,
      GetPartitionsArgs args, boolean assumeResult) throws NoSuchObjectException, MetaException {
    Exception ex = null;
    try {
      GetPartitionsRequest getPartitionsRequest = new GetPartitionsRequest(tableName, args);
      preHook.accept(tableName);
      GetPartitionsHandler<Partition> getPartsHandler =
          AbstractRequestHandler.offer(handler, getPartitionsRequest);
      List<Partition> partitions = getPartsHandler.getResult().result();
      if (assumeResult && (partitions == null || partitions.isEmpty())) {
        throw new NoSuchObjectException(tableName + " partition not found");
      }
      return partitions;
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(NoSuchObjectException.class, MetaException.class)
          .defaultMetaException();
    } finally {
      postHook.accept(ex);
    }
  }

  public static GetPartitionsResult<Partition> getPartitionsResult(
      Consumer<TableName> preHook,
      Consumer<Exception> postHook,
      IHMSHandler handler, TableName tableName,
      GetPartitionsArgs args) throws TException {
    Exception ex = null;
    try {
      GetPartitionsRequest getPartitionsRequest = new GetPartitionsRequest(tableName, args);
      preHook.accept(tableName);
      GetPartitionsHandler<Partition> getPartsHandler =
          AbstractRequestHandler.offer(handler, getPartitionsRequest);
      return getPartsHandler.getResult();
    } catch (Exception e) {
      ex = e;
      throw handleException(ex).defaultTException();
    } finally {
      postHook.accept(ex);
    }
  }

  public static GetPartitionsResult<String> getPartitionNames(Consumer<TableName> preExecutor,
      Consumer<Exception> postConsumer, IHMSHandler handler, TableName tableName,
      GetPartitionsArgs args) throws TException {
    Exception ex = null;
    try {
      preExecutor.accept(tableName);
      GetPartitionsRequest getPartitionsRequest = new GetPartitionsRequest(tableName, args, true);
      GetPartitionsHandler<String> getPartNamesHandler =
          AbstractRequestHandler.offer(handler, getPartitionsRequest);
      return getPartNamesHandler.getResult();
    } catch (Exception e) {
      ex = e;
      throw handleException(ex).defaultTException();
    } finally {
      postConsumer.accept(ex);
    }
  }

  public static void validatePartVals(IHMSHandler handler,
      TableName tableName, List<String> partVals) throws MetaException, NoSuchObjectException {
    if (partVals == null || partVals.isEmpty()) {
      throw new MetaException("The partVals is null or empty");
    }
    GetTableRequest request = new GetTableRequest(tableName.getDb(), tableName.getTable());
    request.setCatName(tableName.getCat());
    Table table = handler.get_table_core(request);
    int size = table.getPartitionKeysSize();
    if (size != partVals.size()) {
      throw new MetaException("Unmatched partition values, partition keys size: " +
          size + ", partition values size: " + partVals.size());
    }
  }
}
