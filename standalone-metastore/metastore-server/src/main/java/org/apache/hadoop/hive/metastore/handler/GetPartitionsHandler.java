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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;
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
import org.apache.hadoop.hive.metastore.api.GetPartitionsByFilterRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthRequest;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsRequest;
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
public class GetPartitionsHandler<Req, T> extends AbstractRequestHandler<GetPartitionsHandler.GetPartitionsRequest<Req>,
    GetPartitionsHandler.GetPartitionsResult<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(GetPartitionsHandler.class);
  private static final String NO_FILTER_STRING = "";
  private RawStore rs;
  private String catName;
  private String dbName;
  private String tblName;
  private Table table;
  private Configuration conf;
  private MetaStoreFilterHook filterHook;
  private boolean isServerFilterEnabled;

  GetPartitionsHandler(IHMSHandler handler, GetPartitionsRequest request) {
    super(handler, false, request);
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    catName = normalizeIdentifier(request.getTableName().getCat());
    dbName = normalizeIdentifier(request.getTableName().getDb());
    tblName = normalizeIdentifier(request.getTableName().getTable());
    conf = handler.getConf();
    rs = handler.getMS();
    filterHook = handler.getMetaFilterHook();
    isServerFilterEnabled = filterHook != null;
    GetTableRequest getTableRequest = new GetTableRequest(dbName, tblName);
    getTableRequest.setCatName(catName);
    table = handler.get_table_core(getTableRequest);
    ((HMSHandler) handler).firePreEvent(new PreReadTableEvent(table, handler));
    authorizeTableForPartitionMetadata();

    LOG.info("Starting to get {} of {}", request.isFetchPartNames() ? "partition names" : "partitions",
        TableName.getQualified(catName, dbName, tblName));
  }

  @Override
  protected GetPartitionsResult execute() throws TException, IOException {
    Req req = request.getReq();
    if (req instanceof PartitionValuesRequest pvq) {
      return getPartitionValues(pvq);
    } else if (req instanceof GetPartitionsByNamesRequest gpbr) {
      return getPartitionsByNames(gpbr);
    } else if (req instanceof PartitionsRequest pr) {
      return getPartitions(pr);
    } else if (req instanceof GetPartitionsByFilterRequest fpr) {
      return getPartitionsByFilter(fpr);
    } else if (req instanceof PartitionsByExprRequest pber) {
      return getPartitionsByExpr(pber);
    } else if (req instanceof GetPartitionsPsWithAuthRequest gpar) {
      return getPartitionsByVals(gpar);
    }
    throw new UnsupportedOperationException("Not yet implemented");
  }

  private GetPartitionsResult getPartitionsByVals(GetPartitionsPsWithAuthRequest gpar) throws TException {
    GetPartitionsArgs args = GetPartitionsArgs.from(gpar);
    if (request.isFetchPartNames()) {
      List<String> ret = rs.listPartitionNamesPs(catName, dbName, tblName,
          args.getPart_vals(), (short) args.getMax());
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

  private GetPartitionsResult getPartitionValues(PartitionValuesRequest pvq) throws MetaException {
    PartitionValuesResponse resp = rs.listPartitionValues(catName, dbName, tblName, pvq.getPartitionKeys(),
        pvq.isApplyDistinct(), pvq.getFilter(), pvq.isAscending(),
        pvq.getPartitionOrder(), pvq.getMaxParts());
    return new GetPartitionsResult<>(List.of(resp), true);
  }

  private void checkLimitNumberOfPartitionsByPs(List<String> partVals, int requestMax) throws TException {
    if (exceedsPartitionFetchLimit(requestMax)) {
      checkLimitNumberOfPartitions(tblName, rs.getNumPartitionsByPs(catName, dbName, tblName, partVals));
    }
  }

  private GetPartitionsResult<Partition> getPartitionsByFilter(GetPartitionsByFilterRequest filterReq) throws TException {
    List<Partition> ret;
    GetPartitionsArgs args = GetPartitionsArgs.from(filterReq);
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

    return new GetPartitionsResult<>(ret, true);
  }

  /**
   * Check if user can access the table associated with the partition. If not, then throw exception
   * so user cannot access partitions associated with this table
   * @throws NoSuchObjectException
   * @throws MetaException
   */
  private void authorizeTableForPartitionMetadata() throws NoSuchObjectException, MetaException {
    FilterUtils.checkDbAndTableFilters(
        isServerFilterEnabled, filterHook, catName, dbName, tblName);
  }

  private GetPartitionsResult getPartitionsByNames(GetPartitionsByNamesRequest gpbr) throws TException {
    List<Partition> ret = null;
    boolean success = false;
    rs.openTransaction();
    try {
      GetPartitionsArgs args = GetPartitionsArgs.from(gpbr);
      checkLimitNumberOfPartitions(tblName, args.getPartNames().size());
      ret = rs.getPartitionsByNames(catName, dbName, tblName, args);
      ret = FilterUtils.filterPartitionsIfEnabled(isServerFilterEnabled, filterHook, ret);

      // If requested add column statistics in each of the partition objects
      if (gpbr.isGet_col_stats()) {
        // Since each partition may have stats collected for different set of columns, we
        // request them separately.
        for (Partition part: ret) {
          String partName = Warehouse.makePartName(table.getPartitionKeys(), part.getValues());
          List<ColumnStatistics> partColStatsList =
              rs.getPartitionColumnStatistics(catName, dbName, tblName,
                  Collections.singletonList(partName),
                  StatsSetupConst.getColumnsHavingStats(part.getParameters()),
                  gpbr.getEngine());
          if (partColStatsList != null && !partColStatsList.isEmpty()) {
            ColumnStatistics partColStats = partColStatsList.getFirst();
            if (partColStats != null) {
              part.setColStats(partColStats);
            }
          }
        }
      }

      List<String> processorCapabilities = gpbr.getProcessorCapabilities();
      if (processorCapabilities == null || processorCapabilities.isEmpty() ||
          processorCapabilities.contains("MANAGERAWMETADATA")) {
        LOG.info("Skipping translation for processor with {}", gpbr.getProcessorIdentifier());
      } else {
        if (handler.getMetadataTransformer() != null) {
          ret = handler.getMetadataTransformer().transformPartitions(ret, table,
              processorCapabilities, gpbr.getProcessorIdentifier());
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

  private GetPartitionsResult getPartitions(PartitionsRequest pr) throws TException {
    GetPartitionsArgs args = GetPartitionsArgs.from(pr);
    if (request.isFetchPartNames()) {
      List<String> ret = rs.listPartitionNames(catName, dbName, tblName, (short) args.getMax());
      return new GetPartitionsResult<>(ret, true);
    } else {
      List<Partition> ret;
      checkLimitNumberOfPartitionsByFilter(NO_FILTER_STRING, args.getMax());
      ret = rs.listPartitionsPsWithAuth(catName, dbName, tblName, args);
      return new GetPartitionsResult<>(ret, true);
    }
  }

  private void checkLimitNumberOfPartitionsByFilter(String filterString, int requestMax) throws TException {
    if (exceedsPartitionFetchLimit(requestMax)) {
      checkLimitNumberOfPartitions(tblName, rs.getNumPartitionsByFilter(catName, dbName, tblName, filterString));
    }
  }

  private GetPartitionsResult getPartitionsByExpr(PartitionsByExprRequest pber) throws TException {
    GetPartitionsArgs args = GetPartitionsArgs.from(pber);
    if (request.isFetchPartNames()) {
      List<String> ret = rs.listPartitionNames(catName, dbName, tblName,
          args.getDefaultPartName(), args.getExpr(), pber.getOrder(), args.getMax());
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
      GetPartitionsResult result = new GetPartitionsResult<>(partitions, true);
      result.setHasUnknownPartitions(hasUnknownPartitions);
      return result;
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
  protected void afterExecute(GetPartitionsResult<T> result) throws TException, IOException {
    if (result != null && result.success()) {
      List ret = result.result();
      if (request.isFetchPartNames()) {
        ret = FilterUtils.filterPartitionNamesIfEnabled(isServerFilterEnabled,
            filterHook, catName, dbName, tblName, ret);
      } else if (!(request.req instanceof PartitionValuesRequest) &&
          !(request.req instanceof GetPartitionsByNamesRequest)) {
        // GetPartitionsMethod.NAMES has already selected the result
        ret = FilterUtils.filterPartitionsIfEnabled(isServerFilterEnabled, filterHook, ret);
      }
      result.setResult(ret);
    }
  }

  @Override
  public String toString() {
    return "GetPartitionsHandler [" + id + "] -  Get partitions from " +
        TableName.getQualified(catName, dbName, tblName) + ":";
  }

  public static class GetPartitionsResult<T> implements Result {
    private List<T> result;
    private final boolean success;
    private boolean hasUnknownPartitions;

    public GetPartitionsResult(List<T> getPartsResult, boolean success) {
      this.result = getPartsResult;
      this.success = success;
    }

    public void setHasUnknownPartitions(boolean unknownPartitions) {
      this.hasUnknownPartitions = unknownPartitions;
    }

    public void setResult(List<T> result) {
      this.result = result;
    }

    public boolean hasUnknownPartitions() {
      return hasUnknownPartitions;
    }

    @Override
    public boolean success() {
      return success;
    }

    public List<T> result() {
      return result;
    }
  }

  public static class GetPartitionsRequest<Req> extends TAbstractBase {
    private final TableName tableName;
    private final boolean fetchPartNames;
    private final Req req;

    public GetPartitionsRequest(Req req, TableName tableName,
        boolean fetchPartNames) {
      this.tableName = tableName;
      this.fetchPartNames = fetchPartNames;
      this.req = req;
    }

    public GetPartitionsRequest(Req req, TableName tableName) {
      this(req, tableName, false);
    }

    public Req getReq() {
      return req;
    }

    public TableName getTableName() {
      return tableName;
    }

    public boolean isFetchPartNames() {
      return fetchPartNames;
    }
  }

  public static <Req> List<Partition> getPartitions(Consumer<TableName> preHook,
      Consumer<Pair<GetPartitionsResult, Exception>> postHook, IHMSHandler handler, TableName tableName,
      Req req, boolean uniqPartition) throws NoSuchObjectException, MetaException {
    Exception ex = null;
    GetPartitionsResult result = null;
    try {
      GetPartitionsRequest getPartitionsRequest = new GetPartitionsRequest(req, tableName);
      preHook.accept(tableName);
      GetPartitionsHandler<Req, Partition> getPartsHandler =
          AbstractRequestHandler.offer(handler, getPartitionsRequest);
      result = getPartsHandler.getResult();
      List<Partition> partitions = result.result();
      if (uniqPartition) {
        List<FieldSchema> partitionKeys = getPartsHandler.table.getPartitionKeys();
        String requestPartName = null;
        if (req instanceof GetPartitionsPsWithAuthRequest gpar) {
          if (gpar.getPartNames() != null && !gpar.getPartNames().isEmpty()) {
            requestPartName = gpar.getPartNames().getFirst();
          } else {
            requestPartName = Warehouse.makePartName(partitionKeys, gpar.getPartVals());
          }
        } else if (req instanceof GetPartitionsByNamesRequest gbnr) {
          requestPartName = gbnr.getNames().getFirst();
        }
        if (partitions == null || partitions.isEmpty()) {
          throw new NoSuchObjectException(tableName + " partition: " + requestPartName + " not found");
        } else if (partitions.size() > 1) {
          throw new MetaException(
              "Expecting only one partition but more than one partitions are found.");
        } else {
          // Check ObjectStore getPartitionWithAuth
          // We need to compare partition name with requested name since some DBs
          // (like MySQL, Derby) considers 'a' = 'a ' whereas others like (Postgres,
          // Oracle) doesn't exhibit this problem.
          Partition partition = partitions.getFirst();
          String partName = Warehouse.makePartName(partitionKeys, partition.getValues());
          if (!partName.equals(requestPartName)) {
            throw new MetaException("Expecting a partition with name " + requestPartName
                + ", but metastore is returning a partition with name " + partName + ".");
          }
        }
      }
      return partitions;
    } catch (Exception e) {
      ex = e;
      // Create a new dummy GetPartitionsResult for postHook to consume
      result = new GetPartitionsResult(List.of(), false);
      throw handleException(e).throwIfInstance(NoSuchObjectException.class, MetaException.class)
          .defaultMetaException();
    } finally {
      postHook.accept(Pair.of(result, ex));
    }
  }

  public static <Req> GetPartitionsResult<Partition> getPartitionsResult(
      Consumer<TableName> preHook,
      Consumer<Pair<GetPartitionsResult, Exception>> postHook,
      IHMSHandler handler, TableName tableName, Req req) throws TException {
    GetPartitionsResult result = null;
    Exception ex = null;
    try {
      GetPartitionsRequest getPartitionsRequest = new GetPartitionsRequest(req, tableName);
      preHook.accept(tableName);
      GetPartitionsHandler<Req, Partition> getPartsHandler =
          AbstractRequestHandler.offer(handler, getPartitionsRequest);
      result = getPartsHandler.getResult();
      return result;
    } catch (Exception e) {
      ex = e;
      throw handleException(ex).defaultTException();
    } finally {
      postHook.accept(Pair.of(result, ex));
    }
  }

  public static <Req> GetPartitionsResult<String> getPartitionNames(Consumer<TableName> preExecutor,
      Consumer<Pair<GetPartitionsResult, Exception>> postConsumer, IHMSHandler handler, TableName tableName,
      Req req) throws TException {
    Exception ex = null;
    GetPartitionsResult result = null;
    try {
      preExecutor.accept(tableName);
      GetPartitionsRequest getPartitionsRequest = new GetPartitionsRequest(req, tableName, true);
      GetPartitionsHandler<Req, String> getPartNamesHandler =
          AbstractRequestHandler.offer(handler, getPartitionsRequest);
      result = getPartNamesHandler.getResult();
      return result;
    } catch (Exception e) {
      ex = e;
      throw handleException(ex).defaultTException();
    } finally {
      postConsumer.accept(Pair.of(result, ex));
    }
  }

  public static String validatePartVals(IHMSHandler handler,
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
    return Warehouse.makePartName(table.getPartitionKeys(), partVals);
  }

  public static PartitionsRequest createPartitionsRequest(TableName tableName, int max) {
    PartitionsRequest pr = new PartitionsRequest(tableName.getDb(), tableName.getTable());
    pr.setCatName(tableName.getCat());
    pr.setMaxParts((short) max);
    return pr;
  }
}
