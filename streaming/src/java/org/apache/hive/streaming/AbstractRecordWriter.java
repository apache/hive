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

package org.apache.hive.streaming;


import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.HeapMemoryMonitor;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SubStructObjectInspector;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRecordWriter implements RecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRecordWriter.class.getName());

  private static final String DEFAULT_LINE_DELIMITER_PATTERN = "[\r\n]";
  protected HiveConf conf;
  protected StreamingConnection conn;
  protected Table table;
  protected List<String> inputColumns;
  protected List<String> inputTypes;
  protected String fullyQualifiedTableName;
  protected Map<String, List<RecordUpdater>> updaters = new HashMap<>();
  protected Map<String, Path> partitionPaths = new HashMap<>();
  protected Set<String> addedPartitions = new HashSet<>();
  // input OI includes table columns + partition columns
  protected StructObjectInspector inputRowObjectInspector;
  // output OI strips off the partition columns and retains other columns
  protected ObjectInspector outputRowObjectInspector;
  protected List<String> partitionColumns = new ArrayList<>();
  protected ObjectInspector[] partitionObjInspectors = null;
  protected StructField[] partitionStructFields = null;
  protected Object[] partitionFieldData;
  protected ObjectInspector[] bucketObjInspectors = null;
  protected StructField[] bucketStructFields = null;
  protected Object[] bucketFieldData;
  protected List<Integer> bucketIds = new ArrayList<>();
  protected int totalBuckets;
  protected String defaultPartitionName;
  protected boolean isBucketed;
  protected AcidOutputFormat<?, ?> acidOutputFormat;
  protected Long curBatchMinWriteId;
  protected Long curBatchMaxWriteId;
  protected final String lineDelimiter;
  protected HeapMemoryMonitor heapMemoryMonitor;
  // if low memory canary is set and if records after set canary exceeds threshold, trigger a flush.
  // This is to avoid getting notified of low memory too often and flushing too often.
  protected AtomicBoolean lowMemoryCanary;
  protected long ingestSizeBytes = 0;
  protected boolean autoFlush;
  protected float memoryUsageThreshold;
  protected long ingestSizeThreshold;
  protected FileSystem fs;

  public AbstractRecordWriter(final String lineDelimiter) {
    this.lineDelimiter = lineDelimiter == null || lineDelimiter.isEmpty() ?
      DEFAULT_LINE_DELIMITER_PATTERN : lineDelimiter;
  }

  protected static class OrcMemoryPressureMonitor implements HeapMemoryMonitor.Listener {
    private static final Logger LOG = LoggerFactory.getLogger(OrcMemoryPressureMonitor.class.getName());
    private final AtomicBoolean lowMemoryCanary;

    OrcMemoryPressureMonitor(final AtomicBoolean lowMemoryCanary) {
      this.lowMemoryCanary = lowMemoryCanary;
    }

    @Override
    public void memoryUsageAboveThreshold(final long usedMemory, final long maxMemory) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Orc memory pressure notified! usedMemory: {} maxMemory: {}.",
          LlapUtil.humanReadableByteCount(usedMemory), LlapUtil.humanReadableByteCount(maxMemory));
      }
      lowMemoryCanary.set(true);
    }
  }

  @Override
  public void init(StreamingConnection conn, long minWriteId, long maxWriteId) throws StreamingException {
    if (conn == null) {
      throw new StreamingException("Streaming connection cannot be null during record writer initialization");
    }
    this.conn = conn;
    this.curBatchMinWriteId = minWriteId;
    this.curBatchMaxWriteId = maxWriteId;
    this.conf = conn.getHiveConf();
    this.defaultPartitionName = conf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME);
    this.table = conn.getTable();
    String location = table.getSd().getLocation();
    try {
      URI uri = new URI(location);
      this.fs = FileSystem.newInstance(uri, conf);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Created new filesystem instance: {}", System.identityHashCode(this.fs));
      }
    } catch (URISyntaxException e) {
      throw new StreamingException("Unable to create URI from location: " + location, e);
    } catch (IOException e) {
      throw new StreamingException("Unable to get filesystem for location: " + location, e);
    }
    this.inputColumns = table.getSd().getCols().stream().map(FieldSchema::getName).collect(Collectors.toList());
    this.inputTypes = table.getSd().getCols().stream().map(FieldSchema::getType).collect(Collectors.toList());
    if (conn.isPartitionedTable() && conn.isDynamicPartitioning()) {
      this.partitionColumns = table.getPartitionKeys().stream().map(FieldSchema::getName)
        .collect(Collectors.toList());
      this.inputColumns.addAll(partitionColumns);
      this.inputTypes
        .addAll(table.getPartitionKeys().stream().map(FieldSchema::getType).collect(Collectors.toList()));
    }
    this.fullyQualifiedTableName = Warehouse.getQualifiedName(table.getDbName(), table.getTableName());
    String outFormatName = this.table.getSd().getOutputFormat();
    try {
      this.acidOutputFormat = (AcidOutputFormat<?, ?>) ReflectionUtils
        .newInstance(JavaUtils.loadClass(outFormatName), conf);
    } catch (Exception e) {
      String shadePrefix = conf.getVar(HiveConf.ConfVars.HIVE_CLASSLOADER_SHADE_PREFIX);
      if (shadePrefix != null && !shadePrefix.trim().isEmpty()) {
        try {
          LOG.info("Shade prefix: {} specified. Using as fallback to load {}..", shadePrefix, outFormatName);
          this.acidOutputFormat = (AcidOutputFormat<?, ?>) ReflectionUtils
            .newInstance(JavaUtils.loadClass(shadePrefix, outFormatName), conf);
        } catch (ClassNotFoundException e1) {
          throw new StreamingException(e.getMessage(), e);
        }
      } else {
        throw new StreamingException(e.getMessage(), e);
      }
    }
    setupMemoryMonitoring();
    try {
      final AbstractSerDe serDe = createSerde();
      this.inputRowObjectInspector = (StructObjectInspector) serDe.getObjectInspector();
      if (conn.isPartitionedTable() && conn.isDynamicPartitioning()) {
        preparePartitioningFields();
        int dpStartCol = inputRowObjectInspector.getAllStructFieldRefs().size() - table.getPartitionKeys().size();
        this.outputRowObjectInspector = new SubStructObjectInspector(inputRowObjectInspector, 0, dpStartCol);
      } else {
        this.outputRowObjectInspector = inputRowObjectInspector;
      }
      prepareBucketingFields();
    } catch (SerDeException e) {
      throw new StreamingException("Unable to create SerDe", e);
    }
  }

  protected void setupMemoryMonitoring() {
    this.autoFlush = conf.getBoolVar(HiveConf.ConfVars.HIVE_STREAMING_AUTO_FLUSH_ENABLED);
    this.memoryUsageThreshold = conf.getFloatVar(HiveConf.ConfVars.HIVE_HEAP_MEMORY_MONITOR_USAGE_THRESHOLD);
    this.ingestSizeThreshold = conf.getSizeVar(HiveConf.ConfVars.HIVE_STREAMING_AUTO_FLUSH_CHECK_INTERVAL_SIZE);
    LOG.info("Memory monitoring settings - autoFlush: {} memoryUsageThreshold: {} ingestSizeThreshold: {}",
      autoFlush, memoryUsageThreshold, ingestSizeBytes);
    this.heapMemoryMonitor = new HeapMemoryMonitor(memoryUsageThreshold);
    MemoryUsage tenuredMemUsage = heapMemoryMonitor.getTenuredGenMemoryUsage();
    if (tenuredMemUsage != null) {
      lowMemoryCanary = new AtomicBoolean(false);
      heapMemoryMonitor.registerListener(new OrcMemoryPressureMonitor(lowMemoryCanary));
      heapMemoryMonitor.start();
      // alert if we already running low on memory (starting with low memory will lead to frequent auto flush)
      float currentUsage = (float) tenuredMemUsage.getUsed() / (float) tenuredMemUsage.getMax();
      if (currentUsage > memoryUsageThreshold) {
        LOG.warn("LOW MEMORY ALERT! Tenured gen memory is already low. Increase memory to improve performance." +
            " Used: {} Max: {}", LlapUtil.humanReadableByteCount(tenuredMemUsage.getUsed()),
          LlapUtil.humanReadableByteCount(tenuredMemUsage.getMax()));
      }
    }
  }

  protected void prepareBucketingFields() {
    this.isBucketed = table.getSd().getNumBuckets() > 0;
    // For unbucketed tables we have exactly 1 RecordUpdater (until HIVE-19208) for each AbstractRecordWriter which
    // ends up writing to a file bucket_000000.
    // See also {@link #getBucket(Object)}
    this.totalBuckets = isBucketed ? table.getSd().getNumBuckets() : 1;
    if (isBucketed) {
      this.bucketIds = getBucketColIDs(table.getSd().getBucketCols(), table.getSd().getCols());
      this.bucketFieldData = new Object[bucketIds.size()];
      this.bucketObjInspectors = getObjectInspectorsForBucketedCols(bucketIds, inputRowObjectInspector);
      this.bucketStructFields = new StructField[bucketIds.size()];
      List<? extends StructField> allFields = inputRowObjectInspector.getAllStructFieldRefs();
      for (int i = 0; i < bucketIds.size(); i++) {
        bucketStructFields[i] = allFields.get(bucketIds.get(i));
      }
    }
  }

  protected void preparePartitioningFields() {
    final int numPartitions = table.getPartitionKeys().size();
    this.partitionFieldData = new Object[numPartitions];
    this.partitionObjInspectors = new ObjectInspector[numPartitions];
    int startIdx = inputRowObjectInspector.getAllStructFieldRefs().size() - numPartitions;
    int endIdx = inputRowObjectInspector.getAllStructFieldRefs().size();
    int j = 0;
    for (int i = startIdx; i < endIdx; i++) {
      StructField structField = inputRowObjectInspector.getAllStructFieldRefs().get(i);
      partitionObjInspectors[j++] = structField.getFieldObjectInspector();
    }
    this.partitionStructFields = new StructField[partitionColumns.size()];
    for (int i = 0; i < partitionColumns.size(); i++) {
      String partCol = partitionColumns.get(i);
      partitionStructFields[i] = inputRowObjectInspector.getStructFieldRef(partCol);
    }
  }

  /**
   * used to tag error msgs to provided some breadcrumbs
   */
  protected String getWatermark(String partition) {
    return partition + " writeIds[" + curBatchMinWriteId + "," + curBatchMaxWriteId + "]";
  }

  // return the column numbers of the bucketed columns
  protected List<Integer> getBucketColIDs(List<String> bucketCols, List<FieldSchema> cols) {
    ArrayList<Integer> result = new ArrayList<>(bucketCols.size());
    HashSet<String> bucketSet = new HashSet<>(bucketCols);
    for (int i = 0; i < cols.size(); i++) {
      if (bucketSet.contains(cols.get(i).getName())) {
        result.add(i);
      }
    }
    return result;
  }

  /**
   * Create SerDe for the record writer.
   *
   * @return - serde
   * @throws SerializationError - if serde cannot be created.
   */
  public abstract AbstractSerDe createSerde() throws SerializationError;

  /**
   * Encode a record as an Object that Hive can read with the ObjectInspector associated with the
   * serde returned by {@link #createSerde}.  This is public so that test frameworks can use it.
   *
   * @param record record to be deserialized
   * @return deserialized record as an Object
   * @throws SerializationError - any error during serialization or deserialization of record
   */
  public abstract Object encode(byte[] record) throws SerializationError;

  // returns the bucket number to which the record belongs to
  protected int getBucket(Object row) {
    if (!isBucketed) {
      return 0;
    }
    Object[] bucketFields = getBucketFields(row);
    int bucketingVersion = Utilities.getBucketingVersion(
      table.getParameters().get(hive_metastoreConstants.TABLE_BUCKETING_VERSION));

    return bucketingVersion == 2 ?
      ObjectInspectorUtils.getBucketNumber(bucketFields, bucketObjInspectors, totalBuckets) :
      ObjectInspectorUtils.getBucketNumberOld(bucketFields, bucketObjInspectors, totalBuckets);
  }

  protected List<String> getPartitionValues(final Object row) {
    if (!conn.isPartitionedTable()) {
      return null;
    }
    List<String> partitionValues = new ArrayList<>();
    if (conn.isPartitionedTable() && conn.isDynamicPartitioning()) {
      Object[] partitionFields = getPartitionFields(row);
      for (int i = 0; i < partitionObjInspectors.length; i++) {
        ObjectInspector oi = partitionObjInspectors[i];
        Object field = partitionFields[i];
        Object partitionValue = ObjectInspectorUtils.copyToStandardObject(field, oi, ObjectInspectorUtils
          .ObjectInspectorCopyOption.WRITABLE);
        if (partitionValue == null || partitionValue.toString().length() == 0) {
          partitionValues.add(defaultPartitionName);
        } else {
          partitionValues.add(partitionValue.toString());
        }
      }
    } else {
      partitionValues = conn.getStaticPartitionValues();
    }
    return partitionValues;
  }

  @Override
  public void flush() throws StreamingIOFailure {
    try {
      if (LOG.isDebugEnabled()) {
        logStats("Stats before flush:");
      }
      for (Map.Entry<String, List<RecordUpdater>> entry : updaters.entrySet()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Flushing record updater for partitions: {}", entry.getKey());
        }
        for (RecordUpdater updater : entry.getValue()) {
          if (updater != null) {
            updater.flush();
          }
        }
      }
      ingestSizeBytes = 0;
      if (LOG.isDebugEnabled()) {
        logStats("Stats after flush:");
      }
    } catch (IOException e) {
      throw new StreamingIOFailure("Unable to flush recordUpdater", e);
    }
  }

  @Override
  public void close() throws StreamingIOFailure {
    heapMemoryMonitor.close();
    boolean haveError = false;
    String partition = null;
    if (LOG.isDebugEnabled()) {
      logStats("Stats before close:");
    }
    for (Map.Entry<String, List<RecordUpdater>> entry : updaters.entrySet()) {
      partition = entry.getKey();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Closing updater for partitions: {}", partition);
      }
      for (RecordUpdater updater : entry.getValue()) {
        if (updater != null) {
          try {
            //try not to leave any files open
            updater.close(false);
          } catch (Exception ex) {
            haveError = true;
            LOG.error("Unable to close " + updater + " due to: " + ex.getMessage(), ex);
          }
        }
      }
      entry.getValue().clear();
    }
    updaters.clear();
    if (LOG.isDebugEnabled()) {
      logStats("Stats after close:");
    }
    if (haveError) {
      throw new StreamingIOFailure("Encountered errors while closing (see logs) " + getWatermark(partition));
    }
  }

  protected static ObjectInspector[] getObjectInspectorsForBucketedCols(List<Integer> bucketIds
    , StructObjectInspector recordObjInspector) {
    ObjectInspector[] result = new ObjectInspector[bucketIds.size()];

    for (int i = 0; i < bucketIds.size(); i++) {
      int bucketId = bucketIds.get(i);
      result[i] =
        recordObjInspector.getAllStructFieldRefs().get(bucketId).getFieldObjectInspector();
    }
    return result;
  }

  protected Object[] getBucketFields(Object row) {
    for (int i = 0; i < bucketIds.size(); i++) {
      bucketFieldData[i] = inputRowObjectInspector.getStructFieldData(row, bucketStructFields[i]);
    }
    return bucketFieldData;
  }

  protected Object[] getPartitionFields(Object row) {
    for (int i = 0; i < partitionFieldData.length; i++) {
      partitionFieldData[i] = inputRowObjectInspector.getStructFieldData(row, partitionStructFields[i]);
    }
    return partitionFieldData;
  }

  @Override
  public void write(final long writeId, final InputStream inputStream) throws StreamingException {
    try (Scanner scanner = new Scanner(inputStream).useDelimiter(lineDelimiter)) {
      while (scanner.hasNext()) {
        write(writeId, scanner.next().getBytes());
      }
    }
  }

  @Override
  public void write(final long writeId, final byte[] record) throws StreamingException {
    checkAutoFlush();
    ingestSizeBytes += record.length;
    try {
      Object encodedRow = encode(record);
      int bucket = getBucket(encodedRow);
      List<String> partitionValues = getPartitionValues(encodedRow);
      getRecordUpdater(partitionValues, bucket).insert(writeId, encodedRow);
      // ingest size bytes gets resetted on flush() whereas connection stats is not
      conn.getConnectionStats().incrementRecordsWritten();
      conn.getConnectionStats().incrementRecordsSize(record.length);
    } catch (IOException e) {
      throw new StreamingIOFailure("Error writing record in transaction write id ("
        + writeId + ")", e);
    }
  }

  protected void checkAutoFlush() throws StreamingIOFailure {
    if (!autoFlush) {
      return;
    }
    if (lowMemoryCanary != null) {
      if (lowMemoryCanary.get() && ingestSizeBytes > ingestSizeThreshold) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Low memory canary is set and ingestion size (buffered) threshold '{}' exceeded. " +
            "Flushing all record updaters..", LlapUtil.humanReadableByteCount(ingestSizeThreshold));
        }
        flush();
        conn.getConnectionStats().incrementAutoFlushCount();
        lowMemoryCanary.set(false);
      }
    } else {
      if (ingestSizeBytes > ingestSizeThreshold) {
        MemoryMXBean mxBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapUsage = mxBean.getHeapMemoryUsage();
        float memUsedFraction = ((float) heapUsage.getUsed() / (float) heapUsage.getMax());
        if (memUsedFraction > memoryUsageThreshold) {
          if (LOG.isDebugEnabled()) {
            LOG.info("Memory usage threshold '{}' and ingestion size (buffered) threshold '{}' exceeded. " +
              "Flushing all record updaters..", memUsedFraction, LlapUtil.humanReadableByteCount(ingestSizeThreshold));
          }
          flush();
          conn.getConnectionStats().incrementAutoFlushCount();
        }
      }
    }
  }

  @Override
  public Set<String> getPartitions() {
    return addedPartitions;
  }

  protected RecordUpdater createRecordUpdater(final Path partitionPath, int bucketId, Long minWriteId,
    Long maxWriteID)
    throws IOException {
    // Initialize table properties from the table parameters. This is required because the table
    // may define certain table parameters that may be required while writing. The table parameter
    // 'transactional_properties' is one such example.
    Properties tblProperties = new Properties();
    tblProperties.putAll(table.getParameters());
    return acidOutputFormat.getRecordUpdater(partitionPath,
      new AcidOutputFormat.Options(conf)
        .filesystem(fs)
        .inspector(outputRowObjectInspector)
        .bucket(bucketId)
        .tableProperties(tblProperties)
        .minimumWriteId(minWriteId)
        .maximumWriteId(maxWriteID)
        .statementId(-1)
        .finalDestination(partitionPath));
  }

  protected RecordUpdater getRecordUpdater(List<String> partitionValues, int bucketId) throws StreamingIOFailure {
    RecordUpdater recordUpdater;
    String key;
    Path destLocation;
    try {
      key = partitionValues == null ? fullyQualifiedTableName : partitionValues.toString();
      // add partition in metastore for dynamic partition. We make a metastore call for every new partition value that
      // we encounter even if partition already exists (exists check require a metastore call anyways).
      if (partitionPaths.containsKey(key)) {
        destLocation = partitionPaths.get(key);
      } else {
        // un-partitioned table
        if (partitionValues == null) {
          destLocation = new Path(table.getSd().getLocation());
        } else {
          PartitionInfo partitionInfo = conn.createPartitionIfNotExists(partitionValues);
          // collect the newly added partitions. connection.commitTransaction() will report the dynamically added
          // partitions to TxnHandler
          if (!partitionInfo.isExists()) {
            addedPartitions.add(partitionInfo.getName());
            if (LOG.isDebugEnabled()) {
              LOG.debug("Created partition {} for table {}", partitionInfo.getName(), fullyQualifiedTableName);
            }
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Partition {} already exists for table {}", partitionInfo.getName(), fullyQualifiedTableName);
            }
          }
          destLocation = new Path(partitionInfo.getPartitionLocation());
        }
        partitionPaths.put(key, destLocation);
      }
      updaters.computeIfAbsent(key, k -> initializeBuckets());
      recordUpdater = updaters.get(key).get(bucketId);
    } catch (StreamingException e) {
      throw new StreamingIOFailure("Unable to create partition: " + partitionValues + "for " + conn, e);
    }
    if (recordUpdater == null) {
      try {
        recordUpdater = createRecordUpdater(destLocation, bucketId, curBatchMinWriteId, curBatchMaxWriteId);
      } catch (IOException e) {
        String errMsg = "Failed creating RecordUpdater for " + getWatermark(destLocation.toString());
        LOG.error(errMsg, e);
        throw new StreamingIOFailure(errMsg, e);
      }
      List<RecordUpdater> partitionUpdaters = updaters.get(key);
      partitionUpdaters.set(bucketId, recordUpdater);
    }
    return recordUpdater;
  }

  protected List<RecordUpdater> initializeBuckets() {
    List<RecordUpdater> result = new ArrayList<>(totalBuckets);
    for (int bucket = 0; bucket < totalBuckets; bucket++) {
      result.add(bucket, null); //so that get(i) returns null rather than ArrayOutOfBounds
    }
    return result;
  }

  protected void logStats(final String prefix) {
    int openRecordUpdaters = updaters.values()
      .stream()
      .mapToInt(List::size)
      .sum();
    long bufferedRecords = updaters.values()
      .stream()
      .flatMap(List::stream)
      .filter(Objects::nonNull)
      .mapToLong(RecordUpdater::getBufferedRowCount)
      .sum();
    MemoryUsage memoryUsage = heapMemoryMonitor.getTenuredGenMemoryUsage();
    String oldGenUsage = "NA";
    if (memoryUsage != null) {
      oldGenUsage = "used/max => " + LlapUtil.humanReadableByteCount(memoryUsage.getUsed()) + "/" +
        LlapUtil.humanReadableByteCount(memoryUsage.getMax());
    }
    LOG.debug("{} [record-updaters: {}, partitions: {}, buffered-records: {} total-records: {} " +
        "buffered-ingest-size: {}, total-ingest-size: {} tenured-memory-usage: {}]", prefix, openRecordUpdaters,
      partitionPaths.size(), bufferedRecords, conn.getConnectionStats().getRecordsWritten(),
      LlapUtil.humanReadableByteCount(ingestSizeBytes),
      LlapUtil.humanReadableByteCount(conn.getConnectionStats().getRecordsSize()), oldGenUsage);
  }
}
