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

package org.apache.hive.hcatalog.streaming;


import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.thrift.TException;

import java.io.IOException;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;


public abstract class AbstractRecordWriter implements RecordWriter {
  static final private Logger LOG = LoggerFactory.getLogger(AbstractRecordWriter.class.getName());

  final HiveConf conf;
  final HiveEndPoint endPoint;
  final Table tbl;

  final IMetaStoreClient msClient;
  protected final List<Integer> bucketIds;
  ArrayList<RecordUpdater> updaters = null;

  public final int totalBuckets;

  private final Path partitionPath;

  final AcidOutputFormat<?,?> outf;
  private Object[] bucketFieldData; // Pre-allocated in constructor. Updated on each write.
  private Long curBatchMinTxnId;
  private Long curBatchMaxTxnId;

  private static final class TableWriterPair {
    private final Table tbl;
    private final Path partitionPath;
    TableWriterPair(Table t, Path p) {
      tbl = t;
      partitionPath = p;
    }
  }
  /**
   * @deprecated As of release 1.3/2.1.  Replaced by {@link #AbstractRecordWriter(HiveEndPoint, HiveConf, StreamingConnection)}
   */
  protected AbstractRecordWriter(HiveEndPoint endPoint, HiveConf conf)
    throws ConnectionError, StreamingException {
    this(endPoint, conf, null);
  }
  protected AbstractRecordWriter(HiveEndPoint endPoint2, HiveConf conf, StreamingConnection conn)
          throws StreamingException {
    this.endPoint = endPoint2;
    this.conf = conf!=null ? conf
                : HiveEndPoint.createHiveConf(DelimitedInputWriter.class, endPoint.metaStoreUri);
    try {
      msClient = HCatUtil.getHiveMetastoreClient(this.conf);
      UserGroupInformation ugi = conn != null ? conn.getUserGroupInformation() : null;
      if (ugi == null) {
        this.tbl = msClient.getTable(endPoint.database, endPoint.table);
        this.partitionPath = getPathForEndPoint(msClient, endPoint);
      } else {
        TableWriterPair twp = ugi.doAs(
          new PrivilegedExceptionAction<TableWriterPair>() {
            @Override
            public TableWriterPair run() throws Exception {
              return new TableWriterPair(msClient.getTable(endPoint.database, endPoint.table),
                getPathForEndPoint(msClient, endPoint));
            }
          });
        this.tbl = twp.tbl;
        this.partitionPath = twp.partitionPath;
      }
      this.totalBuckets = tbl.getSd().getNumBuckets();
      if (totalBuckets <= 0) {
        throw new StreamingException("Cannot stream to table that has not been bucketed : "
          + endPoint);
      }
      this.bucketIds = getBucketColIDs(tbl.getSd().getBucketCols(), tbl.getSd().getCols());
      this.bucketFieldData = new Object[bucketIds.size()];
      String outFormatName = this.tbl.getSd().getOutputFormat();
      outf = (AcidOutputFormat<?, ?>) ReflectionUtils.newInstance(JavaUtils.loadClass(outFormatName), conf);
      bucketFieldData = new Object[bucketIds.size()];
    } catch(InterruptedException e) {
      throw new StreamingException(endPoint2.toString(), e);
    } catch (MetaException | NoSuchObjectException e) {
      throw new ConnectionError(endPoint2, e);
    } catch (TException | ClassNotFoundException | IOException e) {
      throw new StreamingException(e.getMessage(), e);
    }
  }

  /**
   * used to tag error msgs to provied some breadcrumbs
   */
  String getWatermark() {
    return partitionPath + " txnIds[" + curBatchMinTxnId + "," + curBatchMaxTxnId + "]";
  }
  // return the column numbers of the bucketed columns
  private List<Integer> getBucketColIDs(List<String> bucketCols, List<FieldSchema> cols) {
    ArrayList<Integer> result =  new ArrayList<Integer>(bucketCols.size());
    HashSet<String> bucketSet = new HashSet<String>(bucketCols);
    for (int i = 0; i < cols.size(); i++) {
      if( bucketSet.contains(cols.get(i).getName()) ) {
        result.add(i);
      }
    }
    return result;
  }

  /**
   * Get the SerDe for the Objects created by {@link #encode}.  This is public so that test
   * frameworks can use it.
   * @return serde
   * @throws SerializationError
   */
  public abstract AbstractSerDe getSerde() throws SerializationError;

  /**
   * Encode a record as an Object that Hive can read with the ObjectInspector associated with the
   * serde returned by {@link #getSerde}.  This is public so that test frameworks can use it.
   * @param record record to be deserialized
   * @return deserialized record as an Object
   * @throws SerializationError
   */
  public abstract Object encode(byte[] record) throws SerializationError;

  protected abstract ObjectInspector[] getBucketObjectInspectors();
  protected abstract StructObjectInspector getRecordObjectInspector();
  protected abstract StructField[] getBucketStructFields();

  // returns the bucket number to which the record belongs to
  protected int getBucket(Object row) throws SerializationError {
    ObjectInspector[] inspectors = getBucketObjectInspectors();
    Object[] bucketFields = getBucketFields(row);
    return ObjectInspectorUtils.getBucketNumber(bucketFields, inspectors, totalBuckets);
  }

  @Override
  public void flush() throws StreamingIOFailure {
    try {
      for (RecordUpdater updater : updaters) {
        if (updater != null) {
          updater.flush();
        }
      }
    } catch (IOException e) {
      throw new StreamingIOFailure("Unable to flush recordUpdater", e);
    }
  }

  @Override
  public void clear() throws StreamingIOFailure {
  }

  /**
   * Creates a new record updater for the new batch
   * @param minTxnId smallest Txnid in the batch
   * @param maxTxnID largest Txnid in the batch
   * @throws StreamingIOFailure if failed to create record updater
   */
  @Override
  public void newBatch(Long minTxnId, Long maxTxnID)
          throws StreamingIOFailure, SerializationError {
    curBatchMinTxnId = minTxnId;
    curBatchMaxTxnId = maxTxnID;
    updaters = new ArrayList<RecordUpdater>(totalBuckets);
    for (int bucket = 0; bucket < totalBuckets; bucket++) {
      updaters.add(bucket, null);
    }
  }

  @Override
  public void closeBatch() throws StreamingIOFailure {
    boolean haveError = false;
    for (RecordUpdater updater : updaters) {
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
    updaters.clear();
    if(haveError) {
      throw new StreamingIOFailure("Encountered errors while closing (see logs) " + getWatermark());
    }
  }

  protected static ObjectInspector[] getObjectInspectorsForBucketedCols(List<Integer> bucketIds
          , StructObjectInspector recordObjInspector)
          throws SerializationError {
    ObjectInspector[] result = new ObjectInspector[bucketIds.size()];

    for (int i = 0; i < bucketIds.size(); i++) {
      int bucketId = bucketIds.get(i);
      result[i] =
              recordObjInspector.getAllStructFieldRefs().get( bucketId ).getFieldObjectInspector();
    }
    return result;
  }


  private Object[] getBucketFields(Object row) throws SerializationError {
    StructObjectInspector recordObjInspector = getRecordObjectInspector();
    StructField[] bucketStructFields = getBucketStructFields();
    for (int i = 0; i < bucketIds.size(); i++) {
      bucketFieldData[i] = recordObjInspector.getStructFieldData(row,  bucketStructFields[i]);
    }
    return bucketFieldData;
  }

  private RecordUpdater createRecordUpdater(int bucketId, Long minTxnId, Long maxTxnID)
          throws IOException, SerializationError {
    try {
      // Initialize table properties from the table parameters. This is required because the table
      // may define certain table parameters that may be required while writing. The table parameter
      // 'transactional_properties' is one such example.
      Properties tblProperties = new Properties();
      tblProperties.putAll(tbl.getParameters());
      return  outf.getRecordUpdater(partitionPath,
              new AcidOutputFormat.Options(conf)
                      .inspector(getSerde().getObjectInspector())
                      .bucket(bucketId)
                      .tableProperties(tblProperties)
                      .minimumTransactionId(minTxnId)
                      .maximumTransactionId(maxTxnID)
                      .statementId(-1)
                      .finalDestination(partitionPath));
    } catch (SerDeException e) {
      throw new SerializationError("Failed to get object inspector from Serde "
              + getSerde().getClass().getName(), e);
    }
  }

  RecordUpdater getRecordUpdater(int bucketId) throws StreamingIOFailure, SerializationError {
    RecordUpdater recordUpdater = updaters.get(bucketId);
    if (recordUpdater == null) {
      try {
        recordUpdater = createRecordUpdater(bucketId, curBatchMinTxnId, curBatchMaxTxnId);
      } catch (IOException e) {
        String errMsg = "Failed creating RecordUpdater for " + getWatermark();
        LOG.error(errMsg, e);
        throw new StreamingIOFailure(errMsg, e);
      }
      updaters.set(bucketId, recordUpdater);
    }
    return recordUpdater;
  }

  private Path getPathForEndPoint(IMetaStoreClient msClient, HiveEndPoint endPoint)
          throws StreamingException {
    try {
      String location;
      if(endPoint.partitionVals==null || endPoint.partitionVals.isEmpty() ) {
        location = msClient.getTable(endPoint.database,endPoint.table)
                .getSd().getLocation();
      } else {
        location = msClient.getPartition(endPoint.database, endPoint.table,
                endPoint.partitionVals).getSd().getLocation();
      }
      return new Path(location);
    } catch (TException e) {
      throw new StreamingException(e.getMessage()
              + ". Unable to get path for end point: "
              + endPoint.partitionVals, e);
    }
  }
}
