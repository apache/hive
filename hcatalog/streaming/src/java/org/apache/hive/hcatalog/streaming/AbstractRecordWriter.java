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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TException;

import java.io.IOException;

import java.util.Random;

abstract class AbstractRecordWriter implements RecordWriter {
  static final private Log LOG = LogFactory.getLog(AbstractRecordWriter.class.getName());

  final HiveConf conf;
  final HiveEndPoint endPoint;
  final Table tbl;

  final HiveMetaStoreClient msClient;
  RecordUpdater updater = null;

  private final int totalBuckets;
  private Random rand = new Random();
  private int currentBucketId = 0;
  private final Path partitionPath;

  final AcidOutputFormat<?,?> outf;

  protected AbstractRecordWriter(HiveEndPoint endPoint, HiveConf conf)
          throws ConnectionError, StreamingException {
    this.endPoint = endPoint;
    this.conf = conf!=null ? conf
                : HiveEndPoint.createHiveConf(DelimitedInputWriter.class, endPoint.metaStoreUri);
    try {
      msClient = new HiveMetaStoreClient(this.conf);
      this.tbl = msClient.getTable(endPoint.database, endPoint.table);
      this.partitionPath = getPathForEndPoint(msClient, endPoint);
      this.totalBuckets = tbl.getSd().getNumBuckets();
      if(totalBuckets <= 0) {
        throw new StreamingException("Cannot stream to table that has not been bucketed : "
                + endPoint);
      }
      String outFormatName = this.tbl.getSd().getOutputFormat();
      outf = (AcidOutputFormat<?,?>) ReflectionUtils.newInstance(Class.forName(outFormatName), conf);
    } catch (MetaException e) {
      throw new ConnectionError(endPoint, e);
    } catch (NoSuchObjectException e) {
      throw new ConnectionError(endPoint, e);
    } catch (TException e) {
      throw new StreamingException(e.getMessage(), e);
    } catch (ClassNotFoundException e) {
      throw new StreamingException(e.getMessage(), e);
    }
  }

  protected AbstractRecordWriter(HiveEndPoint endPoint)
          throws ConnectionError, StreamingException {
    this(endPoint, HiveEndPoint.createHiveConf(AbstractRecordWriter.class, endPoint.metaStoreUri) );
  }

  abstract SerDe getSerde() throws SerializationError;

  @Override
  public void flush() throws StreamingIOFailure {
    try {
      updater.flush();
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
    try {
      this.currentBucketId = rand.nextInt(totalBuckets);
      LOG.debug("Creating Record updater");
      updater = createRecordUpdater(currentBucketId, minTxnId, maxTxnID);
    } catch (IOException e) {
      LOG.error("Failed creating record updater", e);
      throw new StreamingIOFailure("Unable to get new record Updater", e);
    }
  }

  @Override
  public void closeBatch() throws StreamingIOFailure {
    try {
      updater.close(false);
      updater = null;
    } catch (IOException e) {
      throw new StreamingIOFailure("Unable to close recordUpdater", e);
    }
  }

  private RecordUpdater createRecordUpdater(int bucketId, Long minTxnId, Long maxTxnID)
          throws IOException, SerializationError {
    try {
      return  outf.getRecordUpdater(partitionPath,
              new AcidOutputFormat.Options(conf)
                      .inspector(getSerde().getObjectInspector())
                      .bucket(bucketId)
                      .minimumTransactionId(minTxnId)
                      .maximumTransactionId(maxTxnID));
    } catch (SerDeException e) {
      throw new SerializationError("Failed to get object inspector from Serde "
              + getSerde().getClass().getName(), e);
    }
  }

  private Path getPathForEndPoint(HiveMetaStoreClient msClient, HiveEndPoint endPoint)
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
