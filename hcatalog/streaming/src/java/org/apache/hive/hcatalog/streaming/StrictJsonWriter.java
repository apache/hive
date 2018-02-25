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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.apache.hive.hcatalog.data.HCatRecordObjectInspector;
import org.apache.hive.hcatalog.data.JsonSerDe;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Streaming Writer handles utf8 encoded Json (Strict syntax).
 * Uses org.apache.hive.hcatalog.data.JsonSerDe to process Json input
 */
public class StrictJsonWriter extends AbstractRecordWriter {
  private JsonSerDe serde;

  private final HCatRecordObjectInspector recordObjInspector;
  private final ObjectInspector[] bucketObjInspectors;
  private final StructField[] bucketStructFields;

  /**
   * @deprecated As of release 1.3/2.1.  Replaced by {@link #StrictJsonWriter(HiveEndPoint, HiveConf, StreamingConnection)}
   */
  public StrictJsonWriter(HiveEndPoint endPoint)
    throws ConnectionError, SerializationError, StreamingException {
    this(endPoint, null, null);
  }

  /**
   * @deprecated As of release 1.3/2.1.  Replaced by {@link #StrictJsonWriter(HiveEndPoint, HiveConf, StreamingConnection)}
   */
  public StrictJsonWriter(HiveEndPoint endPoint, HiveConf conf) throws StreamingException {
    this(endPoint, conf, null);
  }
  /**
   * @param endPoint the end point to write to
   * @throws ConnectionError
   * @throws SerializationError
   * @throws StreamingException
   */
  public StrictJsonWriter(HiveEndPoint endPoint, StreamingConnection conn)
          throws ConnectionError, SerializationError, StreamingException {
    this(endPoint, null, conn);
  }
  /**
   * @param endPoint the end point to write to
   * @param conf a Hive conf object. Should be null if not using advanced Hive settings.
   * @param conn connection this Writer is to be used with
   * @throws ConnectionError
   * @throws SerializationError
   * @throws StreamingException
   */
  public StrictJsonWriter(HiveEndPoint endPoint, HiveConf conf, StreamingConnection conn)
          throws ConnectionError, SerializationError, StreamingException {
    super(endPoint, conf, conn);
    this.serde = createSerde(tbl, conf);
    // get ObjInspectors for entire record and bucketed cols
    try {
      recordObjInspector = ( HCatRecordObjectInspector ) serde.getObjectInspector();
      this.bucketObjInspectors = getObjectInspectorsForBucketedCols(bucketIds, recordObjInspector);
    } catch (SerDeException e) {
      throw new SerializationError("Unable to get ObjectInspector for bucket columns", e);
    }

    // get StructFields for bucketed cols
    bucketStructFields = new StructField[bucketIds.size()];
    List<? extends StructField> allFields = recordObjInspector.getAllStructFieldRefs();
    for (int i = 0; i < bucketIds.size(); i++) {
      bucketStructFields[i] = allFields.get(bucketIds.get(i));
    }
  }

  @Override
  public AbstractSerDe getSerde() {
    return serde;
  }

  protected HCatRecordObjectInspector getRecordObjectInspector() {
    return recordObjInspector;
  }

  @Override
  protected StructField[] getBucketStructFields() {
    return bucketStructFields;
  }

  protected ObjectInspector[] getBucketObjectInspectors() {
    return bucketObjInspectors;
  }


  @Override
  public void write(long transactionId, byte[] record)
          throws StreamingIOFailure, SerializationError {
    try {
      Object encodedRow = encode(record);
      int bucket = getBucket(encodedRow);
      getRecordUpdater(bucket).insert(transactionId, encodedRow);
    } catch (IOException e) {
      throw new StreamingIOFailure("Error writing record in transaction("
              + transactionId + ")", e);
    }

  }

  /**
   * Creates JsonSerDe
   * @param tbl   used to create serde
   * @param conf  used to create serde
   * @return
   * @throws SerializationError if serde could not be initialized
   */
  private static JsonSerDe createSerde(Table tbl, HiveConf conf)
          throws SerializationError {
    try {
      Properties tableProps = MetaStoreUtils.getTableMetadata(tbl);
      JsonSerDe serde = new JsonSerDe();
      SerDeUtils.initializeSerDe(serde, conf, tableProps, null);
      return serde;
    } catch (SerDeException e) {
      throw new SerializationError("Error initializing serde " + JsonSerDe.class.getName(), e);
    }
  }

  @Override
  public Object encode(byte[] utf8StrRecord) throws SerializationError {
    try {
      Text blob = new Text(utf8StrRecord);
      return serde.deserialize(blob);
    } catch (SerDeException e) {
      throw new SerializationError("Unable to convert byte[] record into Object", e);
    }
  }

}
