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

package org.apache.hive.hcatalog.streaming;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.io.Text;
import org.apache.hive.hcatalog.data.JsonSerDe;

import java.io.IOException;
import java.util.Properties;

/**
 * Streaming Writer handles utf8 encoded Json (Strict syntax).
 * Uses org.apache.hive.hcatalog.data.JsonSerDe to process Json input
 */
public class StrictJsonWriter extends AbstractRecordWriter {
  private JsonSerDe serde;

  /**
   *
   * @param endPoint the end point to write to
   * @throws ConnectionError
   * @throws SerializationError
   * @throws StreamingException
   */
  public StrictJsonWriter(HiveEndPoint endPoint)
          throws ConnectionError, SerializationError, StreamingException {
    super(endPoint, null);
  }

  /**
   *
   * @param endPoint the end point to write to
   * @param conf a Hive conf object. Should be null if not using advanced Hive settings.
   * @throws ConnectionError
   * @throws SerializationError
   * @throws StreamingException
   */
  public StrictJsonWriter(HiveEndPoint endPoint, HiveConf conf)
          throws ConnectionError, SerializationError, StreamingException {
    super(endPoint, conf);
  }

  @Override
  SerDe getSerde() throws SerializationError {
    if(serde!=null) {
      return serde;
    }
    serde = createSerde(tbl, conf);
    return serde;
  }

  @Override
  public void write(long transactionId, byte[] record)
          throws StreamingIOFailure, SerializationError {
    try {
      Object encodedRow = encode(record);
      updater.insert(transactionId, encodedRow);
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

  /**
   * Encode Utf8 encoded string bytes using JsonSerde
   * @param utf8StrRecord
   * @return  The encoded object
   * @throws SerializationError
   */
  private Object encode(byte[] utf8StrRecord) throws SerializationError {
    try {
      Text blob = new Text(utf8StrRecord);
      return serde.deserialize(blob);
    } catch (SerDeException e) {
      throw new SerializationError("Unable to convert byte[] record into Object", e);
    }
  }

}
