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

package org.apache.hadoop.hive.serde2;

public class SerDeStats {

  /**
   * Class used to pass statistics information from serializer/deserializer to the tasks.
   * A SerDeStats object is returned by calling SerDe.getStats().
   */

  // currently we support only raw data size stat
  private long rawDataSize;
  private long rowCount;

  public SerDeStats() {
    rawDataSize = 0;
    rowCount = 0;
  }

  /**
   * Return the raw data size
   * @return raw data size
   */
  public long getRawDataSize() {
    return rawDataSize;
  }

  /**
   * Set the raw data size
   * @param uSize - size to be set
   */
  public void setRawDataSize(long uSize) {
    rawDataSize = uSize;
  }

  /**
   * Return the row count
   * @return row count
   */
  public long getRowCount() {
    return rowCount;
  }

  /**
   * Set the row count
   * @param rowCount - count of rows
   */
  public void setRowCount(long rowCount) {
    this.rowCount = rowCount;
  }

}
