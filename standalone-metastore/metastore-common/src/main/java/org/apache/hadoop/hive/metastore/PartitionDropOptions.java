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

package org.apache.hadoop.hive.metastore;

/**
 * Class to generalize the switches for dropPartitions().
 */
public class PartitionDropOptions {

  private boolean deleteData = true;
  private boolean ifExists = false;
  private boolean returnResults = true;
  private boolean purgeData = false;

  private Long writeId;
  private Long txnId;

  public static PartitionDropOptions instance() { return new PartitionDropOptions(); }

  public PartitionDropOptions deleteData(boolean deleteData) {
    this.deleteData = deleteData;
    return this;
  }

  public boolean isDeleteData() {
    return deleteData;
  }

  public PartitionDropOptions ifExists(boolean ifExists) {
    this.ifExists = ifExists;
    return this;
  }

  public boolean isIfExists() {
    return ifExists;
  }

  public PartitionDropOptions returnResults(boolean returnResults) {
    this.returnResults = returnResults;
    return this;
  }

  public boolean isReturnResults() {
    return returnResults;
  }

  public PartitionDropOptions purgeData(boolean purgeData) {
    this.purgeData = purgeData;
    return this;
  }

  public boolean isPurgeData() {
    return purgeData;
  }

  public PartitionDropOptions setWriteId(Long writeId) {
    this.writeId = writeId;
    return this;
  }

  public Long getWriteId() {
    return writeId;
  }

  public void setTxnId(Long txnId) {
    this.txnId = txnId;
  }

  public Long getTxnId() {
    return txnId;
  }

} // class PartitionDropSwitches;

