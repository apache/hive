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

package org.apache.hadoop.hive.metastore.model;


public class MPartitionEvent {

  private String dbName;

  private String tblName;

  private String partName;

  private long eventTime;

  private int eventType;

  public MPartitionEvent(String dbName, String tblName, String partitionName, int eventType) {
    super();
    this.dbName = dbName;
    this.tblName = tblName;
    this.partName = partitionName;
    this.eventType = eventType;
    this.eventTime = System.currentTimeMillis();
  }

  public MPartitionEvent() {}

  /**
   * @param dbName the dbName to set
   */
  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  /**
   * @param tblName the tblName to set
   */
  public void setTblName(String tblName) {
    this.tblName = tblName;
  }

  /**
   * @param partName the partSpec to set
   */
  public void setPartName(String partName) {
    this.partName = partName;
  }

  /**
   * @param createTime the eventTime to set
   */
  public void setEventTime(long createTime) {
    this.eventTime = createTime;
  }

  /**
   * @param eventType the EventType to set
   */
  public void setEventType(int eventType) {
    this.eventType = eventType;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "MPartitionEvent [dbName=" + dbName + ", tblName=" + tblName + ", partName=" + partName
        + ", eventTime=" + eventTime + ", EventType=" + eventType + "]";
  }


}
