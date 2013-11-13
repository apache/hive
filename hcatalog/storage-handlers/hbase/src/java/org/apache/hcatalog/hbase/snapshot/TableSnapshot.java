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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.hbase.snapshot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The snapshot for a table and a list of column families.
 */
public class TableSnapshot implements Serializable {

  private String name;

  private Map<String, Long> cfRevisionMap;

  private long latestRevision;


  public TableSnapshot(String name, Map<String, Long> cfRevMap, long latestRevision) {
    this.name = name;
    if (cfRevMap == null) {
      throw new IllegalArgumentException("revision map cannot be null");
    }
    this.cfRevisionMap = cfRevMap;
    this.latestRevision = latestRevision;
  }

  /**
   * Gets the table name.
   *
   * @return String The name of the table.
   */
  public String getTableName() {
    return name;
  }

  /**
   * Gets the column families.
   *
   * @return List<String> A list of column families associated with the snapshot.
   */
  public List<String> getColumnFamilies(){
    return new ArrayList<String>(this.cfRevisionMap.keySet());
  }

  /**
   * For wire serialization only
   */
  Map<String, Long> getColumnFamilyRevisionMap() {
    return cfRevisionMap;
  }

  /**
   * Gets the revision.
   *
   * @param familyName The name of the column family.
   * @return the revision
   */
  public long getRevision(String familyName){
    if(cfRevisionMap.containsKey(familyName))
      return cfRevisionMap.get(familyName);
    return latestRevision;
  }

  /**
   * @return the latest committed revision when this snapshot was taken
   */
  public long getLatestRevision() {
    return latestRevision;
  }

  @Override
  public String toString() {
    String snapshot = "Table Name : " + name +" Latest Revision: " + latestRevision
        + " Column Familiy revision : " + cfRevisionMap.toString();
    return snapshot;
  }
}
