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

package org.apache.hadoop.hive.accumulo.mr;

import java.util.HashMap;
import java.util.Map;

/**
 * Index table definition.
 */
public class AccumuloIndexDefinition {
  private final String baseTable;
  private final String indexTable;
  private final Map<String, String> colMap;


  public AccumuloIndexDefinition(String baseTable, String indexTable) {
    this.colMap = new HashMap<String, String>();
    this.baseTable = baseTable;
    this.indexTable = indexTable;
  }

  public String getBaseTable() {
    return baseTable;
  }


  public String getIndexTable() {
    return indexTable;
  }

  public void addIndexCol(String cf, String cq, String colType) {
    colMap.put(encode(cf, cq), colType);
  }

  public Map<String, String> getColumnMap() {
    return colMap;
  }

  public void setColumnTuples(String columns) {
    if (columns != null) {
      String cols = columns.trim();
      if (!cols.isEmpty() && !"*".equals(cols)) {
        for (String col : cols.split(",")) {
          String[] cfcqtp = col.trim().split(":");
          addIndexCol(cfcqtp[0], cfcqtp[1], cfcqtp[2]);
        }
      }
    }
  }

  public boolean contains(String cf, String cq) {
    return colMap.containsKey(encode(cf, cq));
  }

  public String getColType(String cf, String cq) {
    return colMap.get(encode(cf, cq));
  }

  private String encode(String cf, String cq) {
    return cq + ":" + cq;
  }
}
