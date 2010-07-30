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

package org.apache.hadoop.hive.ql.index.compact;

import java.io.Serializable;
import java.util.HashMap;

public class IndexMetadataChangeWork implements Serializable {

  private static final long serialVersionUID = 1L;

  private HashMap<String, String> partSpec;
  private String indexTbl;
  private String dbName;
  
  public IndexMetadataChangeWork() {
  }
  
  public IndexMetadataChangeWork(HashMap<String, String> partSpec,
      String indexTbl, String dbName) {
    super();
    this.partSpec = partSpec;
    this.indexTbl = indexTbl;
    this.dbName = dbName;
  }

  public HashMap<String, String> getPartSpec() {
    return partSpec;
  }

  public void setPartSpec(HashMap<String, String> partSpec) {
    this.partSpec = partSpec;
  }

  public String getIndexTbl() {
    return indexTbl;
  }

  public void setIndexTbl(String indexTbl) {
    this.indexTbl = indexTbl;
  }
  
  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }
  
}
