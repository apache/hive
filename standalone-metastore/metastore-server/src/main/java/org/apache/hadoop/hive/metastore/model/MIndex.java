/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore.model;

/**
 * Represents hive's index definition.
 */
public class MIndex {

  private String indexName;
  private MTable origTable;
  private int createTime;
  private int lastAccessTime;
  private MTable indexTable;
  private MStorageDescriptor sd;
  private String indexHandlerClass;
  private boolean deferredRebuild;
  private int pageSize;
  private int bufferPoolSize;
  private String pointerType;
  private String indexType;

  public MIndex() {
  }

  /**
   * @param indexName index name
   * @param baseTable base table
   * @param createTime creation time
   * @param lastAccessTime
   * @param indexTable
   * @param sd
   * @param indexHandlerClass
   * @param deferredRebuild
   */
  public MIndex(String indexName, MTable baseTable, int createTime,
                int lastAccessTime, MTable indexTable,
                MStorageDescriptor sd, String indexHandlerClass, boolean deferredRebuild,
                int pageSize, int bufferPoolSize, String pointerType, String indexType) {
    super();
    this.indexName = indexName;
    this.origTable = baseTable;
    this.createTime = createTime;
    this.lastAccessTime = lastAccessTime;
    this.indexTable = indexTable;
    this.sd = sd;
    this.indexHandlerClass = indexHandlerClass;
    this.deferredRebuild = deferredRebuild;
    this.pageSize = pageSize;
    this.bufferPoolSize = bufferPoolSize;
    this.pointerType = pointerType;
    this.indexType = indexType;
  }

  /**
   * @return index name
   */
  public String getIndexName() {
    return indexName;
  }

  /**
   * @param indexName index name
   */
  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  /**
   * @return create time
   */
  public int getCreateTime() {
    return createTime;
  }

  /**
   * @param createTime create time
   */
  public void setCreateTime(int createTime) {
    this.createTime = createTime;
  }

  /**
   * @return last access time
   */
  public int getLastAccessTime() {
    return lastAccessTime;
  }

  /**
   * @param lastAccessTime last access time
   */
  public void setLastAccessTime(int lastAccessTime) {
    this.lastAccessTime = lastAccessTime;
  }

//  /**
//   * @return parameters
//   */
//  public Map<String, String> getParameters() {
//    return parameters;
//  }
//
//  /**
//   * @param parameters parameters
//   */
//  public void setParameters(Map<String, String> parameters) {
//    this.parameters = parameters;
//  }

  /**
   * @return original table
   */
  public MTable getOrigTable() {
    return origTable;
  }

  /**
   * @param origTable
   */
  public void setOrigTable(MTable origTable) {
    this.origTable = origTable;
  }

  /**
   * @return index table
   */
  public MTable getIndexTable() {
    return indexTable;
  }

  /**
   * @param indexTable
   */
  public void setIndexTable(MTable indexTable) {
    this.indexTable = indexTable;
  }

  /**
   * @return storage descriptor
   */
  public MStorageDescriptor getSd() {
    return sd;
  }

  /**
   * @param sd
   */
  public void setSd(MStorageDescriptor sd) {
    this.sd = sd;
  }

  /**
   * @return indexHandlerClass
   */
  public String getIndexHandlerClass() {
    return indexHandlerClass;
  }

  /**
   * @param indexHandlerClass
   */
  public void setIndexHandlerClass(String indexHandlerClass) {
    this.indexHandlerClass = indexHandlerClass;
  }

  /**
   * @return auto rebuild
   */
  public boolean isDeferredRebuild() {
    return deferredRebuild;
  }

  /**
   * @return auto rebuild
   */
  public boolean getDeferredRebuild() {
    return deferredRebuild;
  }

  /**
   * @param deferredRebuild
   */
  public void setDeferredRebuild(boolean deferredRebuild) {
    this.deferredRebuild = deferredRebuild;
  }

  public int getPageSize() {
    return pageSize;
  }

  public int getBufferPoolSize() {
    return bufferPoolSize;
  }

  public String getPointerType() {
    return pointerType;
  }

  public String getIndexType() {
    return indexType;
  }
}
