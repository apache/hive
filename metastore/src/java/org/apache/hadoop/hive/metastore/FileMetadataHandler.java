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

package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.hbase.MetadataStore;

/**
 * The base implementation of a file metadata handler for a specific file type.
 * There are currently two classes for each file type (of 1), this one, which is very simple due
 * to the fact that it just calls the proxy class for most calls; and the proxy class, that
 * contains the actual implementation that depends on some stuff in QL (for ORC).
 */
public abstract class FileMetadataHandler {
  protected static final Log LOG = LogFactory.getLog(FileMetadataHandler.class);

  private Configuration conf;
  private PartitionExpressionProxy expressionProxy;
  private FileFormatProxy fileFormatProxy;
  private MetadataStore store;

  /**
   * Same as RawStore.getFileMetadataByExpr.
   */
  public abstract void getFileMetadataByExpr(List<Long> fileIds, byte[] expr,
      ByteBuffer[] metadatas, ByteBuffer[] results, boolean[] eliminated) throws IOException;

  protected abstract FileMetadataExprType getType();

  protected PartitionExpressionProxy getExpressionProxy() {
    return expressionProxy;
  }

  protected FileFormatProxy getFileFormatProxy() {
    return fileFormatProxy;
  }

  protected MetadataStore getStore() {
    return store;
  }

  /**
   * Configures the handler. Called once before use.
   * @param conf Config.
   * @param expressionProxy Expression proxy to access ql stuff.
   * @param store Storage interface to manipulate the metadata.
   */
  public void configure(
      Configuration conf, PartitionExpressionProxy expressionProxy, MetadataStore store) {
    this.conf = conf;
    this.expressionProxy = expressionProxy;
    this.store = store;
    this.fileFormatProxy = expressionProxy.getFileFormatProxy(getType());
  }

  /**
   * Caches the file metadata for a particular file.
   * @param fileId File id.
   * @param fs The filesystem of the file.
   * @param path Path to the file.
   */
  public void cacheFileMetadata(long fileId, FileSystem fs, Path path)
      throws IOException, InterruptedException {
    // ORC is in ql, so we cannot do anything here. For now, all the logic is in the proxy.
    ByteBuffer[] cols = fileFormatProxy.getAddedColumnsToCache();
    ByteBuffer[] vals = (cols == null) ? null : new ByteBuffer[cols.length];
    ByteBuffer metadata = fileFormatProxy.getMetadataToCache(fs, path, vals);
    LOG.info("Caching file metadata for " + path + ", size " + metadata.remaining());
    store.storeFileMetadata(fileId, metadata, cols, vals);
  }

  /**
   * @return the added column names to be cached in metastore with the metadata for this type.
   */
  public ByteBuffer[] createAddedCols() {
    return fileFormatProxy.getAddedColumnsToCache();
  }

  /**
   * @return the values for the added columns returned by createAddedCols for respective metadatas.
   */
  public ByteBuffer[][] createAddedColVals(List<ByteBuffer> metadata) {
    return fileFormatProxy.getAddedValuesToCache(metadata);
  }
}
