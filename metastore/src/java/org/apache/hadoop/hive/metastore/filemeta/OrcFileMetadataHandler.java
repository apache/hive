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

package org.apache.hadoop.hive.metastore.filemeta;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.hbase.HBaseReadWrite;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;

public class OrcFileMetadataHandler implements FileMetadataHandler {
  private final Configuration conf;
  private final PartitionExpressionProxy expressionProxy;
  private final HBaseReadWrite hbase;

  public OrcFileMetadataHandler(Configuration conf,
      PartitionExpressionProxy expressionProxy, HBaseReadWrite hbase) {
    this.conf = conf;
    this.expressionProxy = expressionProxy;
    this.hbase = hbase;
  }

  @Override
  public void getFileMetadataByExpr(List<Long> fileIds, byte[] expr,
      ByteBuffer[] metadatas, ByteBuffer[] results, boolean[] eliminated) throws IOException {
    SearchArgument sarg = expressionProxy.createSarg(expr);
    // For now, don't push anything into HBase, nor store anything special in HBase
    if (metadatas == null) {
      // null means don't return metadata; we'd need the array anyway for now.
      metadatas = new ByteBuffer[results.length];
    }
    hbase.getFileMetadata(fileIds, metadatas);
    for (int i = 0; i < metadatas.length;  ++i) {
      if (metadatas[i] == null) continue;
      ByteBuffer result = expressionProxy.applySargToFileMetadata(sarg, metadatas[i]);
      eliminated[i] = (result == null);
      if (!eliminated[i]) {
        results[i] = result;
      }
    }
  }

}
