package org.apache.hadoop.hive.metastore;
/*
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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;

import java.util.List;

/**
 * Default implementation of PartitionExpressionProxy.  Eventually this should use the SARGs in
 * Hive's storage-api.  For now it just throws UnsupportedOperationException.
 */
public class DefaultPartitionExpressionProxy implements PartitionExpressionProxy {
  @Override
  public String convertExprToFilter(byte[] expr, String defaultPartitionName) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean filterPartitionsByExpr(List<FieldSchema> partColumns, byte[] expr, String
      defaultPartitionName, List<String> partitionNames) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileMetadataExprType getMetadataType(String inputFormat) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileFormatProxy getFileFormatProxy(FileMetadataExprType type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SearchArgument createSarg(byte[] expr) {
    throw new UnsupportedOperationException();
  }
}
