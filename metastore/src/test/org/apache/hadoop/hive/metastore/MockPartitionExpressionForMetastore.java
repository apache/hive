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

import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import java.util.List;

/**
 * Test Mock-out for PartitionExpressionForMetastore.
 */
public class MockPartitionExpressionForMetastore implements PartitionExpressionProxy {
  @Override
  public String convertExprToFilter(byte[] expr) throws MetaException {
    return null;
  }

  @Override
  public boolean filterPartitionsByExpr(List<String> partColumnNames,
      List<PrimitiveTypeInfo> partColumnTypeInfos, byte[] expr, String defaultPartitionName,
      List<String> partitionNames) throws MetaException {
    return false;
  }

  @Override
  public FileMetadataExprType getMetadataType(String inputFormat) {
    return null;
  }

  @Override
  public SearchArgument createSarg(byte[] expr) {
    return null;
  }

  @Override
  public FileFormatProxy getFileFormatProxy(FileMetadataExprType type) {
    return null;
  }
}
