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

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;

// This is added as part of moving MSCK code from ql to standalone-metastore. There is a metastore API to drop
// partitions by name but we cannot use it because msck typically will contain partition value (year=2014). We almost
// never drop partition by name (year). So we need to construct expression filters, the current
// PartitionExpressionProxy implementations (PartitionExpressionForMetastore and HCatClientHMSImpl.ExpressionBuilder)
// all depend on ql code to build ExprNodeDesc for the partition expressions. It also depends on kryo for serializing
// the expression objects to byte[]. For MSCK drop partition, we don't need complex expression generator. For now,
// all we do is split the partition spec (year=2014/month=24) into filter expression year='2014' and month='24' and
// rely on metastore database to deal with type conversions. Ideally, PartitionExpressionProxy default implementation
// should use SearchArgument (storage-api) to construct the filter expression and not depend on ql, but the usecase
// for msck is pretty simple and this specific implementation should suffice.
public class MsckPartitionExpressionProxy implements PartitionExpressionProxy {

  @Override
  public String convertExprToFilter(final byte[] exprBytes, final String defaultPartitionName) throws MetaException {
    return new String(exprBytes, StandardCharsets.UTF_8);
  }

  @Override
  public boolean filterPartitionsByExpr(List<FieldSchema> partColumns, byte[] expr, String
    defaultPartitionName, List<String> partitionNames) throws MetaException {
    return false;
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
