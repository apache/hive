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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(MsckPartitionExpressionProxy.class);

  @Override
  public String convertExprToFilter(final byte[] exprBytes, final String defaultPartitionName,
                                    boolean decodeFilterExpToStr) throws MetaException {
    return new String(exprBytes, StandardCharsets.UTF_8);
  }

  @Override
  public boolean filterPartitionsByExpr(List<FieldSchema> partColumns, byte[] expr, String
    defaultPartitionName, List<String> partitionNames) throws MetaException {
    String partExpr = new String(expr, StandardCharsets.UTF_8);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Partition expr: {}", expr);
    }
    //This is to find in partitionNames all that match expr
    //reverse of the Msck.makePartExpr
    Set<String> filterParts = new HashSet<>();
    String[] partitions = partExpr.split(" OR ");
    for (String part : partitions) {
      part = part.substring(1, part.length() - 1);
      String[] pKeyValues = part.split(" AND ");
      StringBuilder builder = new StringBuilder();
      for (String pKeyValue : pKeyValues) {
        String[] colAndValue = pKeyValue.split("=");
        String key = FileUtils.unescapePathName(colAndValue[0]);
        //take the value inside without the single quote marks '2018-10-30' becomes 2018-10-31
        String value = FileUtils.unescapePathName(colAndValue[1].substring(1, colAndValue[1].length() - 1));
        builder.append(key + "=" + value).append("/");
      }
      builder.setLength(builder.length() - 1);
      filterParts.add(builder.toString());
    }

    List<String> partNamesSeq =  new ArrayList<>();
    for (String part : partitionNames) {
      //list of partitions [year=2001/month=1, year=2002/month=2, year=2001/month=3]
      //Given expr: e.g. year='2001' AND month='1'. Only when all the expressions in the expr can be found,
      //do we add the partition to the filtered result [year=2001/month=1]
      if (filterParts.contains(FileUtils.unescapePathName(part))) {
        partNamesSeq.add(part);
      }
    }
    partitionNames.clear();
    partitionNames.addAll(partNamesSeq);
    LOG.info("The returned partition list is of size: {}", partitionNames.size());
    if (LOG.isDebugEnabled()) {
      for(String s : partitionNames){
        LOG.debug("Matched partition: {}", s);
      }
    }
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
