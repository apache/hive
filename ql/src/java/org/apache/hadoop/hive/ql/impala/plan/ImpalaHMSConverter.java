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

package org.apache.hadoop.hive.ql.impala.plan;

import org.apache.hadoop.hive.metastore.HMSConverter;
import org.apache.hadoop.hive.metastore.localcache.HMSPartitionConverter;
import org.apache.hadoop.hive.metastore.localcache.HMSPartitionNamesConverter;
import org.apache.hadoop.hive.metastore.localcache.HMSTableConverter;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.impala.catalog.ImpalaPartitionConverter;
import org.apache.hadoop.hive.ql.impala.catalog.ImpalaPartitionConverter.ImpalaGetPartitionsByNamesRequest;
import org.apache.hadoop.hive.ql.impala.catalog.ImpalaTableConverter;
import org.apache.hadoop.hive.ql.impala.catalog.ImpalaTableConverter.ImpalaGetTableRequest;
import org.apache.hadoop.hive.ql.impala.catalog.ImpalaPartitionNamesConverter;
import org.apache.hadoop.hive.ql.impala.catalog.ImpalaPartitionNamesConverter.ImpalaPartitionNamesRequest;
import org.apache.hadoop.hive.ql.impala.catalog.ImpalaPartitionNamesConverter.ImpalaPartitionNamesResult;

/**
 * Top level Converter for HMS objects.  Responsible for creating object specific converters.
 */
public class ImpalaHMSConverter implements HMSConverter {

  @Override
  public HMSPartitionConverter getPartitionConverter(GetPartitionsByNamesRequest rqst,
      Table table) {
    // Not gonna handle conversions when there is no file metadata.
    if (!rqst.isGetFileMetadata()) {
      return null;
    }
    if (!(rqst instanceof ImpalaGetPartitionsByNamesRequest)) {
      return null;
    }
    return new ImpalaPartitionConverter(rqst, table);
  }

  @Override
  public HMSTableConverter getTableConverter(GetTableRequest rqst) {
    // Not gonna handle conversions when there is no file metadata.
    if (!rqst.isGetFileMetadata()) {
      return null;
    }
    if (!(rqst instanceof ImpalaGetTableRequest)) {
      return null;
    }
    return new ImpalaTableConverter(rqst);
  }

  @Override
  public HMSPartitionNamesConverter getPartitionNamesConverter(GetPartitionNamesPsRequest rqst,
      GetPartitionNamesPsResponse result) {
    if (!(rqst instanceof ImpalaPartitionNamesRequest)) {
      return null;
    }
    // if the result is already of type ImpalaPartitionNamesResult, there is no need for a
    // conversion so no need to create a converter.
    if (result instanceof ImpalaPartitionNamesResult) {
      return null;
    }
    return new ImpalaPartitionNamesConverter(rqst);
  }
}
