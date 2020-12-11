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

package org.apache.hadoop.hive.ql.impala.catalog;

import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.localcache.HMSPartitionNamesConverter;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.impala.prune.ImpalaBasicHdfsTable;

import java.util.ArrayList;
import java.util.List;

/**
 * The ImpalaPartitionNamesConverter class is used for converting fetched partition names
 * into a converted Impala class.
 * The class converts the HMS result into a Result that extends the original
 * GetPartitionsPsResponse class.  This child class will contain a conversion into the
 * ImpalaBasicHdfsTable class which contains all the ImpalaBasicPartitions.  The
 * ImpalaBasicPartitions is a thin Impala HdfsPartition which just contains the partition
 * name.
 */
public class ImpalaPartitionNamesConverter implements HMSPartitionNamesConverter {

  private final ImpalaPartitionNamesRequest request;

  public ImpalaPartitionNamesConverter(GetPartitionNamesPsRequest request) {
    this.request = (ImpalaPartitionNamesRequest) request;
  }

  @Override
  public GetPartitionNamesPsResponse convertPartitionNames(GetPartitionNamesPsResponse result)
      throws MetaException {
    try {
      ImpalaPartitionNamesResult response = new ImpalaPartitionNamesResult(result);
      response.basicTable = new ImpalaBasicHdfsTable(request.msTbl, request.msDb,
          response.getNames(), request.validWriteIdList, request.defaultPartitionName);
      return response;
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  public static class ImpalaPartitionNamesRequest extends GetPartitionNamesPsRequest {
    public final Database msDb;
    public final Table msTbl;
    public final ValidWriteIdList validWriteIdList;
    public final String defaultPartitionName;

    public ImpalaPartitionNamesRequest(Table msTbl, Database msDb,
        ValidWriteIdList validWriteIdList, String defaultPartitionName,
        int numPartitions) {
      super(msTbl.getDbName(), msTbl.getTableName());
      this.msDb = msDb;
      this.msTbl = msTbl;
      this.validWriteIdList = validWriteIdList;
      this.defaultPartitionName = defaultPartitionName;
      if (validWriteIdList != null) {
        setValidWriteIdList(validWriteIdList.toString());
      }
      // A blank string is treated as a generic wildcard which ensures that all partition names
      // will be fetched.
      List<String> partValues = new ArrayList<>();
      for (int i = 0; i < numPartitions; ++i) {
        partValues.add("");
      }
      setPartValues(partValues);
    }
  }

  public static class ImpalaPartitionNamesResult extends GetPartitionNamesPsResponse {
    public ImpalaBasicHdfsTable basicTable;

    public ImpalaPartitionNamesResult() {
    }

    /**
     * Clone object (shallow clone)
     */
    public ImpalaPartitionNamesResult(GetPartitionNamesPsResponse other) {
      super(other);
    }
  }
}
