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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.common.ErrorType;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class which handles querying the metadata server using the MetaStoreClient. The list of
 * partitions matching the partition filter is fetched from the server and the information is
 * serialized and written into the JobContext configuration. The inputInfo is also updated with
 * info required in the client process context.
 */
class InitializeInput {

  private static final Logger LOG = LoggerFactory.getLogger(InitializeInput.class);

  /**
   * @see org.apache.hive.hcatalog.mapreduce.InitializeInput#setInput(org.apache.hadoop.conf.Configuration, InputJobInfo)
   */
  public static void setInput(Job job, InputJobInfo theirInputJobInfo) throws Exception {
    setInput(job.getConfiguration(), theirInputJobInfo);
  }

  /**
   * Set the input to use for the Job. This queries the metadata server with the specified
   * partition predicates, gets the matching partitions, and puts the information in the job
   * configuration object.
   *
   * To ensure a known InputJobInfo state, only the database name, table name, filter, and
   * properties are preserved. All other modification from the given InputJobInfo are discarded.
   *
   * After calling setInput, InputJobInfo can be retrieved from the job configuration as follows:
   * {code}
   * InputJobInfo inputInfo = (InputJobInfo) HCatUtil.deserialize(
   *     job.getConfiguration().get(HCatConstants.HCAT_KEY_JOB_INFO));
   * {code}
   *
   * @param conf the job Configuration object
   * @param theirInputJobInfo information on the Input to read
   * @throws Exception
   */
  public static void setInput(Configuration conf,
                InputJobInfo theirInputJobInfo) throws Exception {
    InputJobInfo inputJobInfo = InputJobInfo.create(
      theirInputJobInfo.getDatabaseName(),
      theirInputJobInfo.getTableName(),
      theirInputJobInfo.getFilter(),
      theirInputJobInfo.getProperties());
    conf.set(
      HCatConstants.HCAT_KEY_JOB_INFO,
      HCatUtil.serialize(getInputJobInfo(conf, inputJobInfo, null)));
  }

  /**
   * Returns the given InputJobInfo after populating with data queried from the metadata service.
   */
  private static InputJobInfo getInputJobInfo(
    Configuration conf, InputJobInfo inputJobInfo, String locationFilter) throws Exception {
    HiveMetaStoreClient client = null;
    HiveConf hiveConf = null;
    try {
      if (conf != null) {
        hiveConf = HCatUtil.getHiveConf(conf);
      } else {
        hiveConf = new HiveConf(HCatInputFormat.class);
      }
      client = HCatUtil.getHiveClient(hiveConf);
      Table table = HCatUtil.getTable(client, inputJobInfo.getDatabaseName(),
        inputJobInfo.getTableName());

      List<PartInfo> partInfoList = new ArrayList<PartInfo>();

      inputJobInfo.setTableInfo(HCatTableInfo.valueOf(table.getTTable()));
      if (table.getPartitionKeys().size() != 0) {
        //Partitioned table
        List<Partition> parts = client.listPartitionsByFilter(inputJobInfo.getDatabaseName(),
          inputJobInfo.getTableName(),
          inputJobInfo.getFilter(),
          (short) -1);

        // Default to 100,000 partitions if hive.metastore.maxpartition is not defined
        int maxPart = hiveConf.getInt("hcat.metastore.maxpartitions", 100000);
        if (parts != null && parts.size() > maxPart) {
          throw new HCatException(ErrorType.ERROR_EXCEED_MAXPART, "total number of partitions is " + parts.size());
        }

        // populate partition info
        for (Partition ptn : parts) {
          HCatSchema schema = HCatUtil.extractSchema(
            new org.apache.hadoop.hive.ql.metadata.Partition(table, ptn));
          PartInfo partInfo = extractPartInfo(schema, ptn.getSd(),
            ptn.getParameters(), conf, inputJobInfo);
          partInfo.setPartitionValues(InternalUtil.createPtnKeyValueMap(table, ptn));
          partInfoList.add(partInfo);
        }

      } else {
        //Non partitioned table
        HCatSchema schema = HCatUtil.extractSchema(table);
        PartInfo partInfo = extractPartInfo(schema, table.getTTable().getSd(),
          table.getParameters(), conf, inputJobInfo);
        partInfo.setPartitionValues(new HashMap<String, String>());
        partInfoList.add(partInfo);
      }
      inputJobInfo.setPartitions(partInfoList);

      return inputJobInfo;
    } finally {
      HCatUtil.closeHiveClientQuietly(client);
    }

  }

  private static PartInfo extractPartInfo(HCatSchema schema, StorageDescriptor sd,
                      Map<String, String> parameters, Configuration conf,
                      InputJobInfo inputJobInfo) throws IOException {

    StorerInfo storerInfo = InternalUtil.extractStorerInfo(sd, parameters);

    Properties hcatProperties = new Properties();
    HiveStorageHandler storageHandler = HCatUtil.getStorageHandler(conf, storerInfo);

    // copy the properties from storageHandler to jobProperties
    Map<String, String> jobProperties = HCatUtil.getInputJobProperties(storageHandler, inputJobInfo);

    for (String key : parameters.keySet()) {
      hcatProperties.put(key, parameters.get(key));
    }
    // FIXME
    // Bloating partinfo with inputJobInfo is not good
    return new PartInfo(schema, storageHandler, sd.getLocation(),
      hcatProperties, jobProperties, inputJobInfo.getTableInfo());
  }

}
