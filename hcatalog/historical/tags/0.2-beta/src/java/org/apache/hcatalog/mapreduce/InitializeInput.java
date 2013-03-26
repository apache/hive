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
package org.apache.hcatalog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hcatalog.common.ErrorType;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.schema.HCatSchema;

/**
 * The Class which handles querying the metadata server using the MetaStoreClient. The list of
 * partitions matching the partition filter is fetched from the server and the information is
 * serialized and written into the JobContext configuration. The inputInfo is also updated with
 * info required in the client process context.
 */
public class InitializeInput {

  /** The prefix for keys used for storage driver arguments */
  static final String HCAT_KEY_PREFIX = "hcat.";
  private static final HiveConf hiveConf = new HiveConf(HCatInputFormat.class);

  private static HiveMetaStoreClient createHiveMetaClient(Configuration conf, HCatTableInfo inputInfo) throws Exception {
    if (inputInfo.getServerUri() != null){

      hiveConf.setBoolean(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, true);
      hiveConf.set(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname,
          inputInfo.getServerKerberosPrincipal());

      hiveConf.set("hive.metastore.local", "false");
      hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, inputInfo.getServerUri());
    }

    return new HiveMetaStoreClient(hiveConf,null);
  }

  /**
   * Set the input to use for the Job. This queries the metadata server with the specified partition predicates,
   * gets the matching partitions, puts the information in the configuration object.
   * @param job the job object
   * @param inputInfo the hcat table input info
   * @throws Exception
   */
  public static void setInput(Job job, HCatTableInfo inputInfo) throws Exception {

    //* Create and initialize an JobInfo object
    //* Serialize the JobInfo and save in the Job's Configuration object

    HiveMetaStoreClient client = null;

    try {
      client = createHiveMetaClient(job.getConfiguration(),inputInfo);
      Table table = client.getTable(inputInfo.getDatabaseName(), inputInfo.getTableName());
      HCatSchema tableSchema = HCatUtil.getTableSchemaWithPtnCols(table);

      List<PartInfo> partInfoList = new ArrayList<PartInfo>();

      if( table.getPartitionKeys().size() != 0 ) {
        //Partitioned table
        List<Partition> parts = client.listPartitionsByFilter(
            inputInfo.getDatabaseName(), inputInfo.getTableName(),
            inputInfo.getFilter(), (short) -1);

        // Default to 100,000 partitions if hive.metastore.maxpartition is not defined
        int maxPart = hiveConf.getInt("hcat.metastore.maxpartitions", 100000);
        if (parts != null && parts.size() > maxPart) {
          throw new HCatException(ErrorType.ERROR_EXCEED_MAXPART, "total number of partitions is " + parts.size());
        }

        // populate partition info
        for (Partition ptn : parts){
          PartInfo partInfo = extractPartInfo(ptn.getSd(),ptn.getParameters());
          partInfo.setPartitionValues(createPtnKeyValueMap(table,ptn));
          partInfoList.add(partInfo);
        }

      }else{
        //Non partitioned table
        PartInfo partInfo = extractPartInfo(table.getSd(),table.getParameters());
        partInfo.setPartitionValues(new HashMap<String,String>());
        partInfoList.add(partInfo);
      }

      JobInfo hcatJobInfo = new JobInfo(inputInfo, tableSchema, partInfoList);
      inputInfo.setJobInfo(hcatJobInfo);

      job.getConfiguration().set(
          HCatConstants.HCAT_KEY_JOB_INFO,
          HCatUtil.serialize(hcatJobInfo)
      );
    } finally {
      if (client != null ) {
        client.close();
      }
    }
  }

  private static Map<String, String> createPtnKeyValueMap(Table table, Partition ptn) throws IOException{
    List<String> values = ptn.getValues();
    if( values.size() != table.getPartitionKeys().size() ) {
      throw new IOException("Partition values in partition inconsistent with table definition, table "
          + table.getTableName() + " has "
          + table.getPartitionKeys().size()
          + " partition keys, partition has " + values.size() + "partition values" );
    }

    Map<String,String> ptnKeyValues = new HashMap<String,String>();

    int i = 0;
    for(FieldSchema schema : table.getPartitionKeys()) {
      // CONCERN : the way this mapping goes, the order *needs* to be preserved for table.getPartitionKeys() and ptn.getValues()
      ptnKeyValues.put(schema.getName().toLowerCase(), values.get(i));
      i++;
    }

    return ptnKeyValues;
  }

  static PartInfo extractPartInfo(StorageDescriptor sd, Map<String,String> parameters) throws IOException{
    HCatSchema schema = HCatUtil.extractSchemaFromStorageDescriptor(sd);
    String inputStorageDriverClass = null;
    Properties hcatProperties = new Properties();
    if (parameters.containsKey(HCatConstants.HCAT_ISD_CLASS)){
      inputStorageDriverClass = parameters.get(HCatConstants.HCAT_ISD_CLASS);
    }else{
      // attempt to default to RCFile if the storage descriptor says it's an RCFile
      if ((sd.getInputFormat() != null) && (sd.getInputFormat().equals(HCatConstants.HIVE_RCFILE_IF_CLASS))){
        inputStorageDriverClass = HCatConstants.HCAT_RCFILE_ISD_CLASS;
      }else{
        throw new IOException("No input storage driver classname found, cannot read partition");
      }
    }
    for (String key : parameters.keySet()){
      if (key.startsWith(HCAT_KEY_PREFIX)){
        hcatProperties.put(key, parameters.get(key));
      }
    }
    return new PartInfo(schema,inputStorageDriverClass,  sd.getLocation(), hcatProperties);
  }



  static StorerInfo extractStorerInfo(StorageDescriptor sd, Map<String, String> properties) throws IOException {
    String inputSDClass, outputSDClass;

    if (properties.containsKey(HCatConstants.HCAT_ISD_CLASS)){
      inputSDClass = properties.get(HCatConstants.HCAT_ISD_CLASS);
    }else{
      // attempt to default to RCFile if the storage descriptor says it's an RCFile
      if ((sd.getInputFormat() != null) && (sd.getInputFormat().equals(HCatConstants.HIVE_RCFILE_IF_CLASS))){
        inputSDClass = HCatConstants.HCAT_RCFILE_ISD_CLASS;
      }else{
        throw new IOException("No input storage driver classname found for table, cannot write partition");
      }
    }

    if (properties.containsKey(HCatConstants.HCAT_OSD_CLASS)){
      outputSDClass = properties.get(HCatConstants.HCAT_OSD_CLASS);
    }else{
      // attempt to default to RCFile if the storage descriptor says it's an RCFile
      if ((sd.getOutputFormat() != null) && (sd.getOutputFormat().equals(HCatConstants.HIVE_RCFILE_OF_CLASS))){
        outputSDClass = HCatConstants.HCAT_RCFILE_OSD_CLASS;
      }else{
        throw new IOException("No output storage driver classname found for table, cannot write partition");
      }
    }

    Properties hcatProperties = new Properties();
    for (String key : properties.keySet()){
      if (key.startsWith(HCAT_KEY_PREFIX)){
        hcatProperties.put(key, properties.get(key));
      }
    }

    return new StorerInfo(inputSDClass, outputSDClass, hcatProperties);
  }

}
