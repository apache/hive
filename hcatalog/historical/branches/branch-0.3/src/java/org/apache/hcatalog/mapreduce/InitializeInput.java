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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
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
  
  private static final Log LOG = LogFactory.getLog(InitializeInput.class);

  /** The prefix for keys used for storage driver arguments */
  static final String HCAT_KEY_PREFIX = "hcat.";
  private static HiveConf hiveConf;

  private static HiveMetaStoreClient createHiveMetaClient(Configuration conf, InputJobInfo inputJobInfo) throws Exception {

      hiveConf = getHiveConf(inputJobInfo, conf);
      return new HiveMetaStoreClient(hiveConf, null);
  }

  /**
   * Set the input to use for the Job. This queries the metadata server with the specified partition predicates,
   * gets the matching partitions, puts the information in the configuration object.
   * @param job the job object
   * @param inputJobInfo information on the Input to read
   * @throws Exception
   */
  public static void setInput(Job job, InputJobInfo inputJobInfo) throws Exception {

    //* Create and initialize an InputJobInfo object
    //* Serialize the InputJobInfo and save in the Job's Configuration object

    job.getConfiguration().set(
        HCatConstants.HCAT_KEY_JOB_INFO, 
        getSerializedHcatKeyJobInfo(job, inputJobInfo,null));
  }

  public static String getSerializedHcatKeyJobInfo(Job job, InputJobInfo inputJobInfo, String locationFilter) throws Exception {
    //* Create and initialize an InputJobInfo object

    HiveMetaStoreClient client = null;

    try {
      if (job != null){
        client = createHiveMetaClient(job.getConfiguration(),inputJobInfo);
      } else {
        hiveConf = new HiveConf(HCatInputFormat.class);
        client = new HiveMetaStoreClient(hiveConf, null);
      }
      Table table = client.getTable(inputJobInfo.getDatabaseName(),
                                                inputJobInfo.getTableName());

      List<PartInfo> partInfoList = new ArrayList<PartInfo>();

      if( table.getPartitionKeys().size() != 0 ) {
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
      inputJobInfo.setPartitions(partInfoList);
      inputJobInfo.setTableInfo(HCatTableInfo.valueOf(table));

      return HCatUtil.serialize(inputJobInfo);
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

    static HiveConf getHiveConf(InputJobInfo iInfo, Configuration conf)
            throws IOException {

        HiveConf hiveConf = new HiveConf(HCatInputFormat.class);

        if (iInfo.getServerUri() != null) {
            // User specified a thrift url

            hiveConf.set("hive.metastore.local", "false");
            hiveConf.set(ConfVars.METASTOREURIS.varname, iInfo.getServerUri());

            String kerberosPrincipal = iInfo.getServerKerberosPrincipal();
            if (kerberosPrincipal != null) {
                hiveConf.setBoolean(
                        HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname,
                        true);
                hiveConf.set(
                        HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname,
                        kerberosPrincipal);
            } else {

                kerberosPrincipal = conf
                        .get(HCatConstants.HCAT_METASTORE_PRINCIPAL);

                if (kerberosPrincipal == null) {
                    kerberosPrincipal = conf
                            .get(ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname);
                }
                if (kerberosPrincipal != null) {
                    hiveConf.setBoolean(
                            ConfVars.METASTORE_USE_THRIFT_SASL.varname, true);
                    hiveConf.set(ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname,
                            kerberosPrincipal);
                }

                if (conf.get(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE) != null) {
                    hiveConf.set("hive.metastore.token.signature",
                            conf.get(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE));
                }
            }

        } else {
            // Thrift url is null, copy the hive conf into the job conf and
            // restore it
            // in the backend context

            if (conf.get(HCatConstants.HCAT_KEY_HIVE_CONF) == null) {
                conf.set(HCatConstants.HCAT_KEY_HIVE_CONF,
                        HCatUtil.serialize(hiveConf.getAllProperties()));
            } else {
                // Copy configuration properties into the hive conf
                Properties properties = (Properties) HCatUtil.deserialize(conf
                        .get(HCatConstants.HCAT_KEY_HIVE_CONF));

                for (Map.Entry<Object, Object> prop : properties.entrySet()) {
                    if (prop.getValue() instanceof String) {
                        hiveConf.set((String) prop.getKey(),
                                (String) prop.getValue());
                    } else if (prop.getValue() instanceof Integer) {
                        hiveConf.setInt((String) prop.getKey(),
                                (Integer) prop.getValue());
                    } else if (prop.getValue() instanceof Boolean) {
                        hiveConf.setBoolean((String) prop.getKey(),
                                (Boolean) prop.getValue());
                    } else if (prop.getValue() instanceof Long) {
                        hiveConf.setLong((String) prop.getKey(),
                                (Long) prop.getValue());
                    } else if (prop.getValue() instanceof Float) {
                        hiveConf.setFloat((String) prop.getKey(),
                                (Float) prop.getValue());
                    }
                }
            }

        }
        return hiveConf;
    }

}
