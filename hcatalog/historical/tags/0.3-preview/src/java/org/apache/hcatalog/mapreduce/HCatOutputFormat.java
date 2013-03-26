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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hcatalog.common.ErrorType;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;

/** The OutputFormat to use to write data to HCat. The key value is ignored and
 * and should be given as null. The value is the HCatRecord to write.*/
public class HCatOutputFormat extends HCatBaseOutputFormat {

    static final private Log LOG = LogFactory.getLog(HCatOutputFormat.class);

    private static int maxDynamicPartitions;
    private static boolean harRequested;

    /**
     * Set the info about the output to write for the Job. This queries the metadata server
     * to find the StorageDriver to use for the table.  Throws error if partition is already published.
     * @param job the job object
     * @param outputJobInfo the table output info
     * @throws IOException the exception in communicating with the metadata server
     */
    @SuppressWarnings("unchecked")
    public static void setOutput(Job job, OutputJobInfo outputJobInfo) throws IOException {
      HiveMetaStoreClient client = null;

      try {

        Configuration conf = job.getConfiguration();
        client = createHiveClient(outputJobInfo.getServerUri(), conf);
        Table table = client.getTable(outputJobInfo.getDatabaseName(), outputJobInfo.getTableName());

        if (table.getPartitionKeysSize() == 0 ){
          if ((outputJobInfo.getPartitionValues() != null) && (!outputJobInfo.getPartitionValues().isEmpty())){
            // attempt made to save partition values in non-partitioned table - throw error.
            throw new HCatException(ErrorType.ERROR_INVALID_PARTITION_VALUES,
                "Partition values specified for non-partitioned table");
          }
          // non-partitioned table
          outputJobInfo.setPartitionValues(new HashMap<String, String>());

        } else {
          // partitioned table, we expect partition values
          // convert user specified map to have lower case key names
          Map<String, String> valueMap = new HashMap<String, String>();
          if (outputJobInfo.getPartitionValues() != null){
            for(Map.Entry<String, String> entry : outputJobInfo.getPartitionValues().entrySet()) {
              valueMap.put(entry.getKey().toLowerCase(), entry.getValue());
            }
          }

          if (
              (outputJobInfo.getPartitionValues() == null)
              || (outputJobInfo.getPartitionValues().size() < table.getPartitionKeysSize())
          ){
            // dynamic partition usecase - partition values were null, or not all were specified
            // need to figure out which keys are not specified.
            List<String> dynamicPartitioningKeys = new ArrayList<String>();
            boolean firstItem = true;
            for (FieldSchema fs : table.getPartitionKeys()){
              if (!valueMap.containsKey(fs.getName().toLowerCase())){
                dynamicPartitioningKeys.add(fs.getName().toLowerCase());
              }
            }

            if (valueMap.size() + dynamicPartitioningKeys.size() != table.getPartitionKeysSize()){
              // If this isn't equal, then bogus key values have been inserted, error out.
              throw new HCatException(ErrorType.ERROR_INVALID_PARTITION_VALUES,"Invalid partition keys specified");
            }

            outputJobInfo.setDynamicPartitioningKeys(dynamicPartitioningKeys);
            String dynHash;
            if ((dynHash = conf.get(HCatConstants.HCAT_DYNAMIC_PTN_JOBID)) == null){
              dynHash = String.valueOf(Math.random());
//              LOG.info("New dynHash : ["+dynHash+"]");
//            }else{
//              LOG.info("Old dynHash : ["+dynHash+"]");
            }
            conf.set(HCatConstants.HCAT_DYNAMIC_PTN_JOBID, dynHash);

          }

          outputJobInfo.setPartitionValues(valueMap);
        }

        StorageDescriptor tblSD = table.getSd();
        HCatSchema tableSchema = HCatUtil.extractSchemaFromStorageDescriptor(tblSD);
        StorerInfo storerInfo = InitializeInput.extractStorerInfo(tblSD,table.getParameters());

        List<String> partitionCols = new ArrayList<String>();
        for(FieldSchema schema : table.getPartitionKeys()) {
          partitionCols.add(schema.getName());
        }

        Class<? extends HCatOutputStorageDriver> driverClass =
          (Class<? extends HCatOutputStorageDriver>) Class.forName(storerInfo.getOutputSDClass());
        HCatOutputStorageDriver driver = driverClass.newInstance();

        String tblLocation = tblSD.getLocation();
        String location = driver.getOutputLocation(job,
            tblLocation, partitionCols,
            outputJobInfo.getPartitionValues(),conf.get(HCatConstants.HCAT_DYNAMIC_PTN_JOBID));

        //Serialize the output info into the configuration
        outputJobInfo.setTableInfo(HCatTableInfo.valueOf(table));
        outputJobInfo.setOutputSchema(tableSchema);
        outputJobInfo.setLocation(location);
        outputJobInfo.setHarRequested(harRequested);
        outputJobInfo.setMaximumDynamicPartitions(maxDynamicPartitions);
        conf.set(HCatConstants.HCAT_KEY_OUTPUT_INFO, HCatUtil.serialize(outputJobInfo));

        Path tblPath = new Path(tblLocation);

        /*  Set the umask in conf such that files/dirs get created with table-dir
         * permissions. Following three assumptions are made:
         * 1. Actual files/dirs creation is done by RecordWriter of underlying
         * output format. It is assumed that they use default permissions while creation.
         * 2. Default Permissions = FsPermission.getDefault() = 777.
         * 3. UMask is honored by underlying filesystem.
         */

        FsPermission.setUMask(conf, FsPermission.getDefault().applyUMask(
            tblPath.getFileSystem(conf).getFileStatus(tblPath).getPermission()));

        try {
          UserGroupInformation.class.getMethod("isSecurityEnabled");
          Security.getInstance().handleSecurity(job, outputJobInfo, client, conf, harRequested);
        } catch (NoSuchMethodException e) {
          LOG.info("Security is not supported by this version of hadoop.");
        }
      } catch(Exception e) {
        if( e instanceof HCatException ) {
          throw (HCatException) e;
        } else {
          throw new HCatException(ErrorType.ERROR_SET_OUTPUT, e);
        }
      } finally {
        if( client != null ) {
          client.close();
        }
//        HCatUtil.logAllTokens(LOG,job);
      }
    }

    /**
     * Set the schema for the data being written out to the partition. The
     * table schema is used by default for the partition if this is not called.
     * @param job the job object
     * @param schema the schema for the data
     */
    public static void setSchema(final Job job, final HCatSchema schema) throws IOException {

        OutputJobInfo jobInfo = getJobInfo(job);
        Map<String,String> partMap = jobInfo.getPartitionValues();
        setPartDetails(jobInfo, schema, partMap);
        job.getConfiguration().set(HCatConstants.HCAT_KEY_OUTPUT_INFO, HCatUtil.serialize(jobInfo));
    }

    /**
     * Get the record writer for the job. Uses the Table's default OutputStorageDriver
     * to get the record writer.
     * @param context the information about the current task.
     * @return a RecordWriter to write the output for the job.
     * @throws IOException
     */
    @Override
    public RecordWriter<WritableComparable<?>, HCatRecord>
      getRecordWriter(TaskAttemptContext context
                      ) throws IOException, InterruptedException {
      return getOutputFormat(context).getRecordWriter(context);
    }


    /**
     * Get the output committer for this output format. This is responsible
     * for ensuring the output is committed correctly.
     * @param context the task context
     * @return an output committer
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context
                                       ) throws IOException, InterruptedException {
        return getOutputFormat(context).getOutputCommitter(context);
    }

    static HiveMetaStoreClient createHiveClient(String url, Configuration conf) throws IOException, MetaException {
      HiveConf hiveConf = getHiveConf(url, conf);
//      HCatUtil.logHiveConf(LOG, hiveConf);
      try {
        return new HiveMetaStoreClient(hiveConf);
      } catch (MetaException e) {
        LOG.error("Error connecting to the metastore (conf follows): "+e.getMessage(), e);
        HCatUtil.logHiveConf(LOG, hiveConf);
        throw e;
      }
    }


    static HiveConf getHiveConf(String url, Configuration conf) throws IOException {
      HiveConf hiveConf = new HiveConf(HCatOutputFormat.class);

      if( url != null ) {
        //User specified a thrift url

        hiveConf.set("hive.metastore.local", "false");
        hiveConf.set(ConfVars.METASTOREURIS.varname, url);

        String kerberosPrincipal = conf.get(HCatConstants.HCAT_METASTORE_PRINCIPAL);
        if (kerberosPrincipal == null){
            kerberosPrincipal = conf.get(ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname);
        }
        if (kerberosPrincipal != null){
            hiveConf.setBoolean(ConfVars.METASTORE_USE_THRIFT_SASL.varname, true);
            hiveConf.set(ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname, kerberosPrincipal);
        }
      } else {
        //Thrift url is null, copy the hive conf into the job conf and restore it
        //in the backend context

        if( conf.get(HCatConstants.HCAT_KEY_HIVE_CONF) == null ) {
          conf.set(HCatConstants.HCAT_KEY_HIVE_CONF, HCatUtil.serialize(hiveConf.getAllProperties()));
        } else {
          //Copy configuration properties into the hive conf
          Properties properties = (Properties) HCatUtil.deserialize(conf.get(HCatConstants.HCAT_KEY_HIVE_CONF));

          for(Map.Entry<Object, Object> prop : properties.entrySet() ) {
            if( prop.getValue() instanceof String ) {
              hiveConf.set((String) prop.getKey(), (String) prop.getValue());
            } else if( prop.getValue() instanceof Integer ) {
              hiveConf.setInt((String) prop.getKey(), (Integer) prop.getValue());
            } else if( prop.getValue() instanceof Boolean ) {
              hiveConf.setBoolean((String) prop.getKey(), (Boolean) prop.getValue());
            } else if( prop.getValue() instanceof Long ) {
              hiveConf.setLong((String) prop.getKey(), (Long) prop.getValue());
            } else if( prop.getValue() instanceof Float ) {
              hiveConf.setFloat((String) prop.getKey(), (Float) prop.getValue());
            }
          }
        }

      }

      if(conf.get(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE) != null) {
        hiveConf.set("hive.metastore.token.signature", conf.get(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE));
      }

      // figure out what the maximum number of partitions allowed is, so we can pass it on to our outputinfo
      if (HCatConstants.HCAT_IS_DYNAMIC_MAX_PTN_CHECK_ENABLED){
        maxDynamicPartitions = hiveConf.getIntVar(HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS);
      }else{
        maxDynamicPartitions = -1; // disables bounds checking for maximum number of dynamic partitions
      }
      harRequested = hiveConf.getBoolVar(HiveConf.ConfVars.HIVEARCHIVEENABLED);
      return hiveConf;
    }

}
