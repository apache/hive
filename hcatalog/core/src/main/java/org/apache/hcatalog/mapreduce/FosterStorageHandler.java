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

package org.apache.hcatalog.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *  This class is used to encapsulate the InputFormat, OutputFormat and SerDe
 *  artifacts of tables which don't define a SerDe. This StorageHandler assumes
 *  the supplied storage artifacts are for a file-based storage system.
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.mapreduce.FosterStorageHandler} instead
 */
public class FosterStorageHandler extends HCatStorageHandler {

  public Configuration conf;
  /** The directory under which data is initially written for a partitioned table */
  protected static final String DYNTEMP_DIR_NAME = "_DYN";

  /** The directory under which data is initially written for a non partitioned table */
  protected static final String TEMP_DIR_NAME = "_TEMP";

  private Class<? extends InputFormat> ifClass;
  private Class<? extends OutputFormat> ofClass;
  private Class<? extends SerDe> serDeClass;

  public FosterStorageHandler(String ifName, String ofName, String serdeName) throws ClassNotFoundException {
    this((Class<? extends InputFormat>) Class.forName(ifName),
      (Class<? extends OutputFormat>) Class.forName(ofName),
      (Class<? extends SerDe>) Class.forName(serdeName));
  }

  public FosterStorageHandler(Class<? extends InputFormat> ifClass,
                Class<? extends OutputFormat> ofClass,
                Class<? extends SerDe> serDeClass) {
    this.ifClass = ifClass;
    this.ofClass = ofClass;
    this.serDeClass = serDeClass;
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return ifClass;    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return ofClass;    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return serDeClass;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return null;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc,
                      Map<String, String> jobProperties) {

  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc,
                       Map<String, String> jobProperties) {
    try {
      OutputJobInfo jobInfo = (OutputJobInfo)
        HCatUtil.deserialize(tableDesc.getJobProperties().get(
          HCatConstants.HCAT_KEY_OUTPUT_INFO));
      String parentPath = jobInfo.getTableInfo().getTableLocation();
      String dynHash = tableDesc.getJobProperties().get(
        HCatConstants.HCAT_DYNAMIC_PTN_JOBID);

      // For dynamic partitioned writes without all keyvalues specified,
      // we create a temp dir for the associated write job
      if (dynHash != null) {
        parentPath = new Path(parentPath,
          DYNTEMP_DIR_NAME + dynHash).toString();
      }

      String outputLocation;

      if (Boolean.valueOf((String)tableDesc.getProperties().get("EXTERNAL"))
           && jobInfo.getLocation() != null && jobInfo.getLocation().length() > 0) {
        // honor external table that specifies the location
        outputLocation = jobInfo.getLocation();
      } else if (dynHash == null && jobInfo.getPartitionValues().size() == 0) {
        // For non-partitioned tables, we send them to the temp dir
        outputLocation = TEMP_DIR_NAME;
      } else {
        List<String> cols = new ArrayList<String>();
        List<String> values = new ArrayList<String>();

        //Get the output location in the order partition keys are defined for the table.
        for (String name :
          jobInfo.getTableInfo().
            getPartitionColumns().getFieldNames()) {
          String value = jobInfo.getPartitionValues().get(name);
          cols.add(name);
          values.add(value);
        }
        outputLocation = FileUtils.makePartName(cols, values);
      }

      jobInfo.setLocation(new Path(parentPath, outputLocation).toString());

      //only set output dir if partition is fully materialized
      if (jobInfo.getPartitionValues().size()
        == jobInfo.getTableInfo().getPartitionColumns().size()) {
        jobProperties.put("mapred.output.dir", jobInfo.getLocation());
      }

      //TODO find a better home for this, RCFile specifc
      jobProperties.put(RCFile.COLUMN_NUMBER_CONF_STR,
        Integer.toOctalString(
          jobInfo.getOutputSchema().getFields().size()));
      jobProperties.put(HCatConstants.HCAT_KEY_OUTPUT_INFO,
        HCatUtil.serialize(jobInfo));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to set output path", e);
    }

  }

  @Override
  OutputFormatContainer getOutputFormatContainer(
    org.apache.hadoop.mapred.OutputFormat outputFormat) {
    return new FileOutputFormatContainer(outputFormat);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider()
    throws HiveException {
    return new DefaultHiveAuthorizationProvider();
  }
  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    //do nothing by default
    //EK: added the same (no-op) implementation as in
    // org.apache.hive.hcatalog.DefaultStorageHandler (hive 0.12)
    // this is needed to get 0.11 API compat layer to work
    // see HIVE-4896
  }

}
