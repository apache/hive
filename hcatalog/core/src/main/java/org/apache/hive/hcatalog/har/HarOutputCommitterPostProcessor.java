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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.har;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatException;

public class HarOutputCommitterPostProcessor {

  boolean isEnabled = false;

  public boolean isEnabled() {
    return isEnabled;
  }

  public void setEnabled(boolean enabled) {
    this.isEnabled = enabled;
  }


  public void exec(JobContext context, Partition partition, Path partPath) throws IOException {
//    LOG.info("Archiving partition ["+partPath.toString()+"]");
    makeHar(context, partPath.toUri().toString(), harFile(partPath));
    partition.getParameters().put(hive_metastoreConstants.IS_ARCHIVED, "true");
  }

  public String harFile(Path ptnPath) throws IOException {
    String harFile = ptnPath.toString().replaceFirst("/+$", "") + ".har";
//    LOG.info("har file : " + harFile);
    return harFile;
  }

  public String getParentFSPath(Path ptnPath) throws IOException {
    return ptnPath.toUri().getPath().replaceFirst("/+$", "");
  }

  public String getProcessedLocation(Path ptnPath) throws IOException {
    String harLocn = ("har://" + ptnPath.toUri().getPath()).replaceFirst("/+$", "") + ".har" + Path.SEPARATOR;
//    LOG.info("har location : " + harLocn);
    return harLocn;
  }


  /**
   * Creates a har file from the contents of a given directory, using that as root.
   * @param dir Directory to archive
   * @param harFile The HAR file to create
   */
  public static void makeHar(JobContext context, String dir, String harFile) throws IOException {
//    Configuration conf = context.getConfiguration();
//    Credentials creds = context.getCredentials();

//    HCatUtil.logAllTokens(LOG,context);

    int lastSep = harFile.lastIndexOf(Path.SEPARATOR_CHAR);
    Path archivePath = new Path(harFile.substring(0, lastSep));
    final String[] args = {
      "-archiveName",
      harFile.substring(lastSep + 1, harFile.length()),
      "-p",
      dir,
      "*",
      archivePath.toString()
    };
//    for (String arg : args){
//      LOG.info("Args to har : "+ arg);
//    }
    try {
      Configuration newConf = new Configuration();
      FileSystem fs = archivePath.getFileSystem(newConf);

      String hadoopTokenFileLocationEnvSetting = System.getenv(HCatConstants.SYSENV_HADOOP_TOKEN_FILE_LOCATION);
      if ((hadoopTokenFileLocationEnvSetting != null) && (!hadoopTokenFileLocationEnvSetting.isEmpty())) {
        newConf.set(HCatConstants.CONF_MAPREDUCE_JOB_CREDENTIALS_BINARY, hadoopTokenFileLocationEnvSetting);
//      LOG.info("System.getenv(\"HADOOP_TOKEN_FILE_LOCATION\") =["+  System.getenv("HADOOP_TOKEN_FILE_LOCATION")+"]");
      }
//      for (FileStatus ds : fs.globStatus(new Path(dir, "*"))){
//        LOG.info("src : "+ds.getPath().toUri().toString());
//      }

      final HadoopArchives har = new HadoopArchives(newConf);
      int rc = ToolRunner.run(har, args);
      if (rc != 0) {
        throw new Exception("Har returned error code " + rc);
      }

//      for (FileStatus hs : fs.globStatus(new Path(harFile, "*"))){
//        LOG.info("dest : "+hs.getPath().toUri().toString());
//      }
//      doHarCheck(fs,harFile);
//      LOG.info("Nuking " + dir);
      fs.delete(new Path(dir), true);
    } catch (Exception e) {
      throw new HCatException("Error creating Har [" + harFile + "] from [" + dir + "]", e);
    }
  }

}
