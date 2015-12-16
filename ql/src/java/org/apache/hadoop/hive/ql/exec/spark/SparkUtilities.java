/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManager;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.SparkConf;

/**
 * Contains utilities methods used as part of Spark tasks.
 */
public class SparkUtilities {

  public static HiveKey copyHiveKey(HiveKey key) {
    HiveKey copy = new HiveKey();
    copy.setDistKeyLength(key.getDistKeyLength());
    copy.setHashCode(key.hashCode());
    copy.set(key);
    return copy;
  }

  public static BytesWritable copyBytesWritable(BytesWritable bw) {
    BytesWritable copy = new BytesWritable();
    copy.set(bw);
    return copy;
  }

  public static URI getURI(String path) throws URISyntaxException {
    if (path == null) {
      return null;
    }

      URI uri = new URI(path);
      if (uri.getScheme() == null) {
        // if no file schema in path, we assume it's file on local fs.
        uri = new File(path).toURI();
      }

    return uri;
  }

  /**
   * Uploads a local file to HDFS
   *
   * @param source
   * @param conf
   * @return
   * @throws IOException
   */
  public static URI uploadToHDFS(URI source, HiveConf conf) throws IOException {
    Path localFile = new Path(source.getPath());
    Path remoteFile = new Path(SessionState.get().getSparkSession().getHDFSSessionDir(),
        getFileName(source));
    FileSystem fileSystem = FileSystem.get(conf);
    // Overwrite if the remote file already exists. Whether the file can be added
    // on executor is up to spark, i.e. spark.files.overwrite
    fileSystem.copyFromLocalFile(false, true, localFile, remoteFile);
    Path fullPath = fileSystem.getFileStatus(remoteFile).getPath();
    return fullPath.toUri();
  }

  // checks if a resource has to be uploaded to HDFS for yarn-cluster mode
  public static boolean needUploadToHDFS(URI source, SparkConf sparkConf) {
    return sparkConf.get("spark.master").equals("yarn-cluster") &&
        !source.getScheme().equals("hdfs");
  }

  private static String getFileName(URI uri) {
    if (uri == null) {
      return null;
    }

    String[] splits = uri.getPath().split(File.separator);
    return  splits[splits.length-1];
  }

  public static boolean isDedicatedCluster(Configuration conf) {
    String master = conf.get("spark.master");
    return master.startsWith("yarn-") || master.startsWith("local");
  }

  public static SparkSession getSparkSession(HiveConf conf,
      SparkSessionManager sparkSessionManager) throws HiveException {
    SparkSession sparkSession = SessionState.get().getSparkSession();
    HiveConf sessionConf = SessionState.get().getConf();

    // Spark configurations are updated close the existing session
    // In case of async queries or confOverlay is not empty,
    // sessionConf and conf are different objects
    if (sessionConf.getSparkConfigUpdated() || conf.getSparkConfigUpdated()) {
      sparkSessionManager.closeSession(sparkSession);
      sparkSession =  null;
      conf.setSparkConfigUpdated(false);
      sessionConf.setSparkConfigUpdated(false);
    }
    sparkSession = sparkSessionManager.getSession(sparkSession, conf, true);
    SessionState.get().setSparkSession(sparkSession);
    return sparkSession;
  }
}
