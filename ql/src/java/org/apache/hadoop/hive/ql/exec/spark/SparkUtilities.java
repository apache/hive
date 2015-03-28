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

import org.apache.commons.io.FilenameUtils;
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
    Path tmpDir = SessionState.getHDFSSessionPath(conf);
    FileSystem fileSystem = FileSystem.get(conf);
    fileSystem.copyFromLocalFile(new Path(source.getPath()), tmpDir);
    String filePath = tmpDir + File.separator + getFileName(source);
    Path fullPath = fileSystem.getFileStatus(new Path(filePath)).getPath();
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

    String name = FilenameUtils.getName(uri.getPath());
    return name;
  }

  public static SparkSession getSparkSession(HiveConf conf,
      SparkSessionManager sparkSessionManager) throws HiveException {
    SparkSession sparkSession = SessionState.get().getSparkSession();

    // Spark configurations are updated close the existing session
    if (conf.getSparkConfigUpdated()) {
      sparkSessionManager.closeSession(sparkSession);
      sparkSession =  null;
      conf.setSparkConfigUpdated(false);
    }
    sparkSession = sparkSessionManager.getSession(sparkSession, conf, true);
    SessionState.get().setSparkSession(sparkSession);
    return sparkSession;
  }
}
