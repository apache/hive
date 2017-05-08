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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.spark.client;

import com.google.common.collect.Lists;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SparkClientUtilities {
  protected static final transient Logger LOG = LoggerFactory.getLogger(SparkClientUtilities.class);

  private static final Map<String, Long> downloadedFiles = new ConcurrentHashMap<>();

  /**
   * Add new elements to the classpath.
   *
   * @param newPaths Map of classpath elements and corresponding timestamp
   * @return locally accessible files corresponding to the newPaths
   */
  public static List<String> addToClassPath(Map<String, Long> newPaths, Configuration conf,
      File localTmpDir) throws Exception {
    URLClassLoader loader = (URLClassLoader) Thread.currentThread().getContextClassLoader();
    List<URL> curPath = Lists.newArrayList(loader.getURLs());
    List<String> localNewPaths = new ArrayList<>();

    boolean newPathAdded = false;
    for (Map.Entry<String, Long> entry : newPaths.entrySet()) {
      URL newUrl = urlFromPathString(entry.getKey(), entry.getValue(), conf, localTmpDir);
      localNewPaths.add(newUrl.toString());
      if (newUrl != null && !curPath.contains(newUrl)) {
        curPath.add(newUrl);
        LOG.info("Added jar[" + newUrl + "] to classpath.");
        newPathAdded = true;
      }
    }

    if (newPathAdded) {
      URLClassLoader newLoader =
          new URLClassLoader(curPath.toArray(new URL[curPath.size()]), loader);
      Thread.currentThread().setContextClassLoader(newLoader);
    }
    return localNewPaths;
  }

  /**
   * Create a URL from a string representing a path to a local file.
   * The path string can be just a path, or can start with file:/, file:///
   * @param path  path string
   * @return
   */
  private static URL urlFromPathString(String path, Long timeStamp,
      Configuration conf, File localTmpDir) {
    URL url = null;
    try {
      if (StringUtils.indexOf(path, "file:/") == 0) {
        url = new URL(path);
      } else if (StringUtils.indexOf(path, "hdfs:/") == 0) {
        Path remoteFile = new Path(path);
        Path localFile =
            new Path(localTmpDir.getAbsolutePath() + File.separator + remoteFile.getName());
        Long currentTS = downloadedFiles.get(path);
        if (currentTS == null) {
          currentTS = -1L;
        }
        if (!new File(localFile.toString()).exists() || currentTS < timeStamp) {
          LOG.info("Copying " + remoteFile + " to " + localFile);
          FileSystem remoteFS = remoteFile.getFileSystem(conf);
          remoteFS.copyToLocalFile(remoteFile, localFile);
          downloadedFiles.put(path, timeStamp);
        }
        return urlFromPathString(localFile.toString(), timeStamp, conf, localTmpDir);
      } else {
        url = new File(path).toURL();
      }
    } catch (Exception err) {
      LOG.error("Bad URL " + path + ", ignoring path", err);
    }
    return url;
  }

  public static boolean isYarnClusterMode(String master, String deployMode) {
    return "yarn-cluster".equals(master) ||
        ("yarn".equals(master) && "cluster".equals(deployMode));
  }

  public static boolean isYarnClientMode(String master, String deployMode) {
    return "yarn-client".equals(master) ||
        ("yarn".equals(master) && "client".equals(deployMode));
  }

  public static boolean isYarnMaster(String master) {
    return master != null && master.startsWith("yarn");
  }

  public static boolean isLocalMaster(String master) {
    return master != null && master.startsWith("local");
  }

  public static String getDeployModeFromMaster(String master) {
    if (master != null) {
      if (master.equals("yarn-client")) {
        return "client";
      } else if (master.equals("yarn-cluster")) {
        return "cluster";
      }
    }
    return null;
  }
}
