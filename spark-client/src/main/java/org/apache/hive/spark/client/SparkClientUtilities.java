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
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SparkClientUtilities {
  protected static final transient Log LOG = LogFactory.getLog(SparkClientUtilities.class);

  /**
   * Add new elements to the classpath.
   *
   * @param newPaths Array of classpath elements
   */
  public static void addToClassPath(String[] newPaths) throws Exception {
    ClassLoader cloader = Thread.currentThread().getContextClassLoader();
    URLClassLoader loader = (URLClassLoader) cloader;
    List<URL> curPath = Lists.newArrayList(loader.getURLs());

    for (String newPath : newPaths) {
      URL newUrl = urlFromPathString(newPath);
      if (newUrl != null && !curPath.contains(newUrl)) {
        curPath.add(newUrl);
        LOG.info("Added jar[" + newUrl + "] to classpath.");
      }
    }

    URLClassLoader newLoader = new URLClassLoader(curPath.toArray(new URL[curPath.size()]), loader);
    Thread.currentThread().setContextClassLoader(newLoader);
  }

  /**
   * Create a URL from a string representing a path to a local file.
   * The path string can be just a path, or can start with file:/, file:///
   * @param path  path string
   * @return
   */
  private static URL urlFromPathString(String path) {
    URL url = null;
    try {
      if (StringUtils.indexOf(path, "file:/") == 0) {
        url = new URL(path);
      } else {
        url = new File(path).toURL();
      }
    } catch (Exception err) {
      LOG.error("Bad URL " + path + ", ignoring path");
    }
    return url;
  }
}
