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

import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Contains utilities methods used as part of Spark tasks
 */
public class SparkUtilities {

  // Used to save and retrieve IOContext for multi-insertion.
  public static final String MAP_IO_CONTEXT = "MAP_IO_CONTEXT";

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

  public static URL getURL(String path) throws MalformedURLException {
    if (path == null) {
      return null;
    }

    URL url = null;
    try {
      URI uri = new URI(path);
      if (uri.getScheme() != null) {
        url = uri.toURL();
      } else {
        // if no file schema in path, we assume it's file on local fs.
        url = new File(path).toURI().toURL();
      }
    } catch (URISyntaxException e) {
      // do nothing here, just return null if input path is not a valid URI.
    }

    return url;
  }
}
