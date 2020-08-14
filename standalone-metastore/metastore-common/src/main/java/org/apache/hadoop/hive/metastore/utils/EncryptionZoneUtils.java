/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;

import java.io.IOException;
import java.net.URI;

public class EncryptionZoneUtils {

  private EncryptionZoneUtils() {

  }

  public static boolean isPathEncrypted(Path path, Configuration conf) throws IOException {
    Path fullPath;
    if (path.isAbsolute()) {
      fullPath = path;
    } else {
      fullPath = path.getFileSystem(conf).makeQualified(path);
    }
    return (EncryptionZoneUtils.getEncryptionZoneForPath(fullPath, conf) != null);
  }

  public static EncryptionZone getEncryptionZoneForPath(Path path, Configuration conf) throws IOException {
    URI uri = path.getFileSystem(conf).getUri();
    if ("hdfs".equals(uri.getScheme())) {
      HdfsAdmin hdfsAdmin = new HdfsAdmin(uri, conf);
      if (path.getFileSystem(conf).exists(path)) {
        return hdfsAdmin.getEncryptionZoneForPath(path);
      } else if (!path.getParent().equals(path)) {
        return getEncryptionZoneForPath(path.getParent(), conf);
      } else {
        return null;
      }
    }
    return null;
  }

  public static void createEncryptionZone(Path path, String keyName, Configuration conf) throws IOException {
    URI uri = path.getFileSystem(conf).getUri();
    if ("hdfs".equals(uri.getScheme())) {
      HdfsAdmin hdfsAdmin = new HdfsAdmin(uri, conf);
      hdfsAdmin.createEncryptionZone(path, keyName);
    } else {
      throw new UnsupportedOperationException("Cannot create encryption zone for scheme {}" + uri.getScheme());
    }
  }
}
