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

package org.apache.hadoop.hive.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;


public class StorageUtils {

  /**
   * Returns true if permissions should be set on the given filesystem, false otherwise. Certain implementations of
   * {@link FileSystem} don't have a concept of permissions, such as S3. This method checks to determine if the given
   * {@link FileSystem} falls into that category.
   *
   * @param conf the {@link Configuration} to use when checking if permissions should be set on the {@link FileSystem}
   * @param fs the {@link FileSystem} to check to see if permission should be set or not
   *
   * @return true if permissions should be set on the given {@link FileSystem}, false otherwise
   */
  public static boolean shouldSetPerms(Configuration conf, FileSystem fs) {
      return !(BlobStorageUtils.areOptimizationsEnabled(conf) && BlobStorageUtils.isBlobStorageFileSystem(conf, fs));
  }
}
