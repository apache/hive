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

package org.apache.hadoop.hive.shims;

import org.apache.hadoop.fs.HarFileSystem;

/**
 * HiveHarFileSystem - fixes issue with block locations
 *
 */
public class HiveHarFileSystem extends HarFileSystem {
  /*
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
      long len) throws IOException {

    // In some places (e.g. FileInputFormat) this BlockLocation is used to
    // figure out sizes/offsets and so a completely blank one will not work.
    String [] hosts = {"DUMMY_HOST"};
    return new BlockLocation[]{new BlockLocation(null, hosts, 0, file.getLen())};
  }
  */
}
