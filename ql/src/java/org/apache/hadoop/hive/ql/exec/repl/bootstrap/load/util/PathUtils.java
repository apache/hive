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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import static org.apache.hadoop.hive.ql.Context.EXT_PREFIX;

public class PathUtils {
  private static int pathId = 10000;
  private static Logger LOG = LoggerFactory.getLogger(PathUtils.class);

  public static synchronized Path getExternalTmpPath(Path path, PathInfo pathInfo) {
    URI extURI = path.toUri();
    if (extURI.getScheme().equals("viewfs")) {
      // if we are on viewfs we don't want to use /tmp as tmp dir since rename from /tmp/..
      // to final /user/hive/warehouse/ will fail later, so instead pick tmp dir
      // on same namespace as tbl dir.
      return new Path(pathInfo.computeStagingDir(path.getParent()),
          EXT_PREFIX + Integer.toString(++pathId));
    }
    Path fullyQualifiedPath = new Path(extURI.getScheme(), extURI.getAuthority(), extURI.getPath());
    return new Path(pathInfo.computeStagingDir(fullyQualifiedPath), EXT_PREFIX + Integer.toString(++pathId));
  }
}
