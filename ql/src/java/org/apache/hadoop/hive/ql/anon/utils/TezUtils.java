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

package org.apache.hadoop.hive.ql.anon.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.ql.anon.io.Permissions;
import org.apache.hadoop.hive.ql.anon.tez.AnonContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_SKIP_SWAP;

public final class TezUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TezUtils.class);

  public static boolean swapFiles(final Configuration conf, final AnonContext context) throws IOException {
    if (conf.getBoolean(ANON_SKIP_SWAP, false)) {
      return false;
    }
    final Path tmp = context.getTmpPath();
    final Path dst = context.getInputPath();
    final FileSystem fs = dst.getFileSystem(conf);
    if (!fs.exists(tmp)) {
      throw new IOException("ERASE produced no output at " + tmp + "; refusing to complete the "
          + "swap -- the original " + dst + " is unchanged and may still contain PII");
    }

    LOG.info("in size: {} out size: {}",
        fs.getFileStatus(dst).getLen(), fs.getFileStatus(tmp).getLen());

    fs.setPermission(tmp, Permissions.permission);

    if (fs instanceof DistributedFileSystem) {
      ((DistributedFileSystem) fs).rename(tmp, dst, Options.Rename.OVERWRITE);
    } else {
      final Path bak = new Path(dst.getParent(), "." + dst.getName() + ".anon.bak");
      fs.delete(bak, false);
      if (fs.exists(dst) && !fs.rename(dst, bak)) {
        throw new IOException("swapFiles: could not move original " + dst + " aside to " + bak);
      }
      if (!fs.rename(tmp, dst)) {
        if (fs.exists(bak)) {
          fs.rename(bak, dst);
        }
        throw new IOException("swapFiles: could not rename " + tmp + " onto " + dst
            + " (original restored from backup)");
      }
      fs.delete(bak, false);
    }
    return true;
  }

  public static void deleteQuietly(final Configuration conf, final Path path) {
    if (path == null) {
      return;
    }
    try {
      path.getFileSystem(conf).delete(path, false);
    } catch (Exception e) {
      LOG.warn("could not delete tmp output {}: {}", path, e.getMessage());
    }
  }
}
