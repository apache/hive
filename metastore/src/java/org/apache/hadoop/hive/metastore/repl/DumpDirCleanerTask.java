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
package org.apache.hadoop.hive.metastore.repl;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class DumpDirCleanerTask extends TimerTask {
  public static final Logger LOG = LoggerFactory.getLogger(DumpDirCleanerTask.class);
  private final HiveConf conf;
  private final Path dumpRoot;
  private final long ttl;

  public DumpDirCleanerTask(HiveConf conf) {
    this.conf = conf;
    dumpRoot = new Path(conf.getVar(HiveConf.ConfVars.REPLDIR));
    ttl = conf.getTimeVar(ConfVars.REPL_DUMPDIR_TTL, TimeUnit.MILLISECONDS);
  }

  @Override
  public void run() {
    LOG.debug("Trying to delete old dump dirs");
    try {
      FileSystem fs = FileSystem.get(dumpRoot.toUri(), conf);
      FileStatus[] statuses = fs.listStatus(dumpRoot);
      for (FileStatus status : statuses)
      {
        if (status.getModificationTime() < System.currentTimeMillis() - ttl)
        {
          fs.delete(status.getPath(), true);
          LOG.info("Deleted old dump dir: " + status.getPath());
        }
      }
    } catch (IOException e) {
      LOG.error("Error while trying to delete dump dir", e);
    }
  }
}
