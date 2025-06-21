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

package org.apache.hadoop.hive.metastore.tools.metatool;

import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.tools.MetaToolObjectStore;
import org.apache.hadoop.util.ExitUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides Hive admins a tool. The following can be done with it:
 * - list the file system root
 * - execute JDOQL against the metastore using DataNucleus
 * - perform HA name node upgrade
 * - summarize the data in HMS
 */
public final class HiveMetaTool {
  private static final Logger LOGGER = LoggerFactory.getLogger(HiveMetaTool.class.getName());

  private HiveMetaTool() {
    throw new UnsupportedOperationException("HiveMetaTool should not be instantiated");
  }

  public static void execute(String[] args) throws Exception {
    HiveMetaToolCommandLine cl = HiveMetaToolCommandLine.parseArguments(args);

    MetaToolObjectStore objectStore = new MetaToolObjectStore();
    objectStore.setConf(MetastoreConf.newMetastoreConf());

    MetaToolTask task = null;
    try {
      if (cl.isListFSRoot()) {
        task = new MetaToolTaskListFSRoot();
      } else if (cl.isExecuteJDOQL()) {
        task = new MetaToolTaskExecuteJDOQLQuery();
      } else if (cl.isUpdateLocation()) {
        task = new MetaToolTaskUpdateLocation();
      } else if (cl.isListExtTblLocs()) {
        task = new MetaToolTaskListExtTblLocs();
      } else if (cl.isDiffExtTblLocs()) {
        task = new MetaToolTaskDiffExtTblLocs();
      } else if (cl.isMetadataSummary()) {
        task = new MetaToolTaskMetadataSummary();
      } else {
        throw new IllegalArgumentException("No task was specified!");
      }

      task.setObjectStore(objectStore);
      task.setCommandLine(cl);
      task.execute();
    } finally {
      objectStore.shutdown();
    }
  }

  public static void main(String[] args) {
    int status = 0;
    try {
      execute(args);
    } catch (Exception e) {
      status = -1;
      LOGGER.error("Exception occured", e);
    } finally {
      ExitUtil.terminate(status);
    }
  }
}
