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

package org.apache.hadoop.hive.ql.hooks;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Implementation of a pre execute hook that checks whether
 * a partition is archived or not and also sets that query
 * time for the partition.
 */
public class ArchiverHook implements PreExecute {

  private static final String ARCHIVE_FLAG = "archiveFlag";
  private static final String LAST_QUERY_TIME = "lastQueryTime";

  static final private Log LOG = LogFactory.getLog("hive.ql.hooks.ArchiverHook");

  /**
   * The metastore client.
   */
  private HiveMetaStoreClient ms;

  /**
   * The archiver hook constructor.
   */
  public ArchiverHook() throws Exception {
    ms = new HiveMetaStoreClient(new HiveConf(this.getClass()));
  }

  private Map<String, String> modifyParams(Map<String, String> old_map, String key, String value) {
    Map<String, String> new_map = old_map;
    if (new_map == null)
      new_map = new LinkedHashMap<String, String>();
    new_map.put(key, value);
    return new_map;
  }

  private boolean setLastQueryTime(Table t) throws Exception {
    Map<String, String> old_map = t.getParameters();
    if (old_map != null) {
      String timeStr = old_map.get(LAST_QUERY_TIME);
      if (timeStr != null) {
        long time = Long.parseLong(timeStr);
        long cur_time = System.currentTimeMillis();
        if (cur_time - time < 1*60*60*1000) {
          // lastQueryTime was recently set
          return false;
        }
      }
    }
    t.setParameters(modifyParams(old_map, LAST_QUERY_TIME,
                    Long.toString(System.currentTimeMillis())));
    return true;
  }

  private boolean setArchiveFlag(Table t) {
    Map<String, String> old_map = t.getParameters();
    if (old_map != null) {
      String archF = old_map.get(ARCHIVE_FLAG);
      if (archF != null) {
        if(archF.equals("false")) {
          return false;
        }
      }
    }
    t.setParameters(modifyParams(t.getParameters(), ARCHIVE_FLAG, "false"));
    return true;
  }

  private boolean setLastQueryTime(Partition p) throws Exception {
    Map<String, String> old_map = p.getParameters();
    if (old_map != null) {
      String timeStr = old_map.get(LAST_QUERY_TIME);
      if (timeStr != null) {
        long time = Long.parseLong(timeStr);
        long cur_time = System.currentTimeMillis();
        if (cur_time - time < 1*60*60*1000) {
          // lastQueryTime was recently set
          return false;
        }
      }
    }
    p.setParameters(modifyParams(old_map, LAST_QUERY_TIME,
                    Long.toString(System.currentTimeMillis())));
    return true;
  }

  private boolean setArchiveFlag(Partition p) {
    Map<String, String> old_map = p.getParameters();
    if (old_map != null) {
      String archF = old_map.get(ARCHIVE_FLAG);
      if (archF != null) {
        if(archF.equals("false")) {
          return false;
        }
      }
    }
    p.setParameters(modifyParams(p.getParameters(), ARCHIVE_FLAG, "false"));
    return true;
  }

  public void run(SessionState sess, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs, UserGroupInformation ugi)
    throws Exception {

    //Go over the input paths and check if they are archived or not
    for(ReadEntity re: inputs) {
      boolean isArchived = false;
      if (re.getParameters() != null) {
        String archF = re.getParameters().get(ARCHIVE_FLAG);
        if (archF != null) {
          isArchived = archF.equals("true");
        }
      }

      if (isArchived)
        throw new Exception("Path: " + re.getLocation().toString() + " needs to be unarchived.");

      // Set the last query time
      ReadEntity.Type typ = re.getType();
      switch(typ) {
      case TABLE:
        Table t = re.getTable().getTTable();
        if(setLastQueryTime(t)) {
          LOG.debug("Setting LastQueryTime for table " + re);
          ms.alter_table(MetaStoreUtils.DEFAULT_DATABASE_NAME, t.getTableName(), t);
        }
        break;
      case PARTITION:
        Partition p = re.getPartition().getTPartition();
        if (setLastQueryTime(p)) {
          LOG.debug("Setting LastQueryTime for partition " + re);
          ms.alter_partition(MetaStoreUtils.DEFAULT_DATABASE_NAME, p.getTableName(), p);
        }
        break;
      default:
        throw new Exception("Unknown type for input: " + re.toString());
      }
    }

    // Go over the write paths and set the archived flag to false
    for(WriteEntity we: outputs) {
      WriteEntity.Type typ = we.getType();
      boolean q, a;

      switch(typ) {
      case TABLE:
        Table t = we.getTable().getTTable();
        q = setLastQueryTime(t);
        a = setArchiveFlag(t);
        if(q || a) {
          LOG.debug("Altering dest table for archiver " + we);
          ms.alter_table(MetaStoreUtils.DEFAULT_DATABASE_NAME, t.getTableName(), t);
        }
        break;
      case PARTITION:
        Partition p = we.getPartition().getTPartition();
        q = setLastQueryTime(p);
        a = setArchiveFlag(p);
        if(q || a) {
          if (ms.getPartition(MetaStoreUtils.DEFAULT_DATABASE_NAME,
                              p.getTableName(), p.getValues()) != null) {
            LOG.debug("Altering dest partition for archiver " + we);
            ms.alter_partition(MetaStoreUtils.DEFAULT_DATABASE_NAME, p.getTableName(), p);
          }
        }
        break;
      case DFS_DIR:
      case LOCAL_DIR:
        break;
      default:
        throw new Exception("Unknown type for output: " + we.toString());
      }
    }
  }

}
