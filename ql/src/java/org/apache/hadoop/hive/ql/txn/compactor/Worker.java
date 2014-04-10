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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;

/**
 * A class to do compactions.  This will run in a separate thread.  It will spin on the
 * compaction queue and look for new work to do.
 */
public class Worker extends CompactorThread {
  static final private String CLASS_NAME = Worker.class.getName();
  static final private Log LOG = LogFactory.getLog(CLASS_NAME);
  static final private long SLEEP_TIME = 5000;
  static final private int baseThreadNum = 10002;

  private String name;

  /**
   * Get the hostname that this worker is run on.  Made static and public so that other classes
   * can use the same method to know what host their worker threads are running on.
   * @return hostname
   */
  public static String hostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to resolve my host name " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {
    // Make sure nothing escapes this run method and kills the metastore at large,
    // so wrap it in a big catch Throwable statement.
    try {
      do {
        CompactionInfo ci = txnHandler.findNextToCompact(name);

        if (ci == null && !stop.boolVal) {
          try {
            Thread.sleep(SLEEP_TIME);
            continue;
          } catch (InterruptedException e) {
            LOG.warn("Worker thread sleep interrupted " + e.getMessage());
            continue;
          }
        }

        // Find the table we will be working with.
        Table t1 = null;
        try {
          t1 = resolveTable(ci);
        } catch (MetaException e) {
          txnHandler.markCleaned(ci);
          continue;
        }
        // This chicanery is to get around the fact that the table needs to be final in order to
        // go into the doAs below.
        final Table t = t1;

        // Find the partition we will be working with, if there is one.
        Partition p = null;
        try {
          p = resolvePartition(ci);
        } catch (Exception e) {
          txnHandler.markCleaned(ci);
          continue;
        }

        // Find the appropriate storage descriptor
        final StorageDescriptor sd =  resolveStorageDescriptor(t, p);

        // Check that the table or partition isn't sorted, as we don't yet support that.
        if (sd.getSortCols() != null && !sd.getSortCols().isEmpty()) {
          LOG.error("Attempt to compact sorted table, which is not yet supported!");
          txnHandler.markCleaned(ci);
          continue;
        }

        final boolean isMajor = (ci.type == CompactionType.MAJOR);
        final ValidTxnList txns =
            TxnHandler.createValidTxnList(txnHandler.getOpenTxns());
        final StringBuffer jobName = new StringBuffer(name);
        jobName.append("-compactor-");
        jobName.append(ci.getFullPartitionName());

        // Determine who to run as
        String runAs;
        if (ci.runAs == null) {
          runAs = findUserToRunAs(sd.getLocation(), t);
          txnHandler.setRunAs(ci.id, runAs);
        } else {
          runAs = ci.runAs;
        }

        LOG.info("Starting " + ci.type.toString() + " compaction for " +
            ci.getFullPartitionName());

        final CompactorMR mr = new CompactorMR();
        try {
          if (runJobAsSelf(runAs)) {
            mr.run(conf, jobName.toString(), t, sd, txns, isMajor);
          } else {
            UserGroupInformation ugi = UserGroupInformation.createProxyUser(t.getOwner(),
              UserGroupInformation.getLoginUser());
            ugi.doAs(new PrivilegedExceptionAction<Object>() {
              @Override
              public Object run() throws Exception {
                mr.run(conf, jobName.toString(), t, sd, txns, isMajor);
                return null;
              }
            });
          }
          txnHandler.markCompacted(ci);
        } catch (Exception e) {
          LOG.error("Caught exception while trying to compact " + ci.getFullPartitionName() +
              ".  Marking clean to avoid repeated failures, " + StringUtils.stringifyException(e));
          txnHandler.markCleaned(ci);
        }
      } while (!stop.boolVal);
    } catch (Throwable t) {
      LOG.error("Caught an exception in the main loop of compactor worker " + name +
          ", exiting " + StringUtils.stringifyException(t));
    }
  }

  @Override
  public void init(BooleanPointer stop) throws MetaException {
    super.init(stop);

    StringBuffer name = new StringBuffer(hostname());
    name.append("-");
    name.append(getId());
    this.name = name.toString();
    setName(name.toString());
  }

}
