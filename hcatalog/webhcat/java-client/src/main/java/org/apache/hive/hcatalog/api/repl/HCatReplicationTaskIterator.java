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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.api.repl;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatNotificationEvent;
import org.apache.hive.hcatalog.common.HCatException;

import java.util.Iterator;

public class HCatReplicationTaskIterator implements Iterator<ReplicationTask>{
  private Iterator<HCatNotificationEvent> notifIter = null;

  private class HCatReplicationTaskIteratorNotificationFilter implements IMetaStoreClient.NotificationFilter {

    private String dbName;
    private String tableName;
    public HCatReplicationTaskIteratorNotificationFilter(String dbName, String tableName){
      this.dbName = dbName;
      this.tableName = tableName;
    }
    @Override
    public boolean accept(NotificationEvent event) {
      if (event == null){
        return false; // get rid of trivial case first, so that we can safely assume non-null
      }
      if (this.dbName == null){
        return true; // if our dbName is null, we're interested in all wh events
      }
      if (this.dbName.equalsIgnoreCase(event.getDbName())){
        if (
            (this.tableName == null)
                // if our dbName is equal, but tableName is blank, we're interested in this db-level event
                || (this.tableName.equalsIgnoreCase(event.getTableName()))
          // table level event that matches us
            ){
          return true;
        }
      }
      return false;
    }
  }

  public HCatReplicationTaskIterator(
      HCatClient hcatClient, long eventFrom, int maxEvents, String dbName, String tableName) throws HCatException {

    init(hcatClient,eventFrom,maxEvents, new HCatReplicationTaskIteratorNotificationFilter(dbName,tableName));
  }

  public HCatReplicationTaskIterator(
      HCatClient hcatClient, long eventFrom, int maxEvents,
      IMetaStoreClient.NotificationFilter filter) throws HCatException{
    init(hcatClient,eventFrom,maxEvents,filter);
  }
  private void init(HCatClient hcatClient, long eventFrom, int maxEvents, IMetaStoreClient.NotificationFilter filter) throws HCatException {
    // Simple implementation for now, this will later expand to do DAG evaluation.
    this.notifIter = hcatClient.getNextNotification(eventFrom, maxEvents,filter).iterator();
  }

  @Override
  public boolean hasNext() {
    return notifIter.hasNext();
  }

  @Override
  public ReplicationTask next() {
    return ReplicationTask.create(notifIter.next());
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove() not supported on HCatReplicationTaskIterator");
  }



}



