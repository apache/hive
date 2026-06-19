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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.api.repl;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatNotificationEvent;
import org.apache.hive.hcatalog.common.HCatException;

import java.util.Iterator;
import java.util.List;

public class HCatReplicationTaskIterator implements Iterator<ReplicationTask>{

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

  private HCatClient hcatClient;
  private IMetaStoreClient.NotificationFilter filter;
  private int maxEvents;

  private int batchSize;

  private Iterator<HCatNotificationEvent> batchIter = null;
  private List<HCatNotificationEvent> batch = null;
  private long pos;
  private long maxPos;
  private int eventCount;

  public HCatReplicationTaskIterator(
      HCatClient hcatClient, long eventFrom, int maxEvents, String dbName, String tableName) throws HCatException {
    init(hcatClient,eventFrom,maxEvents, new HCatReplicationTaskIteratorNotificationFilter(dbName,tableName));
    // using init(..) instead of this(..) because the new HCatReplicationTaskIteratorNotificationFilter
    // is an operation that needs to run before delegating to the other ctor, and this messes up chaining
    // ctors
  }

  public HCatReplicationTaskIterator(
      HCatClient hcatClient, long eventFrom, int maxEvents,
      IMetaStoreClient.NotificationFilter filter) throws HCatException{
    init(hcatClient,eventFrom,maxEvents,filter);
  }

  private void init(
      HCatClient hcatClient, long eventFrom, int maxEvents,
      IMetaStoreClient.NotificationFilter filter) throws HCatException {
    // Simple implementation for now, this will later expand to do DAG evaluation.
    this.hcatClient = hcatClient;
    this.filter = filter;
    this.pos = eventFrom;
    if (maxEvents < 1){
      // 0 or -1 implies fetch everything
      this.maxEvents = Integer.MAX_VALUE;
    } else {
      this.maxEvents = maxEvents;
    }
    batchSize = Integer.parseInt(
        hcatClient.getConfVal(HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX.varname,"50"));
    this.eventCount = 0;
    this.maxPos = hcatClient.getCurrentNotificationEventId();
  }

  private void fetchNextBatch() throws HCatException {
    batch = hcatClient.getNextNotification(pos, batchSize, filter);
    batchIter = batch.iterator();
    if (batch.isEmpty()){
      pos += batchSize;
      if (pos < maxPos){
        fetchNextBatch();
        // This way, the only way the recursive stack of fetchNextBatch returns is if:
        //    a) We got a nonempty result, and we can consume
        //    b) We reached the end of the queue, and there are no more events.
        // So, when we return from the fetchNextBatch() stack, if we have no more
        // results in batch, we're done.
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (eventCount >= maxEvents){
      // If we've already satisfied the number of events we were supposed to deliver, we end it.
      return false;
    }
    if ((batchIter != null) && (batchIter.hasNext())){
      // If we have a valid batchIter and it has more elements, return them.
      return true;
    }
    // If we're here, we want more events, and either batchIter is null, or batchIter
    // has reached the end of the current batch. Let's fetch the next batch.
    try {
      fetchNextBatch();
    } catch (HCatException e) {
      // Regrettable that we have to wrap the HCatException into a RuntimeException,
      // but throwing the exception is the appropriate result here, and hasNext()
      // signature will only allow RuntimeExceptions. Iterator.hasNext() really
      // should have allowed IOExceptions
      throw new RuntimeException(e);
    }
    // New batch has been fetched. If it's not empty, we have more elements to process.
    return !batch.isEmpty();
  }

  @Override
  public ReplicationTask next() {
    eventCount++;
    HCatNotificationEvent ev = batchIter.next();
    pos = ev.getEventId();
    return ReplicationTask.create(hcatClient,ev);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove() not supported on HCatReplicationTaskIterator");
  }



}



