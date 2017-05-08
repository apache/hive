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
package org.apache.hadoop.hive.metastore.messaging;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class EventUtils {

  /**
   * Utility function that constructs a notification filter to match a given db name and/or table name.
   * If dbName == null, fetches all warehouse events.
   * If dnName != null, but tableName == null, fetches all events for the db
   * If dbName != null && tableName != null, fetches all events for the specified table
   * @param dbName
   * @param tableName
   * @return
   */
  public static IMetaStoreClient.NotificationFilter getDbTblNotificationFilter(final String dbName, final String tableName){
    return new IMetaStoreClient.NotificationFilter() {
      @Override
      public boolean accept(NotificationEvent event) {
        if (event == null){
          return false; // get rid of trivial case first, so that we can safely assume non-null
        }
        if (dbName == null){
          return true; // if our dbName is null, we're interested in all wh events
        }
        if (dbName.equalsIgnoreCase(event.getDbName())){
          if ( (tableName == null)
              // if our dbName is equal, but tableName is blank, we're interested in this db-level event
              || (tableName.equalsIgnoreCase(event.getTableName()))
            // table level event that matches us
              ){
            return true;
          }
        }
        return false;
      }
    };
  }

  public static IMetaStoreClient.NotificationFilter restrictByMessageFormat(final String messageFormat){
    return new IMetaStoreClient.NotificationFilter() {
      @Override
      public boolean accept(NotificationEvent event) {
        if (event == null){
          return false; // get rid of trivial case first, so that we can safely assume non-null
        }
        if (messageFormat == null){
          return true; // let's say that passing null in will not do any filtering.
        }
        if (messageFormat.equalsIgnoreCase(event.getMessageFormat())){
          return true;
        }
        return false;
      }
    };
  }

  public static IMetaStoreClient.NotificationFilter getEventBoundaryFilter(final Long eventFrom, final Long eventTo){
    return new IMetaStoreClient.NotificationFilter() {
      @Override
      public boolean accept(NotificationEvent event) {
        if ( (event == null) || (event.getEventId() < eventFrom) || (event.getEventId() > eventTo)) {
          return false;
        }
        return true;
      }
    };
  }

  public static IMetaStoreClient.NotificationFilter andFilter(
      final IMetaStoreClient.NotificationFilter... filters ) {
    return new IMetaStoreClient.NotificationFilter() {
      @Override
      public boolean accept(NotificationEvent event) {
        for (IMetaStoreClient.NotificationFilter filter : filters){
          if (!filter.accept(event)){
            return false;
          }
        }
        return true;
      }
    };
  }

  public interface NotificationFetcher {
    public int getBatchSize() throws IOException;
    public long getCurrentNotificationEventId() throws IOException;
    public List<NotificationEvent> getNextNotificationEvents(
        long pos, IMetaStoreClient.NotificationFilter filter) throws IOException;
  }

  // MetaStoreClient-based impl of NotificationFetcher
  public static class MSClientNotificationFetcher implements  NotificationFetcher{

    private IMetaStoreClient msc = null;
    private Integer batchSize = null;

    public MSClientNotificationFetcher(IMetaStoreClient msc){
      this.msc = msc;
    }

    @Override
    public int getBatchSize() throws IOException {
      if (batchSize == null){
        try {
          batchSize = Integer.parseInt(
            msc.getConfigValue(HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX.varname, "50"));
          // TODO: we're asking the metastore what its configuration for this var is - we may
          // want to revisit to pull from client side instead. The reason I have it this way
          // is because the metastore is more likely to have a reasonable config for this than
          // an arbitrary client.
        } catch (TException e) {
          throw new IOException(e);
        }
      }
      return batchSize;
    }

    @Override
    public long getCurrentNotificationEventId() throws IOException {
      try {
        return msc.getCurrentNotificationEventId().getEventId();
      } catch (TException e) {
        throw new IOException(e);
      }
    }

    @Override
    public List<NotificationEvent> getNextNotificationEvents(
        long pos, IMetaStoreClient.NotificationFilter filter) throws IOException {
      try {
        return msc.getNextNotification(pos,getBatchSize(), filter).getEvents();
      } catch (TException e) {
        throw new IOException(e);
      }
    }
  }

  public static class NotificationEventIterator implements Iterator<NotificationEvent> {

    private NotificationFetcher nfetcher;
    private IMetaStoreClient.NotificationFilter filter;
    private int maxEvents;

    private Iterator<NotificationEvent> batchIter = null;
    private List<NotificationEvent> batch = null;
    private long pos;
    private long maxPos;
    private int eventCount;

    public NotificationEventIterator(
        NotificationFetcher nfetcher, long eventFrom, int maxEvents,
        String dbName, String tableName) throws IOException {
      init(nfetcher, eventFrom, maxEvents, EventUtils.getDbTblNotificationFilter(dbName, tableName));
      // using init(..) instead of this(..) because the EventUtils.getDbTblNotificationFilter
      // is an operation that needs to run before delegating to the other ctor, and this messes up chaining
      // ctors
    }

    public NotificationEventIterator(
        NotificationFetcher nfetcher, long eventFrom, int maxEvents,
        IMetaStoreClient.NotificationFilter filter) throws IOException {
      init(nfetcher,eventFrom,maxEvents,filter);
    }

    private void init(
        NotificationFetcher nfetcher, long eventFrom, int maxEvents,
        IMetaStoreClient.NotificationFilter filter) throws IOException {
      this.nfetcher = nfetcher;
      this.filter = filter;
      this.pos = eventFrom;
      if (maxEvents < 1){
        // 0 or -1 implies fetch everything
        this.maxEvents = Integer.MAX_VALUE;
      } else {
        this.maxEvents = maxEvents;
      }

      this.eventCount = 0;
      this.maxPos = nfetcher.getCurrentNotificationEventId();
    }

    private void fetchNextBatch() throws IOException {
      batch = nfetcher.getNextNotificationEvents(pos, filter);
      int batchSize = nfetcher.getBatchSize();
      while ( ((batch == null) || (batch.isEmpty())) && (pos < maxPos) ){
        // no valid events this batch, but we're still not done processing events
        pos += batchSize;
        batch = nfetcher.getNextNotificationEvents(pos,filter);
      }

      if (batch == null){
        batch = new ArrayList<NotificationEvent>();
        // instantiate empty list so that we don't error out on iterator fetching.
        // If we're here, then the next check of pos will show our caller that
        // that we've exhausted our event supply
      }
      batchIter = batch.iterator();
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
      } catch (IOException e) {
        // Regrettable that we have to wrap the IOException into a RuntimeException,
        // but throwing the exception is the appropriate result here, and hasNext()
        // signature will only allow RuntimeExceptions. Iterator.hasNext() really
        // should have allowed IOExceptions
        throw new RuntimeException(e);
      }
      // New batch has been fetched. If it's not empty, we have more elements to process.
      return !batch.isEmpty();
    }

    @Override
    public NotificationEvent next() {
      eventCount++;
      NotificationEvent ev = batchIter.next();
      pos = ev.getEventId();
      return ev;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove() not supported on NotificationEventIterator");
    }

  }
}
