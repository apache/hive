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

package org.apache.hadoop.hive.ql.metadata.events;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationEventPoll {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationEventPoll.class);
  private static final AtomicBoolean inited = new AtomicBoolean(false);
  private static volatile NotificationEventPoll instance;

  Configuration conf;
  // called on the poll thread, as a Hive object is not to be shared between threads
  Supplier<EventUtils.NotificationFetcher> evFetcher;
  ScheduledExecutorService executorService;
  List<EventConsumer> eventConsumers = new ArrayList<>();
  ScheduledFuture<?> pollFuture;
  // written by the poll thread only
  volatile long lastCheckedEventId;
  // a sync not started yet, which later callers can share
  private Future<?> pendingSync;
  private boolean metadataCacheEnabled;

  public static void initialize(Configuration conf) throws Exception {
    if (!inited.getAndSet(true)) {
      try {
        instance = new NotificationEventPoll(conf);
      } catch (Exception err) {
        inited.set(false);
        throw err;
      }
    }
  }

  public static void shutdown() {
    // Should only be called for testing.
    if (inited.get()) {
      instance.stop();
      instance = null;
      inited.set(false);
    }
  }

  private NotificationEventPoll(Configuration conf) throws Exception {
    this(conf, null);
  }

  @VisibleForTesting
  NotificationEventPoll(Configuration conf, Supplier<EventUtils.NotificationFetcher> fetcher) throws Exception {
    this.conf = conf;

    long pollInterval = HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.HIVE_NOTFICATION_EVENT_POLL_INTERVAL, TimeUnit.MILLISECONDS);
    if (pollInterval <= 0) {
      LOG.debug("Non-positive poll interval configured, notification event polling disabled");
      return;
    }

    // Initialize the list of event handlers
    String[] consumerClassNames =
        conf.getStrings(HiveConf.ConfVars.HIVE_NOTFICATION_EVENT_CONSUMERS.varname);
    if (consumerClassNames != null && consumerClassNames.length > 0) {
      for (String consumerClassName : consumerClassNames) {
        Class<?> consumerClass = JavaUtils.loadClass(consumerClassName);
        EventConsumer consumer =
            (EventConsumer) ReflectionUtils.newInstance(consumerClass, conf);
        eventConsumers.add(consumer);
      }
    }
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_METADATA_CACHE_EVENTS_ENABLED)) {
      if (eventConsumers.stream().noneMatch(MetadataCacheEventConsumer.class::isInstance)) {
        eventConsumers.add(ReflectionUtils.newInstance(MetadataCacheEventConsumer.class, conf));
      }
      metadataCacheEnabled = true;
    }
    if (eventConsumers.isEmpty()) {
      LOG.debug("No event consumers configured, notification event polling disabled");
      return;
    }

    HiveConf hiveConf = conf instanceof HiveConf ? (HiveConf) conf : new HiveConf(conf, NotificationEventPoll.class);
    evFetcher = fetcher != null ? fetcher : () -> newFetcher(hiveConf);
    lastCheckedEventId = evFetcher.get().getCurrentNotificationEventId();
    LOG.info("Initializing lastCheckedEventId to {}", lastCheckedEventId);
    if (metadataCacheEnabled) {
      metadataCacheEnabled = TableVersionRegistry.get().enable(conf);
    }

    // Start the scheduled poll task
    ThreadFactory threadFactory =
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("NotificationEventPoll %d")
            .build();
    executorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
    pollFuture = executorService.scheduleAtFixedRate(new Poller(),
        pollInterval, pollInterval, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  void stop() {
    if (metadataCacheEnabled) {
      TableVersionRegistry.get().disable();
    }
    if (pollFuture != null) {
      pollFuture.cancel(true);
      pollFuture = null;
    }
    if (executorService != null) {
      executorService.shutdown();
      executorService = null;
    }
  }

  /**
   * Applies all the events up to the latest one in the metastore, waiting at most the timeout for it.
   * It runs on the poll thread, which has the HS2 metastore client: reading the notification log takes a
   * privilege the session users do not need to have. Calls waiting for one sync not started yet share it.
   * @return whether the events were applied up to the latest one when this was called
   */
  public static boolean syncToLatest(long timeoutMs) {
    NotificationEventPoll poll = instance;
    if (poll == null || poll.executorService == null) {
      return false;
    }
    try {
      poll.requestSync().get(timeoutMs, TimeUnit.MILLISECONDS);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (Exception e) {
      LOG.warn("Could not apply the notification events up to the latest one within {} ms", timeoutMs, e);
      return false;
    }
  }

  @VisibleForTesting
  synchronized Future<?> requestSync() {
    if (pendingSync == null) {
      pendingSync = executorService.submit(() -> {
        synchronized (this) {
          // a caller from now on needs the latest event id read after this point
          pendingSync = null;
        }
        long latest = evFetcher.get().getCurrentNotificationEventId();
        if (latest > lastCheckedEventId) {
          processEvents();
        }
        if (lastCheckedEventId < latest) {
          throw new IllegalStateException("Events applied up to " + lastCheckedEventId + ", latest is " + latest);
        }
        return null;
      });
    }
    return pendingSync;
  }

  /**
   * Passes the events since the last one checked to the consumers. To be run on the poll thread only.
   * @throws Exception if the events could not be read; then the next run tries again
   */
  @VisibleForTesting
  int processEvents() throws Exception {
    int eventsProcessed = 0;
    EventUtils.NotificationFetcher fetcher = evFetcher.get();
    try {
      // Get any new notification events that have been since the last time we checked,
      // And pass them on to the event handlers.
      EventUtils.NotificationEventIterator evIter =
          new EventUtils.NotificationEventIterator(fetcher, lastCheckedEventId, 0, "*", null);

      while (evIter.hasNext()) {
        NotificationEvent event = evIter.next();
        LOG.debug("Event: " + event);
        for (EventConsumer eventConsumer : eventConsumers) {
          try {
            eventConsumer.accept(event);
          } catch (Exception err) {
            LOG.error("Error processing notification event " + event, err);
          }
        }
        eventsProcessed++;
        lastCheckedEventId = event.getEventId();
      }
    } catch (Exception err) {
      if (ExceptionUtils.indexOfType(err, IllegalStateException.class) < 0) {
        throw err;
      }
      // The client found ids missing after lastCheckedEventId: the cleaner removed events not read yet.
      // Going on from there would fail forever, so start again from the latest event.
      long latest = fetcher.getCurrentNotificationEventId();
      LOG.error("Notification events after {} are missing, continuing from {}", lastCheckedEventId, latest, err);
      lastCheckedEventId = latest;
      for (EventConsumer eventConsumer : eventConsumers) {
        eventConsumer.onEventsMissed();
      }
    }
    LOG.debug("Processed {} notification events", eventsProcessed);
    return eventsProcessed;
  }

  private static EventUtils.NotificationFetcher newFetcher(HiveConf conf) {
    try {
      // the configuration of the poll, not a default one: the metastore may be set in it only
      return new EventUtils.MSClientNotificationFetcher(Hive.get(conf));
    } catch (Exception e) {
      throw new IllegalStateException("Could not get the metastore client", e);
    }
  }

  class Poller implements Runnable {
    @Override
    public void run() {
      LOG.debug("Polling for notification events");
      try {
        processEvents();
      } catch (Exception err) {
        LOG.error("Error polling for notification events", err);
      }
    }
  }
}
