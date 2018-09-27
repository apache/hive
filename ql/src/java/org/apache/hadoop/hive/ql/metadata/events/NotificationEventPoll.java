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

package org.apache.hadoop.hive.ql.metadata.events;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationEventPoll {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationEventPoll.class);
  private static final AtomicBoolean inited = new AtomicBoolean(false);
  private static NotificationEventPoll instance;

  Configuration conf;
  ScheduledExecutorService executorService;
  List<EventConsumer> eventConsumers = new ArrayList<>();
  ScheduledFuture<?> pollFuture;
  long lastCheckedEventId;

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
    } else {
      LOG.debug("No event consumers configured, notification event polling disabled");
      return;
    }

    EventUtils.MSClientNotificationFetcher evFetcher
        = new EventUtils.MSClientNotificationFetcher(Hive.get());
    lastCheckedEventId = evFetcher.getCurrentNotificationEventId();
    LOG.info("Initializing lastCheckedEventId to {}", lastCheckedEventId);

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

  private void stop() {
    if (pollFuture != null) {
      pollFuture.cancel(true);
      pollFuture = null;
    }
    if (executorService != null) {
      executorService.shutdown();
      executorService = null;
    }
  }

  class Poller implements Runnable {
    @Override
    public void run() {
      LOG.debug("Polling for notification events");

      int eventsProcessed = 0;
      try {
        // Get any new notification events that have been since the last time we checked,
        // And pass them on to the event handlers.
        EventUtils.MSClientNotificationFetcher evFetcher
            = new EventUtils.MSClientNotificationFetcher(Hive.get());
        EventUtils.NotificationEventIterator evIter =
            new EventUtils.NotificationEventIterator(evFetcher, lastCheckedEventId, 0, "*", null);

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
        LOG.error("Error polling for notification events", err);
      }

      LOG.debug("Processed {} notification events", eventsProcessed);
    }
  }
}
