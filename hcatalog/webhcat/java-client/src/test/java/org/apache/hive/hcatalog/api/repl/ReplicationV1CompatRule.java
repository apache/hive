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

import com.google.common.primitives.Ints;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.EventUtils;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatNotificationEvent;
import org.apache.thrift.TException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Utility class to enable testing of Replv1 compatibility testing.
 *
 * If event formats/etc change in the future, testing against this allows tests
 * to determine if they break backward compatibility with Replv1.
 *
 * Use as a junit TestRule on tests that generate events to test if the events
 * generated are compatible with replv1.
 */
public class ReplicationV1CompatRule implements TestRule {

  public @interface SkipReplV1CompatCheck {

  }

  protected static final Logger LOG = LoggerFactory.getLogger(ReplicationV1CompatRule.class);

  private static ThreadLocal<Long> testEventId = null;
  private IMetaStoreClient metaStoreClient = null;
  private HiveConf hconf = null;
  private List<String> testsToSkip = null;

  public ReplicationV1CompatRule(IMetaStoreClient metaStoreClient, HiveConf hconf){
    this(metaStoreClient, hconf, new ArrayList<String>());
  }
  public ReplicationV1CompatRule(IMetaStoreClient metaStoreClient, HiveConf hconf, List<String> testsToSkip){
    this.metaStoreClient = metaStoreClient;
    this.hconf = hconf;
    testEventId = new ThreadLocal<Long>(){
      @Override
      protected Long initialValue(){
        return getCurrentNotificationId();
      }
    };
    this.testsToSkip = testsToSkip;
    LOG.info("Replv1 backward compatibility tester initialized at " + testEventId.get());
  }

  private Long getCurrentNotificationId(){
    CurrentNotificationEventId cid = null;
    try {
      cid = metaStoreClient.getCurrentNotificationEventId();
      Long l = cid.getEventId();
      return (l == null)? 0L : l;
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Helper method to verify that all events generated since last call are compatible with
   * replv1. If this is called multiple times, it does this check for all events incurred
   * since the last time it was called.
   *
   * @param eventsMustExist : Determines whether or not non-presence of events should be
   *   considered an error. You probably don't need this except during test development
   *   for validation. If you're running this for a whole set of tests in one go, not
   *   having any events is probably an error condition.
   */
  public void doBackwardCompatibilityCheck(boolean eventsMustExist) {

    Long testEventIdPrev = testEventId.get();
    Long testEventIdNow = getCurrentNotificationId();

    testEventId.set(testEventIdNow);

    if (eventsMustExist){
      assertTrue("New events must exist between old["
          + testEventIdPrev + "] and [" + testEventIdNow + "]",
          testEventIdNow > testEventIdPrev);
    } else if (testEventIdNow <= testEventIdPrev){
      return; // nothing further to test.
    }
    doBackwardCompatibilityCheck(testEventIdPrev,testEventIdNow);
  }


  public void doBackwardCompatibilityCheck(long testEventIdBefore, long testEventIdAfter){
    // try to instantiate the old replv1 task generation on every event produced.
    long timeBefore = System.currentTimeMillis();

    Map<NotificationEvent,RuntimeException> unhandledTasks = new LinkedHashMap<>();
    Map<NotificationEvent,RuntimeException> incompatibleTasks = new LinkedHashMap<>();
    int eventCount = 0;

    LOG.info( "Checking replv1 backward compatibility for events between : "
        + testEventIdBefore + " -> " + testEventIdAfter);
    IMetaStoreClient.NotificationFilter evFilter =
        new IMetaStoreClient.NotificationFilter() {
          @Override
          public boolean accept(NotificationEvent notificationEvent) {
            return true;
          }
        };
    EventUtils.MSClientNotificationFetcher evFetcher =
        new EventUtils.MSClientNotificationFetcher(metaStoreClient);
    try {
      EventUtils.NotificationEventIterator evIter = new EventUtils.NotificationEventIterator(
          evFetcher, testEventIdBefore,
          Ints.checkedCast(testEventIdAfter - testEventIdBefore) + 1,
          evFilter);
      ReplicationTask.resetFactory(null);
      assertTrue("We should have found some events",evIter.hasNext());
      while (evIter.hasNext()){
        eventCount++;
        NotificationEvent ev = evIter.next();
        // convert to HCatNotificationEvent, and then try to instantiate a ReplicationTask on it.
        try {
          ReplicationTask rtask = ReplicationTask.create(HCatClient.create(hconf), new HCatNotificationEvent(ev));
          if (rtask instanceof ErroredReplicationTask) {
            unhandledTasks.put(ev, ((ErroredReplicationTask) rtask).getCause());
          }
        } catch (RuntimeException re){
          incompatibleTasks.put(ev, re);
        }
      }
    } catch (IOException e) {
      assertNull("Got an exception when we shouldn't have - replv1 backward incompatibility issue:",e);
    }

    if (unhandledTasks.size() > 0){
      LOG.warn("Events found that would not be coverable by replv1 replication: " + unhandledTasks.size());
      for (NotificationEvent ev : unhandledTasks.keySet()){
        RuntimeException re = unhandledTasks.get(ev);
        LOG.warn(
            "ErroredReplicationTask encountered - new event type does not correspond to a replv1 task:"
                + ev.toString(), re);
      }
    }
    if (incompatibleTasks.size() > 0){
      LOG.warn("Events found that caused errors in replv1 replication: " + incompatibleTasks.size());
      for (NotificationEvent ev : incompatibleTasks.keySet()){
        RuntimeException re = incompatibleTasks.get(ev);
        LOG.warn(
            "RuntimeException encountered - new event type caused a replv1 break."
                + ev.toString(), re);
      }
    }
    assertEquals(0,incompatibleTasks.size());

    long timeAfter = System.currentTimeMillis();
    LOG.info("Backward compatibility check timing:" + timeBefore + " -> " + timeAfter
        + ", ev: " + testEventIdBefore + " => " + testEventIdAfter
        +  ", #events processed=" + eventCount);
  }

  @Override
  public Statement apply(Statement statement, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        Long prevNotificationId = getCurrentNotificationId();
        statement.evaluate();
        Long currNotificationId = getCurrentNotificationId();
        if(!testsToSkip.contains(description.getMethodName())){
          doBackwardCompatibilityCheck(prevNotificationId,currNotificationId);
        } else {
          LOG.info("Skipping backward compatibility check, as requested, for test :" + description);
        }
      }
    };
  }
}
