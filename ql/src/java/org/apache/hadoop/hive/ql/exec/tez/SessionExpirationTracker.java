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

package org.apache.hadoop.hive.ql.exec.tez;

import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SessionExpirationTracker {
  private static final Logger LOG = LoggerFactory.getLogger(SessionExpirationTracker.class);
  private static final Random rdm = new Random();

  /** Priority queue sorted by expiration time of live sessions that could be expired. */
  private final PriorityBlockingQueue<TezSessionPoolSession> expirationQueue;
  /** The background restart queue that is populated when expiration is triggered by a foreground
   * thread (i.e. getting or returning a session), to avoid delaying it. */
  private final BlockingQueue<TezSessionPoolSession> restartQueue;
  private final Thread expirationThread;
  private final Thread restartThread;
  private final long sessionLifetimeMs;
  private final long sessionLifetimeJitterMs;
  private final RestartImpl sessionRestartImpl;
  private volatile SessionState initSessionState;

  interface RestartImpl {
    void closeAndReopenExpiredSession(TezSessionPoolSession session) throws Exception;
  }

  public static SessionExpirationTracker create(HiveConf conf, RestartImpl restartImpl) {
    long sessionLifetimeMs = conf.getTimeVar(
        ConfVars.HIVE_SERVER2_TEZ_SESSION_LIFETIME, TimeUnit.MILLISECONDS);
    if (sessionLifetimeMs == 0) return null;
    return new SessionExpirationTracker(sessionLifetimeMs, conf.getTimeVar(
        ConfVars.HIVE_SERVER2_TEZ_SESSION_LIFETIME_JITTER, TimeUnit.MILLISECONDS), restartImpl);
  }

  private SessionExpirationTracker(
      long sessionLifetimeMs, long sessionLifetimeJitterMs, RestartImpl restartImpl) {
    this.sessionRestartImpl = restartImpl;
    this.sessionLifetimeMs = sessionLifetimeMs;
    this.sessionLifetimeJitterMs = sessionLifetimeJitterMs;

    LOG.debug("Session expiration is enabled; session lifetime is {} [0, {})ms", sessionLifetimeMs,
        sessionLifetimeJitterMs);

    expirationQueue = new PriorityBlockingQueue<>(11, new Comparator<TezSessionPoolSession>() {
      @Override
      public int compare(TezSessionPoolSession o1, TezSessionPoolSession o2) {
        assert o1.getExpirationNs() != null && o2.getExpirationNs() != null;
        return o1.getExpirationNs().compareTo(o2.getExpirationNs());
      }
    });
    restartQueue = new LinkedBlockingQueue<>();

    expirationThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          SessionState.setCurrentSessionState(initSessionState);
          runExpirationThread();
        } catch (Exception e) {
          LOG.warn("Exception in TezSessionPool-expiration thread. Thread will shut down", e);
        } finally {
          LOG.info("TezSessionPool-expiration thread exiting");
        }
      }
    }, "TezSessionPool-expiration");
    restartThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          SessionState.setCurrentSessionState(initSessionState);
          runRestartThread();
        } catch (Exception e) {
          LOG.warn("Exception in TezSessionPool-cleanup thread. Thread will shut down", e);
        } finally {
          LOG.info("TezSessionPool-cleanup thread exiting");
        }
      }
    }, "TezSessionPool-cleanup");
  }


  /** Logic for the thread that restarts the sessions expired during foreground operations. */
  private void runRestartThread() {
    try {
      while (true) {
        TezSessionPoolSession next = restartQueue.take();
        LOG.info("Restarting the expired session [" + next + "]");
        try {
          sessionRestartImpl.closeAndReopenExpiredSession(next);
        } catch (InterruptedException ie) {
          throw ie;
        } catch (Exception e) {
          LOG.error("Failed to close or restart a session, ignoring", e);
        }
      }
    } catch (InterruptedException e) {
      LOG.info("Restart thread is exiting due to an interruption");
    }
  }

  /** Logic for the thread that tracks session expiration and restarts sessions in background. */
  private void runExpirationThread() {
    try {
      while (true) {
        TezSessionPoolSession nextToExpire = null;
        while (true) {
          // Restart the sessions until one of them refuses to restart.
          nextToExpire = expirationQueue.take();
          LOG.debug("Seeing if we can expire [{}]", nextToExpire);
          try {
            if (!nextToExpire.tryExpire(false)) break;
          } catch (Exception e) {
            // Reopen happens even when close fails, so there's not much to do here.
            LOG.error("Failed to expire session " + nextToExpire + "; ignoring", e);
            nextToExpire = null;
            break; // Not strictly necessary; do the whole queue check again.
          }
          LOG.info("Tez session [" + nextToExpire + "] has expired");
        }
        if (nextToExpire != null && LOG.isDebugEnabled()) {
          LOG.debug("[" + nextToExpire + "] is not ready to expire; adding it back");
        }

        // See addToExpirationQueue for why we re-check the queue.
        synchronized (expirationQueue) {
          // Add back the non-expired session. No need to notify, we are the only ones waiting.
          if (nextToExpire != null) {
            expirationQueue.add(nextToExpire);
          }
          nextToExpire = expirationQueue.peek();
          if (nextToExpire != null) {
            // Add some margin to the wait to avoid rechecking close to the boundary.
            long timeToWaitMs = (nextToExpire.getExpirationNs() - System.nanoTime()) / 1000000L;
            timeToWaitMs = Math.max(1, timeToWaitMs + 10);
            LOG.debug("Waiting for ~{}ms to expire [{}]", timeToWaitMs, nextToExpire);
            expirationQueue.wait(timeToWaitMs);
          } else if (LOG.isDebugEnabled()) {
            // Don't wait if empty - go to take() above, that will wait for us.
            LOG.debug("Expiration queue is empty");
          }
        }
      }
    } catch (InterruptedException e) {
      LOG.info("Expiration thread is exiting due to an interruption");
    }
  }


  public void start() {
    initSessionState = SessionState.get();
    expirationThread.start();
    restartThread.start();
  }


  public void stop() {
    if (expirationThread != null) {
      expirationThread.interrupt();
    }
    if (restartThread != null) {
      restartThread.interrupt();
    }
  }


  public void addToExpirationQueue(TezSessionPoolSession session) {
    long jitterModMs = (long)(sessionLifetimeJitterMs * rdm.nextFloat());
    session.setExpirationNs(System.nanoTime() + (sessionLifetimeMs + jitterModMs) * 1000000L);
    LOG.debug("Adding a pool session [{}] to expiration queue", this);
    // Expiration queue is synchronized and notified upon when adding elements. Without jitter, we
    // wouldn't need this, and could simple look at the first element and sleep for the wait time.
    // However, when many things are added at once, it may happen that we will see the one that
    // expires later first, and will sleep past the earlier expiration times. When we wake up we
    // may kill many sessions at once. To avoid this, we will add to queue under lock and recheck
    // time before we wait. We don't have to worry about removals; at worst we'd wake up in vain.
    // Example: expirations of 1:03:00, 1:00:00, 1:02:00 are added (in this order due to jitter).
    // If the expiration threads sees that 1:03 first, it will sleep for 1:03, then wake up and
    // kill all 3 sessions at once because they all have expired, removing any effect from jitter.
    // Instead, expiration thread rechecks the first queue item and waits on the queue. If nothing
    // is added to the queue, the item examined is still the earliest to be expired. If someone
    // adds to the queue while it is waiting, it will notify the thread and it would wake up and
    // recheck the queue.
    synchronized (expirationQueue) {
      expirationQueue.add(session);
      expirationQueue.notifyAll();
    }
  }


  public void removeFromExpirationQueue(TezSessionPoolSession session) {
    expirationQueue.remove(session);
  }

  public void closeAndRestartExpiredSessionAsync(TezSessionPoolSession session) {
    restartQueue.add(session);
  }

  public void closeAndRestartExpiredSession(
      TezSessionPoolSession session) throws Exception {
    sessionRestartImpl.closeAndReopenExpiredSession(session);
  }
}