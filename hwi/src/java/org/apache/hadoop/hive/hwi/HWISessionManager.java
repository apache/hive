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
package org.apache.hadoop.hive.hwi;

import java.util.Collection;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * HiveSessionManager is a Runnable started inside a web application context.
 * It's basic function is to hold a collection of SessionItem(s). It also works
 * as a facade, as jsp clients can not create a Hive Session directly. Hive
 * Sessions are long lived, unlike a traditional Query and Block system clients
 * set up the query to be started with an instance of this class.
 * 
 */
public class HWISessionManager implements Runnable {

  protected static final Log l4j = LogFactory.getLog(HWISessionManager.class
      .getName());

  private boolean goOn;
  private TreeMap<HWIAuth, Set<HWISessionItem>> items;

  protected HWISessionManager() {
    goOn = true;
    items = new TreeMap<HWIAuth, Set<HWISessionItem>>();
  }

  /**
   * This method scans the SessionItem collection. If a SessionItem is in the
   * QUERY_SET state that signals that its thread should be started. If the
   * SessionItem is in the DESTROY state it should be cleaned up and removed
   * from the collection. Currently we are using a sleep. A wait/notify could be
   * implemented. Queries will run for a long time, a one second wait on start
   * will not be noticed.
   * 
   */
  public void run() {
    l4j.debug("Entered run() thread has started");
    while (goOn) {
      l4j.debug("locking items");
      synchronized (items) {

        for (HWIAuth a : items.keySet()) {
          for (HWISessionItem i : items.get(a)) {
            if (i.getStatus() == HWISessionItem.WebSessionItemStatus.DESTROY) {
              items.get(a).remove(i);
            }
            if (i.getStatus() == HWISessionItem.WebSessionItemStatus.KILL_QUERY) {
              l4j.debug("Killing item: " + i.getSessionName());
              i.killIt();
              l4j.debug("Killed item: " + i.getSessionName());
              items.get(a).remove(i);
            }
          }
        }

      } // end sync
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        l4j.error("Could not sleep ", ex);
      }
    } // end while
    l4j.debug("goOn is false. Loop has ended.");
    // Cleanup used here to stop all threads
    synchronized (items) {
      for (HWIAuth a : items.keySet()) {
        for (HWISessionItem i : items.get(a)) {
          try {
            if (i.getStatus() == HWISessionItem.WebSessionItemStatus.QUERY_RUNNING) {
              l4j.debug(i.getSessionName() + "Joining ");
              i.runnable.join(1000);
              l4j.debug(i.getSessionName() + "Joined ");
            }
          } catch (InterruptedException ex) {
            l4j.error(i.getSessionName() + "while joining ", ex);
          }
        }
      }
    }
  } // end run

  protected boolean isGoOn() {
    return goOn;
  }

  protected void setGoOn(boolean goOn) {
    this.goOn = goOn;
  }

  protected TreeMap<HWIAuth, Set<HWISessionItem>> getItems() {
    return items;
  }

  protected void setItems(TreeMap<HWIAuth, Set<HWISessionItem>> items) {
    this.items = items;
  }

  // client methods called from JSP
  /**
   * Rather then return the actual items we return a list copies. This enforces
   * our HWISessionManager by preventing the ability of the client(jsp) to
   * create SessionItems.
   * 
   * @return A set of SessionItems this framework manages
   */
  public Vector<HWISessionItem> findAllSessionItems() {
    Vector<HWISessionItem> otherItems = new Vector<HWISessionItem>();
    for (HWIAuth a : items.keySet()) {
      otherItems.addAll(items.get(a));
    }
    return otherItems;
  }

  /**
   * Here we handle creating the SessionItem, we do this for the JSP client
   * because we need to set parameters the client is not aware of. One such
   * parameter is the command line arguments the server was started with.
   * 
   * @param a
   *          Authenticated user
   * @param sessionName
   *          Represents the session name
   * @return a new SessionItem or null if a session with that name already
   *         exists
   */
  public HWISessionItem createSession(HWIAuth a, String sessionName) {

    l4j.debug("Creating session: " + sessionName);

    HWISessionItem si = null;

    synchronized (items) {
      if (findSessionItemByName(a, sessionName) == null) {
        l4j.debug("Initializing session: " + sessionName + " a for "
            + a.getUser());
        si = new HWISessionItem(a, sessionName);

        if (!items.containsKey(a)) {
          l4j.debug("SessionList is empty " + a.getUser());
          TreeSet<HWISessionItem> list = new TreeSet<HWISessionItem>();
          list.add(si);
          items.put(a, list);
          l4j.debug("Item added " + si.getSessionName() + " for user "
              + a.getUser());
        } else {
          items.get(a).add(si);
          l4j.debug("Item added " + si.getSessionName() + " for user "
              + a.getUser());
        }

      } else {
        l4j.debug("Creating session: " + sessionName + " already exists "
            + a.getUser());
      }
    }
    return si;
  }

  /**
   * Helper method useful when you know the session name you wish to reference.
   * 
   * @param sessionname
   * @return A SessionItem matching the sessionname or null if it does not
   *         exists
   */
  public HWISessionItem findSessionItemByName(HWIAuth auth, String sessionname) {
    Collection<HWISessionItem> sessForUser = items.get(auth);
    if (sessForUser == null) {
      return null;
    }
    for (HWISessionItem si : sessForUser) {
      if (si.getSessionName().equals(sessionname)) {
        return si;
      }
    }
    return null;
  }

  /**
   * Used to list all users that have at least one session
   * 
   * @return keySet of items all users that have any sessions
   */
  public Set<HWIAuth> findAllUsersWithSessions() {
    return items.keySet();
  }

  /**
   * Used to list all the sessions of a user
   * 
   * @param auth
   *          the user being enquired about
   * @return all the sessions of that user
   */
  public Set<HWISessionItem> findAllSessionsForUser(HWIAuth auth) {
    return items.get(auth);
  }

}
