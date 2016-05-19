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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockType;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * Builder class to make constructing {@link LockRequest} easier.
 */
public class LockRequestBuilder {

  private LockRequest req;
  private LockTrie trie;
  private boolean userSet;

  /**
   * @deprecated 
   */
  public LockRequestBuilder() {
    this(null);
  }
  public LockRequestBuilder(String agentInfo) {
    req = new LockRequest();
    trie = new LockTrie();
    userSet = false;
    if(agentInfo != null) {
      req.setAgentInfo(agentInfo);
    }
  }

  /**
   * Get the constructed LockRequest.
   * @return lock request
   */
  public LockRequest build() {
    if (!userSet) {
      throw new RuntimeException("Cannot build a lock without giving a user");
    }
    trie.addLocksToRequest(req);
    try {
      req.setHostname(InetAddress.getLocalHost().getHostName());
    } catch (UnknownHostException e) {
      throw new RuntimeException("Unable to determine our local host!");
    }
    return req;
  }

  /**
   * Set the transaction id.
   * @param txnid transaction id
   * @return reference to this builder
   */
  public LockRequestBuilder setTransactionId(long txnid) {
    req.setTxnid(txnid);
    return this;
  }

  public LockRequestBuilder setUser(String user) {
    if (user == null) user = "unknown";
    req.setUser(user);
    userSet = true;
    return this;
  }

  /**
   * Add a lock component to the lock request
   * @param component to add
   * @return reference to this builder
   */
  public LockRequestBuilder addLockComponent(LockComponent component) {
    trie.add(component);
    return this;
  }

  // For reasons that are completely incomprehensible to me the semantic
  // analyzers often ask for multiple locks on the same entity (for example
  // a shared_read and an exlcusive lock).  The db locking system gets confused
  // by this and dead locks on it.  To resolve that, we'll make sure in the
  // request that multiple locks are coalesced and promoted to the higher
  // level of locking.  To do this we put all locks components in trie based
  // on dbname, tablename, partition name and handle the promotion as new
  // requests come in.  This structure depends on the fact that null is a
  // valid key in a HashMap.  So a database lock will map to (dbname, null,
  // null).
  private static class LockTrie {
    Map<String, TableTrie> trie;

    LockTrie() {
      trie = new HashMap<String, TableTrie>();
    }

    public void add(LockComponent comp) {
      TableTrie tabs = trie.get(comp.getDbname());
      if (tabs == null) {
        tabs = new TableTrie();
        trie.put(comp.getDbname(), tabs);
      }
      setTable(comp, tabs);
    }

    public void addLocksToRequest(LockRequest request) {
      for (TableTrie tab : trie.values()) {
        for (PartTrie part : tab.values()) {
          for (LockComponent lock :  part.values()) {
            request.addToComponent(lock);
          }
        }
      }
    }

    private void setTable(LockComponent comp, TableTrie tabs) {
      PartTrie parts = tabs.get(comp.getTablename());
      if (parts == null) {
        parts = new PartTrie();
        tabs.put(comp.getTablename(), parts);
      }
      setPart(comp, parts);
    }

    private void setPart(LockComponent comp, PartTrie parts) {
      LockComponent existing = parts.get(comp.getPartitionname());
      if (existing == null) {
        // No existing lock for this partition.
        parts.put(comp.getPartitionname(), comp);
      }  else if (existing.getType() != LockType.EXCLUSIVE  &&
          (comp.getType() ==  LockType.EXCLUSIVE ||
            comp.getType() ==  LockType.SHARED_WRITE)) {
        // We only need to promote if comp.type is > existing.type.  For
        // efficiency we check if existing is exclusive (in which case we
        // need never promote) or if comp is exclusive or shared_write (in
        // which case we can promote even though they may both be shared
        // write).  If comp is shared_read there's never a need to promote.
        parts.put(comp.getPartitionname(), comp);
      }
    }

    private static class TableTrie extends HashMap<String, PartTrie> {
    }

    private static class PartTrie extends HashMap<String, LockComponent> {
    }



  }
}
