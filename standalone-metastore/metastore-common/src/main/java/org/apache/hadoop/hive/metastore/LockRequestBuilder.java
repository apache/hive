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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.LinkedHashMap;
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

  public LockRequestBuilder setZeroWaitReadEnabled(boolean zeroWaitRead) {
    req.setZeroWaitReadEnabled(zeroWaitRead);
    return this;
  }

  public LockRequestBuilder setExclusiveCTAS(boolean exclusiveCTAS) {
    req.setExclusiveCTAS(exclusiveCTAS);
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

  /**
   * Add a collection with lock components to the lock request
   * @param components to add
   * @return reference to this builder
   */
  public LockRequestBuilder addLockComponents(Collection<LockComponent> components) {
    trie.addAll(components);
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
  // valid key in a LinkedHashMap.  So a database lock will map to (dbname, null,
  // null).
  private static class LockTrie {

    private LockTypeComparator lockTypeComparator = new LockTypeComparator();
    private Map<String, TableTrie> trie;

    LockTrie() {
      trie = new LinkedHashMap<>();
    }

    public void add(LockComponent comp) {
      TableTrie tabs = trie.get(comp.getDbname());
      if (tabs == null) {
        tabs = new TableTrie();
        trie.put(comp.getDbname(), tabs);
      }
      setTable(comp, tabs);
    }

    public void addAll(Collection<LockComponent> components) {
      for(LockComponent component: components) {
        add(component);
      }
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
      } else if (lockTypeComparator.compare(comp.getType(), existing.getType()) > 0
          || comp.getType() == existing.getType() && existing.getOperationType() == DataOperationType.INSERT) {
        // We only need to promote if comp.type is > existing.type or it's an update/delete
        parts.put(comp.getPartitionname(), comp);
      }
    }

    private static class TableTrie extends LinkedHashMap<String, PartTrie> {
    }

    private static class PartTrie extends LinkedHashMap<String, LockComponent> {
    }

  }
}
