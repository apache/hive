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
package org.apache.hadoop.hive.ql.parse;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

import javax.annotation.Nullable;
import java.text.Collator;
import java.util.HashMap;
import java.util.Map;

/**
 * Statements executed to handle replication have some additional
 * information relevant to the replication subsystem - this class
 * captures those bits of information.
 *
 * Typically, this corresponds to the replicationClause definition
 * in the parser.
 */
public class ReplicationSpec {

  private boolean isInReplicationScope = false; // default is that it's not in a repl scope
  private boolean isMetadataOnly = false; // default is full export/import, not metadata-only
  private String eventId = null;
  private String currStateId = null;
  private boolean isNoop = false;
  private boolean isLazy = false; // lazy mode => we only list files, and expect that the eventual copy will pull data in.
  private boolean isInsert = false; // default is that the import mode is replace-into

  // Key definitions related to replication
  public enum KEY {
    REPL_SCOPE("repl.scope"),
    EVENT_ID("repl.event.id"),
    CURR_STATE_ID("repl.last.id"),
    NOOP("repl.noop"),
    LAZY("repl.lazy"),
    IS_INSERT("repl.is.insert")
    ;
    private final String keyName;

    KEY(String s) {
      this.keyName = s;
    }

    @Override
    public String toString(){
      return keyName;
    }
  }

  public enum SCOPE { NO_REPL, MD_ONLY, REPL };

  static private Collator collator = Collator.getInstance();

  /**
   * Class that extends HashMap with a slightly different put semantic, where
   * put behaves as follows:
   *  a) If the key does not already exist, then retains existing HashMap.put behaviour
   *  b) If the map already contains an entry for the given key, then will replace only
   *     if the new value is "greater" than the old value.
   *
   * The primary goal for this is to track repl updates for dbs and tables, to replace state
   * only if the state is newer.
   */
  public static class ReplStateMap<K,V extends Comparable> extends HashMap<K,V> {
    @Override
    public V put(K k, V v){
      if (!containsKey(k)){
        return super.put(k,v);
      }
      V oldValue = get(k);
      if (v.compareTo(oldValue) > 0){
        return super.put(k,v);
      }
      // we did no replacement, but return the old value anyway. This
      // seems most consistent with HashMap behaviour, becuse the "put"
      // was effectively processed and consumed, although we threw away
      // the enw value.
      return oldValue;
    }
  }

  /**
   * Constructor to construct spec based on either the ASTNode that
   * corresponds to the replication clause itself, or corresponds to
   * the parent node, and will scan through the children to instantiate
   * itself.
   * @param node replicationClause node, or parent of replicationClause node
   */
  public ReplicationSpec(ASTNode node){
    if (node != null){
      if (isApplicable(node)){
        init(node);
        return;
      } else {
        for (int i = 1; i < node.getChildCount(); ++i) {
          ASTNode child = (ASTNode) node.getChild(i);
          if (isApplicable(child)) {
            init(child);
            return;
          }
        }
      }
    }
    // If we reached here, we did not find a replication
    // spec in the node or its immediate children. Defaults
    // are to pretend replication is not happening, and the
    // statement above is running as-is.
  }

  /**
   * Default ctor that is useful for determining default states
   */
  public ReplicationSpec(){
    this((ASTNode)null);
  }

  public ReplicationSpec(boolean isInReplicationScope, boolean isMetadataOnly,
                         String eventReplicationState, String currentReplicationState,
                         boolean isNoop, boolean isLazy, boolean isInsert) {
    this.isInReplicationScope = isInReplicationScope;
    this.isMetadataOnly = isMetadataOnly;
    this.eventId = eventReplicationState;
    this.currStateId = currentReplicationState;
    this.isNoop = isNoop;
    this.isLazy = isLazy;
    this.isInsert = isInsert;
  }

  public ReplicationSpec(Function<String, String> keyFetcher) {
    String scope = keyFetcher.apply(ReplicationSpec.KEY.REPL_SCOPE.toString());
    this.isMetadataOnly = false;
    this.isInReplicationScope = false;
    if (scope != null) {
      if (scope.equalsIgnoreCase("metadata")) {
        this.isMetadataOnly = true;
        this.isInReplicationScope = true;
      } else if (scope.equalsIgnoreCase("all")) {
        this.isInReplicationScope = true;
      }
    }
    this.eventId = keyFetcher.apply(ReplicationSpec.KEY.EVENT_ID.toString());
    this.currStateId = keyFetcher.apply(ReplicationSpec.KEY.CURR_STATE_ID.toString());
    this.isNoop = Boolean.parseBoolean(keyFetcher.apply(ReplicationSpec.KEY.NOOP.toString()));
    this.isLazy = Boolean.parseBoolean(keyFetcher.apply(ReplicationSpec.KEY.LAZY.toString()));
    this.isInsert = Boolean.parseBoolean(keyFetcher.apply(ReplicationSpec.KEY.IS_INSERT.toString()));
  }

  /**
   * Tests if an ASTNode is a Replication Specification
   */
  public static boolean isApplicable(ASTNode node){
    return (node.getToken().getType() == HiveParser.TOK_REPLICATION);
  }

  /**
   * @param currReplState Current object state
   * @param replacementReplState Replacement-candidate state
   * @return whether or not a provided replacement candidate is newer(or equal) to the existing object state or not
   */
  public static boolean allowReplacement(String currReplState, String replacementReplState){
    if ((currReplState == null) || (currReplState.isEmpty())) {
      // if we have no replication state on record for the obj, allow replacement.
      return true;
    }
    if ((replacementReplState == null) || (replacementReplState.isEmpty())) {
      // if we reached this condition, we had replication state on record for the
      // object, but its replacement has no state. Disallow replacement
      return false;
    }

    // First try to extract a long value from the strings, and compare them.
    // If oldReplState is less-than or equal to newReplState, allow.
    long currReplStateLong = Long.parseLong(currReplState.replaceAll("\\D",""));
    long replacementReplStateLong = Long.parseLong(replacementReplState.replaceAll("\\D",""));

    if ((currReplStateLong != 0) || (replacementReplStateLong != 0)){
      return ((currReplStateLong - replacementReplStateLong) <= 0);
    }

    // If the long value of both is 0, though, fall back to lexical comparison.

    // Lexical comparison according to locale will suffice for now, future might add more logic
    return (collator.compare(currReplState.toLowerCase(), replacementReplState.toLowerCase()) <= 0);
  }

 /**
   * Determines if a current replication object(current state of dump) is allowed to
   * replicate-replace-into a given partition
   */
  public boolean allowReplacementInto(Partition ptn){
    return allowReplacement(getLastReplicatedStateFromParameters(ptn.getParameters()),this.getCurrentReplicationState());
  }

  /**
   * Determines if a current replication object(current state of dump) is allowed to
   * replicate-replace-into a given partition
   */
  public boolean allowReplacementInto(org.apache.hadoop.hive.metastore.api.Partition ptn){
    return allowReplacement(getLastReplicatedStateFromParameters(ptn.getParameters()),this.getCurrentReplicationState());
  }

  /**
   * Determines if a current replication event specification is allowed to
   * replicate-replace-into a given partition
   */
  public boolean allowEventReplacementInto(Partition ptn){
    return allowReplacement(getLastReplicatedStateFromParameters(ptn.getParameters()),this.getReplicationState());
  }

  /**
   * Determines if a current replication object(current state of dump) is allowed to
   * replicate-replace-into a given table
   */
  public boolean allowReplacementInto(Table table) {
    return allowReplacement(getLastReplicatedStateFromParameters(table.getParameters()),this.getCurrentReplicationState());
  }

  /**
   * Determines if a current replication event specification is allowed to
   * replicate-replace-into a given table
   */
  public boolean allowEventReplacementInto(Table table) {
    return allowReplacement(getLastReplicatedStateFromParameters(table.getParameters()),this.getReplicationState());
  }

  /**
   * Returns a predicate filter to filter an Iterable<Partition> to return all partitions
   * that the current replication event specification is allowed to replicate-replace-into
   */
  public Predicate<Partition> allowEventReplacementInto() {
    return new Predicate<Partition>() {
      @Override
      public boolean apply(@Nullable Partition partition) {
        if (partition == null){
          return false;
        }
        return (allowEventReplacementInto(partition));
      }
    };
  }

  private static String getLastReplicatedStateFromParameters(Map<String, String> m) {
    if ((m != null) && (m.containsKey(KEY.CURR_STATE_ID.toString()))){
      return m.get(KEY.CURR_STATE_ID.toString());
    }
    return null;
  }

  private void init(ASTNode node){
    // -> ^(TOK_REPLICATION $replId $isMetadataOnly)
    isInReplicationScope = true;
    eventId = PlanUtils.stripQuotes(node.getChild(0).getText());
    if (node.getChildCount() > 1){
      if (node.getChild(1).getText().toLowerCase().equals("metadata")) {
        isMetadataOnly= true;
      }
    }
  }

  /**
   * @return true if this statement is being run for the purposes of replication
   */
  public boolean isInReplicationScope(){
    return isInReplicationScope;
  }

  /**
   * @return true if this statement refers to metadata-only operation.
   */
  public boolean isMetadataOnly(){
    return isMetadataOnly;
  }

  public void setIsMetadataOnly(boolean isMetadataOnly){
    this.isMetadataOnly = isMetadataOnly;
  }

  /**
   * @return true if this statement refers to insert-into operation.
   */
  public boolean isInsert(){ return isInsert; }

  public void setIsInsert(boolean isInsert){
    this.isInsert = isInsert;
  }

  /**
   * @return the replication state of the event that spawned this statement
   */
  public String getReplicationState() {
    return eventId;
  }

  /**
   * @return the current replication state of the wh
   */
  public String getCurrentReplicationState() {
    return currStateId;
  }

  public void setCurrentReplicationState(String currStateId) {
    this.currStateId = currStateId;
  }

  /**
   * @return whether or not the current replication action should be a noop
   */
  public boolean isNoop() {
    return isNoop;
  }

  /**
   * @param isNoop whether or not the current replication action should be a noop
   */
  public void setNoop(boolean isNoop) {
    this.isNoop = isNoop;
  }

  /**
   * @return whether or not the current replication action is set to be lazy
   */
  public boolean isLazy() {
    return isLazy;
  }

  /**
   * @param isLazy whether or not the current replication action should be lazy
   */
  public void setLazy(boolean isLazy){
    this.isLazy = isLazy;
  }


  public String get(KEY key) {
    switch (key){
      case REPL_SCOPE:
        switch (getScope()){
          case MD_ONLY:
            return "metadata";
          case REPL:
            return "all";
          case NO_REPL:
            return "none";
        }
      case EVENT_ID:
        return getReplicationState();
      case CURR_STATE_ID:
        return getCurrentReplicationState();
      case NOOP:
        return String.valueOf(isNoop());
      case LAZY:
        return String.valueOf(isLazy());
      case IS_INSERT:
        return String.valueOf(isInsert());
    }
    return null;
  }

  public SCOPE getScope(){
    if (isInReplicationScope()){
      if (isMetadataOnly()){
        return SCOPE.MD_ONLY;
      } else {
        return SCOPE.REPL;
      }
    } else {
      return SCOPE.NO_REPL;
    }
  }
}
