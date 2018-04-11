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
package org.apache.hadoop.hive.ql.parse;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

import javax.annotation.Nullable;
import java.text.Collator;
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
  private boolean isReplace = true; // default is that the import mode is insert overwrite
  private Type specType = Type.DEFAULT; // DEFAULT means REPL_LOAD or BOOTSTRAP_DUMP or EXPORT

  // Key definitions related to replication
  public enum KEY {
    REPL_SCOPE("repl.scope"),
    EVENT_ID("repl.event.id"),
    CURR_STATE_ID("repl.last.id"),
    NOOP("repl.noop"),
    LAZY("repl.lazy"),
    IS_REPLACE("repl.is.replace")
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

  public enum SCOPE { NO_REPL, MD_ONLY, REPL }

  public enum Type { DEFAULT, INCREMENTAL_DUMP, IMPORT }

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

  public ReplicationSpec(String fromId, String toId) {
    this(true, false, fromId, toId, false, true, false);
  }

  public ReplicationSpec(boolean isInReplicationScope, boolean isMetadataOnly,
                         String eventReplicationState, String currentReplicationState,
                         boolean isNoop, boolean isLazy, boolean isReplace) {
    this.isInReplicationScope = isInReplicationScope;
    this.isMetadataOnly = isMetadataOnly;
    this.eventId = eventReplicationState;
    this.currStateId = currentReplicationState;
    this.isNoop = isNoop;
    this.isLazy = isLazy;
    this.isReplace = isReplace;
    this.specType = Type.DEFAULT;
  }

  public ReplicationSpec(Function<String, String> keyFetcher) {
    String scope = keyFetcher.apply(ReplicationSpec.KEY.REPL_SCOPE.toString());
    this.isInReplicationScope = false;
    this.isMetadataOnly = false;
    this.specType = Type.DEFAULT;
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
    this.isReplace = Boolean.parseBoolean(keyFetcher.apply(ReplicationSpec.KEY.IS_REPLACE.toString()));
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
  public boolean allowReplacement(String currReplState, String replacementReplState){
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
    // If oldReplState is less-than newReplState, allow.
    long currReplStateLong = Long.parseLong(currReplState.replaceAll("\\D",""));
    long replacementReplStateLong = Long.parseLong(replacementReplState.replaceAll("\\D",""));

    // Failure handling of IMPORT command and REPL LOAD commands are different.
    // IMPORT will set the last repl ID before copying data files and hence need to allow
    // replacement if loaded from same dump twice after failing to copy in previous attempt.
    // But, REPL LOAD will set the last repl ID only after the successful copy of data files and
    // hence need not allow if same event is applied twice.
    if (specType == Type.IMPORT) {
      return (currReplStateLong <= replacementReplStateLong);
    } else {
      return (currReplStateLong < replacementReplStateLong);
    }
  }

 /**
   * Determines if a current replication object (current state of dump) is allowed to
   * replicate-replace-into a given metastore object (based on state_id stored in their parameters)
   */
  public boolean allowReplacementInto(Map<String, String> params){
    return allowReplacement(getLastReplicatedStateFromParameters(params),
                            getCurrentReplicationState());
  }

  /**
   * Determines if a current replication event (based on event id) is allowed to
   * replicate-replace-into a given metastore object (based on state_id stored in their parameters)
   */
  public boolean allowEventReplacementInto(Map<String, String> params){
    return allowReplacement(getLastReplicatedStateFromParameters(params), getReplicationState());
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
        return (allowEventReplacementInto(partition.getParameters()));
      }
    };
  }

  private void init(ASTNode node){
    // -> ^(TOK_REPLICATION $replId $isMetadataOnly)
    isInReplicationScope = true;
    eventId = PlanUtils.stripQuotes(node.getChild(0).getText());
    if ((node.getChildCount() > 1)
            && node.getChild(1).getText().toLowerCase().equals("metadata")) {
      isMetadataOnly= true;
      try {
        if (Long.parseLong(eventId) >= 0) {
          // If metadata-only dump, then the state of the dump shouldn't be the latest event id as
          // the data is not yet dumped and shall be dumped in future export.
          currStateId = eventId;
        }
      } catch (Exception ex) {
        // Ignore the exception and fall through the default currentStateId
      }
    }
  }

  public static String getLastReplicatedStateFromParameters(Map<String, String> m) {
    if ((m != null) && (m.containsKey(KEY.CURR_STATE_ID.toString()))){
      return m.get(KEY.CURR_STATE_ID.toString());
    }
    return null;
  }

  /**
   * @return true if this statement refers to incremental dump operation.
   */
  public Type getReplSpecType(){
    return this.specType;
  }

  public void setReplSpecType(Type specType){
    this.specType = specType;
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
   * @return true if this statement refers to insert-into or insert-overwrite operation.
   */
  public boolean isReplace(){ return isReplace; }

  public void setIsReplace(boolean isReplace){
    this.isReplace = isReplace;
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
      case IS_REPLACE:
        return String.valueOf(isReplace());
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
