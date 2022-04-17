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
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

import java.util.Map;

import static org.apache.hadoop.hive.common.repl.ReplConst.REPL_TARGET_DATABASE_PROPERTY;

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
  private boolean isReplace = true; // default is that the import mode is insert overwrite
  private String validWriteIdList = null; // WriteIds snapshot for replicating ACID/MM tables.
  //TxnIds snapshot
  private String validTxnList = null;
  private Type specType = Type.DEFAULT; // DEFAULT means REPL_LOAD or BOOTSTRAP_DUMP or EXPORT
  private boolean needDupCopyCheck = false;
  //Determine if replication is done using repl or export-import
  private boolean isRepl = false;
  private boolean isMetadataOnlyForExternalTables = false;
  private boolean isForceOverwrite = false;

  public void setInReplicationScope(boolean inReplicationScope) {
    isInReplicationScope = inReplicationScope;
  }

  // Key definitions related to replication.
  public enum KEY {
    REPL_SCOPE("repl.scope"),
    EVENT_ID("repl.event.id"),
    CURR_STATE_ID_SOURCE(ReplConst.REPL_TARGET_TABLE_PROPERTY),
    NOOP("repl.noop"),
    IS_REPLACE("repl.is.replace"),
    VALID_WRITEID_LIST("repl.valid.writeid.list"),
    VALID_TXN_LIST("repl.valid.txnid.list"),
    CURR_STATE_ID_TARGET(REPL_TARGET_DATABASE_PROPERTY),
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
    this(true, false, fromId, toId, false, false);
  }

  public ReplicationSpec(boolean isInReplicationScope, boolean isMetadataOnly,
                         String eventReplicationState, String currentReplicationState,
                         boolean isNoop, boolean isReplace) {
    this.setInReplicationScope(isInReplicationScope);
    this.isMetadataOnly = isMetadataOnly;
    this.eventId = eventReplicationState;
    this.currStateId = currentReplicationState;
    this.isNoop = isNoop;
    this.isReplace = isReplace;
    this.specType = Type.DEFAULT;
  }

  public ReplicationSpec(Function<String, String> keyFetcher) {
    String scope = keyFetcher.apply(ReplicationSpec.KEY.REPL_SCOPE.toString());
    this.setInReplicationScope(false);
    this.isMetadataOnly = false;
    this.specType = Type.DEFAULT;
    if (scope != null) {
      if (scope.equalsIgnoreCase("metadata")) {
        this.isMetadataOnly = true;
        this.setInReplicationScope(true);
      } else if (scope.equalsIgnoreCase("all")) {
        this.setInReplicationScope(true);
      }
    }
    this.eventId = keyFetcher.apply(ReplicationSpec.KEY.EVENT_ID.toString());
    this.currStateId = keyFetcher.apply(ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString());
    this.isNoop = Boolean.parseBoolean(keyFetcher.apply(ReplicationSpec.KEY.NOOP.toString()));
    this.isReplace = Boolean.parseBoolean(keyFetcher.apply(ReplicationSpec.KEY.IS_REPLACE.toString()));
    this.validWriteIdList = keyFetcher.apply(ReplicationSpec.KEY.VALID_WRITEID_LIST.toString());
    this.validTxnList = keyFetcher.apply(KEY.VALID_TXN_LIST.toString());
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

  private void init(ASTNode node){
    // -> ^(TOK_REPLICATION $replId $isMetadataOnly)
    setInReplicationScope(true);
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
    if ((m != null) && (m.containsKey(KEY.CURR_STATE_ID_SOURCE.toString()))){
      return m.get(KEY.CURR_STATE_ID_SOURCE.toString());
    }
    return null;
  }

  public static String getTargetLastReplicatedStateFromParameters(Map<String, String> m) {
    if ((m != null) && (m.containsKey(KEY.CURR_STATE_ID_TARGET.toString()))){
      return m.get(KEY.CURR_STATE_ID_TARGET.toString());
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
   * @return true if this statement refers to metadata-only operation.
   */
  public boolean isMetadataOnlyForExternalTables() {
    return isMetadataOnlyForExternalTables;
  }

  public void setMetadataOnlyForExternalTables(boolean metadataOnlyForExternalTables) {
    isMetadataOnlyForExternalTables = metadataOnlyForExternalTables;
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
   * @return the WriteIds snapshot for the current ACID/MM table being replicated
   */
  public String getValidWriteIdList() {
    return validWriteIdList;
  }

  /**
   * @param validWriteIdList WriteIds snapshot for the current ACID/MM table being replicated
   */
  public void setValidWriteIdList(String validWriteIdList) {
    this.validWriteIdList = validWriteIdList;
  }

  public String getValidTxnList() {
    return validTxnList;
  }

  public void setValidTxnList(String validTxnList) {
    this.validTxnList = validTxnList;
  }


  /**
   * @return whether the current replication dumped object related to ACID/Mm table
   */
  public boolean isTransactionalTableDump() {
    return (validWriteIdList != null);
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
      case CURR_STATE_ID_SOURCE:
        return getCurrentReplicationState();
      case NOOP:
        return String.valueOf(isNoop());
      case IS_REPLACE:
        return String.valueOf(isReplace());
      case VALID_WRITEID_LIST:
        return getValidWriteIdList();
      case VALID_TXN_LIST:
        return getValidTxnList();
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


  public static void copyLastReplId(Map<String, String> srcParameter, Map<String, String> destParameter) {
    String lastReplId = srcParameter.get(ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString());
    if (lastReplId != null) {
      destParameter.put(ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString(), lastReplId);
    }
  }

  public boolean needDupCopyCheck() {
    return needDupCopyCheck;
  }

  public void setNeedDupCopyCheck(boolean isFirstIncPending) {
    // Duplicate file check during copy is required until after first successful incremental load.
    // Check HIVE-21197 for more detail.
    this.needDupCopyCheck = isFirstIncPending;
  }

  public boolean isRepl() {
    return isRepl;
  }

  public void setRepl(boolean repl) {
    isRepl = repl;
  }

  public boolean isForceOverwrite() {
    return isForceOverwrite;
  }

  public void setForceOverwrite(boolean forceOverwrite) {
    isForceOverwrite = forceOverwrite;
  }
}
