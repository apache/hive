/**
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
parser grammar AnonParser;

options
{
output=AST;
ASTLabelType=ASTNode;
backtrack=false;
k=3;
}

tokens {
TOK_AT;  // erase from table
TOK_AT_PL; // identity-value list supplied to erase from table
TOK_CAI; // create implicit index
TOK_DAI; // drop implicit index
TOK_DAP; // drop data erasure policy
TOK_BTREE;
TOK_DIRECTORY;
TOK_TABULAR;
TOK_JSON;
TOK_MSGPACK;
TOK_PROTOBUF;
TOK_AVRO;
TOK_XML;
TOK_EXPLAIN_ATTACH;     // EXPLAIN ATTACH ... pre-flight dry run
TOK_VALIDATE_BINDING;   // VALIDATE ERASURE BINDING ...
TOK_AUDIT_POLICY;       // AUDIT ERASURE POLICY ...
TOK_AUDIT_BINDING;      // AUDIT ERASURE BINDING ...
TOK_AUDIT_EXEC;         // AUDIT ERASURE EXECUTIONS ...
TOK_AUDIT_CONFLICTS;    // AUDIT ERASURE CONFLICTS ...
TOK_AUDIT_COMPLIANCE;   // AUDIT ERASURE COMPLIANCE ...
TOK_POLICY_LIST;        // bracketed list of policy names for EXPLAIN ATTACH
TOK_BINDING_OPTS;       // optional WITH (...) + RESOLUTION wrapper
TOK_RESOLUTION_EXPLICIT;
TOK_RESOLUTION_STRICTEST;
TOK_TIME_RANGE;         // FROM ts UNTIL ts filter
TOK_AS_OF;              // AS OF ts filter
TOK_LEP;                // LOAD ERASURE POLICY (persists DRAFT, no validation)
TOK_VEP;                // VALIDATE ERASURE POLICY
TOK_AEP;                // ACTIVATE ERASURE POLICY
TOK_DEP;                // DEACTIVATE ERASURE POLICY
TOK_INVALIDATE_EP;      // INVALIDATE ERASURE POLICY (VALIDATED -> DRAFT)
TOK_POLICY_GRANT;       // GRANT <priv> ON ERASURE POLICY <name> TO USER <principal>
TOK_POLICY_REVOKE;      // REVOKE <priv> ON ERASURE POLICY <name> FROM USER <principal>
TOK_SHOW_ERASURE_LOCKS;    // SHOW ERASURE LOCKS (global, all tables)
TOK_RELEASE_ERASURE_LOCK;  // RELEASE ERASURE LOCK ON TABLE t [FORCE] [WITH REASON '...']
TOK_FORCE_RELEASE;         // optional FORCE keyword on RELEASE
TOK_RELEASE_REASON;        // optional WITH REASON 'free-text' clause
TOK_EXTRACT_FROM_TABLE;    // §4.8 Article 15 / Article 20 subject-access read
TOK_AUDIT_BY_IDENTITY;     // §4.8 Article 33 subject-keyed inverse audit
TOK_POLICY_GRANT_ALL;      // GRANT <priv> ON ALL ERASURE POLICIES TO USER <principal>
TOK_POLICY_REVOKE_ALL;     // REVOKE <priv> ON ALL ERASURE POLICIES FROM USER <principal>
}

@members {
  @Override
  public Object recoverFromMismatchedSet(IntStream input,
      RecognitionException re, BitSet follow) throws RecognitionException {
    throw re;
  }
  @Override
  public void displayRecognitionError(String[] tokenNames,
      RecognitionException e) {
    gParent.errors.add(new ParseError(gParent, e, tokenNames));
  }
}

@rulecatch {
catch (RecognitionException e) {
  throw e;
}
}

/* anonymization related statements */

anonStatement
@init { gParent.pushMsg("anon statements", state); }
@after { gParent.popMsg(state); }
    : eraseFromTable
    | extractFromTable
    | createIndex
    | dropIndex
    | dropErasurePolicy
    | erasurePolicyLifecycle
    | attachErasurePolicy
    | detachErasurePolicy
    | explainAttachPolicy
    | validateErasureBinding
    | auditErasurePolicy
    | auditErasureBinding
    | auditErasureExecutions
    | auditErasureConflicts
    | auditErasureCompliance
    | auditByIdentity
    | releaseErasureLock
    | policyGrant
    | policyRevoke
    ;

// §4.5 privileges include both bare keywords (KW_ERASE, KW_EXPORT) and
// general identifiers (POLICY_VALIDATE / POLICY_ACTIVATE / POLICY_MANAGE
// lex as Identifier). The privName rule lets all five fire as the
// grant target without conflicting with the rest of the grammar.
privName
    : Identifier
    | KW_ERASE
    | KW_EXPORT
    ;

policyGrant
@init { gParent.pushMsg("policy grant", state); }
@after { gParent.popMsg(state); }
    : KW_GRANT priv=privName KW_ON
      ( KW_ERASURE KW_POLICY pname=Identifier KW_TO KW_USER principal=Identifier
        -> ^(TOK_POLICY_GRANT $priv $pname $principal)
      | KW_ALL KW_ERASURE KW_POLICIES KW_TO KW_USER principal=Identifier
        -> ^(TOK_POLICY_GRANT_ALL $priv $principal)
      )
    ;

policyRevoke
@init { gParent.pushMsg("policy revoke", state); }
@after { gParent.popMsg(state); }
    : KW_REVOKE priv=privName KW_ON
      ( KW_ERASURE KW_POLICY pname=Identifier KW_FROM KW_USER principal=Identifier
        -> ^(TOK_POLICY_REVOKE $priv $pname $principal)
      | KW_ALL KW_ERASURE KW_POLICIES KW_FROM KW_USER principal=Identifier
        -> ^(TOK_POLICY_REVOKE_ALL $priv $principal)
      )
    ;

releaseErasureLock
@init { gParent.pushMsg("release erasure lock", state); }
@after { gParent.popMsg(state); }
    : KW_RELEASE KW_ERASURE KW_LOCK KW_ON KW_TABLE tn=tableName
      (f=KW_FORCE)?
      (KW_WITH KW_REASON r=StringLiteral)?
    -> ^(TOK_RELEASE_ERASURE_LOCK $tn ^(TOK_FORCE_RELEASE $f)? ^(TOK_RELEASE_REASON $r)?)
    ;

eraseFromTable
@init { gParent.pushMsg("erase from table", state); }
@after { gParent.popMsg(state); }
    : KW_ERASE KW_FROM KW_TABLE tn=tableName KW_FOR KW_IDENTITY KW_VALUES LPAREN pl=pidList RPAREN
    -> ^(TOK_AT $tn $pl)
    ;

/* §4.8 Article 15 (right of access) / Article 20 (data portability).
 * Mirrors ERASE FROM TABLE in shape so the same (table, column) binding
 * resolves both surfaces. Read-only: the runtime gates on PRIV_EXPORT,
 * records one EXTRACTED audit row, and runs no data rewrite. KW_COLUMN
 * is required so the bound policy's identity-field name resolves through
 * the same path ERASE uses. */
extractFromTable
@init { gParent.pushMsg("extract from table", state); }
@after { gParent.popMsg(state); }
    : KW_EXTRACT KW_FROM KW_TABLE tn=tableName KW_COLUMN cn=Identifier
      KW_FOR KW_IDENTITY KW_VALUES LPAREN pl=pidList RPAREN
      (KW_TO out=StringLiteral)?
    -> ^(TOK_EXTRACT_FROM_TABLE $tn $cn $pl $out?)
    ;

/* §4.8 Article 33 subject-keyed inverse audit. Returns every
 * MErasureRunAudit row whose identityValues column intersects the supplied
 * set, across every table, ordered by startedTs. Privileged under
 * PRIV_POLICY_VALIDATE against the wildcard policy target (the audit
 * surface is organisation-wide). */
auditByIdentity
@init { gParent.pushMsg("audit by identity", state); }
@after { gParent.popMsg(state); }
    : KW_AUDIT KW_BY KW_IDENTITY KW_VALUES LPAREN pl=pidList RPAREN
    -> ^(TOK_AUDIT_BY_IDENTITY $pl)
    ;

pidList
@init { gParent.pushMsg("pid value list", state); }
@after { gParent.popMsg(state); }
    : pidValue (COMMA pidValue)*
    -> ^(TOK_AT_PL pidValue+)
    ;

pidValue
@init { gParent.pushMsg("pid value", state); }
@after { gParent.popMsg(state); }
    : StringLiteral
    | Number
    ;

dropErasurePolicy
@init { gParent.pushMsg("drop erasure policy", state); }
@after { gParent.popMsg(state); }
    : KW_DROP KW_DATA KW_ERASURE KW_POLICY ifExists? id=Identifier
    -> ^(TOK_DAP $id ifExists?)
    ;

erasurePolicyLifecycle
@init { gParent.pushMsg("erasure policy lifecycle", state); }
@after { gParent.popMsg(state); }
    : KW_LOAD       KW_DATA? KW_ERASURE KW_POLICY id=Identifier KW_FROM src=StringLiteral
      -> ^(TOK_LEP $id $src)
    | KW_VALIDATE   KW_DATA? KW_ERASURE KW_POLICY id=Identifier (KW_VERSION ver=StringLiteral)?
      -> ^(TOK_VEP $id $ver?)
    | KW_ACTIVATE   KW_DATA? KW_ERASURE KW_POLICY id=Identifier
      -> ^(TOK_AEP $id)
    | KW_DEACTIVATE KW_DATA? KW_ERASURE KW_POLICY id=Identifier
      -> ^(TOK_DEP $id)
    | KW_INVALIDATE KW_DATA? KW_ERASURE KW_POLICY id=Identifier (KW_VERSION ver=StringLiteral)?
      -> ^(TOK_INVALIDATE_EP $id $ver?)
    ;

attachErasurePolicy
@init { gParent.pushMsg("attach erasure policy", state); }
@after { gParent.popMsg(state); }
    : KW_ATTACH KW_DATA KW_ERASURE KW_POLICY pl=policyNameList
      KW_ON KW_TABLE tn=tableName KW_COLUMN cn=Identifier
      bo=bindingOpts?
    -> ^(TOK_ATTACH_POLICY $pl $tn $cn $bo?)
    ;

detachErasurePolicy
@init { gParent.pushMsg("detach erasure policy", state); }
@after { gParent.popMsg(state); }
    : KW_DETACH KW_DATA KW_ERASURE KW_POLICY pl=policyNameList?
      KW_ON KW_TABLE tn=tableName KW_COLUMN cn=Identifier
    -> ^(TOK_DETACH_POLICY $tn $cn $pl?)
    ;

createIndex
@init { gParent.pushMsg("create implicit index", state); }
@after { gParent.popMsg(state); }
    : KW_CREATE KW_IDENTITY KW_INDEX ifNotExists? id=Identifier KW_ON KW_TABLE tn=tableName LPAREN cn=Identifier RPAREN
       KW_STORED KW_AS it=indexType
    -> ^(TOK_CAI $id $tn $cn $it ifNotExists?)
    ;

indexType
@init { gParent.pushMsg("index type", state); }
@after { gParent.popMsg(state); }
    : KW_BTREE KW_WITH LPAREN
          KW_PAGE KW_SIZE EQUAL ps=Number COMMA
          KW_BUFFER KW_POOL KW_SIZE EQUAL bps=Number COMMA
          KW_POINTER KW_TYPE pt=pointerType
        RPAREN -> ^(TOK_BTREE $ps $bps $pt)
    | KW_DIRECTORY KW_WITH LPAREN KW_POINTER KW_TYPE pt=pointerType RPAREN -> ^(TOK_DIRECTORY $pt)
    | KW_TABULAR -> ^(TOK_TABULAR )
    ;

pointerType
@init { gParent.pushMsg("pointer type", state); }
@after { gParent.popMsg(state); }
    : KW_INT -> TOK_INT
    | KW_BIGINT -> TOK_BIGINT
    ;

dropIndex
@init { gParent.pushMsg("drop implicit index", state); }
@after { gParent.popMsg(state); }
    : KW_DROP KW_IDENTITY KW_INDEX ifExists? id=Identifier KW_ON tn=tableName
    -> ^(TOK_DAI $id $tn ifExists?)
    ;

/* ----------------------------------------------------------------------------
 * EXPLAIN ATTACH / VALIDATE ERASURE BINDING (parked items now wired in)
 *
 *   EXPLAIN ATTACH DATA ERASURE POLICY pn[, pn ...]
 *       ON TABLE table_name COLUMN col_name
 *       [ WITH ( SCHEMA FIELD (sf), ROW LOCATOR (rl),
 *                COLUMN [INTERNAL]? FORMAT (ft) )
 *         RESOLUTION ( EXPLICIT | STRICTEST ) ]
 *
 *   VALIDATE ERASURE BINDING ON TABLE table_name COLUMN col_name
 * ---------------------------------------------------------------------------- */

explainAttachPolicy
@init { gParent.pushMsg("explain attach policy", state); }
@after { gParent.popMsg(state); }
    : KW_EXPLAIN KW_ATTACH KW_DATA KW_ERASURE KW_POLICY pl=policyNameList
      KW_ON KW_TABLE tn=tableName KW_COLUMN cn=Identifier
      bo=bindingOpts?
    -> ^(TOK_EXPLAIN_ATTACH $pl $tn $cn $bo?)
    ;

validateErasureBinding
@init { gParent.pushMsg("validate erasure binding", state); }
@after { gParent.popMsg(state); }
    : KW_VALIDATE KW_ERASURE KW_BINDING KW_ON KW_TABLE tn=tableName KW_COLUMN cn=Identifier
    -> ^(TOK_VALIDATE_BINDING $tn $cn)
    ;

policyNameList
@init { gParent.pushMsg("policy name list", state); }
@after { gParent.popMsg(state); }
    : Identifier (COMMA Identifier)*
    -> ^(TOK_POLICY_LIST Identifier+)
    ;

bindingOpts
@init { gParent.pushMsg("binding options", state); }
@after { gParent.popMsg(state); }
    : KW_WITH LPAREN
        KW_SCHEMA KW_FIELD  LPAREN sf=Identifier RPAREN COMMA
        KW_ROW    KW_LOCATOR LPAREN rl=Identifier RPAREN COMMA
        KW_COLUMN KW_INTERNAL? KW_FORMAT LPAREN ft=serFormatType RPAREN
      RPAREN
      KW_RESOLUTION LPAREN rm=resolutionMode RPAREN
    -> ^(TOK_BINDING_OPTS $sf $rl $ft $rm)
    ;

resolutionMode
@init { gParent.pushMsg("resolution mode", state); }
@after { gParent.popMsg(state); }
    : KW_EXPLICIT  -> TOK_RESOLUTION_EXPLICIT
    | KW_STRICTEST -> TOK_RESOLUTION_STRICTEST
    ;

/* ----------------------------------------------------------------------------
 * AUDIT command set
 *
 *   AUDIT ERASURE POLICY      pn          [FROM ts] [UNTIL ts]
 *   AUDIT ERASURE BINDING     ON TABLE tn COLUMN cn [FROM ts] [UNTIL ts]
 *   AUDIT ERASURE EXECUTIONS  ON TABLE tn [FROM ts] [UNTIL ts]
 *                                          [BY USER u] [FOR IDENTITY id]
 *   AUDIT ERASURE CONFLICTS                [FROM ts] [UNTIL ts]
 *   AUDIT ERASURE COMPLIANCE               [AS OF ts]
 *
 * Timestamps are accepted as string literals (e.g. '2024-01-01') and are
 * parsed by the downstream analyzer.
 * ---------------------------------------------------------------------------- */

auditErasurePolicy
@init { gParent.pushMsg("audit erasure policy", state); }
@after { gParent.popMsg(state); }
    : KW_AUDIT KW_ERASURE KW_POLICY pn=Identifier tr=timeRange?
    -> ^(TOK_AUDIT_POLICY $pn $tr?)
    ;

auditErasureBinding
@init { gParent.pushMsg("audit erasure binding", state); }
@after { gParent.popMsg(state); }
    : KW_AUDIT KW_ERASURE KW_BINDING KW_ON KW_TABLE tn=tableName KW_COLUMN cn=Identifier tr=timeRange?
    -> ^(TOK_AUDIT_BINDING $tn $cn $tr?)
    ;

auditErasureExecutions
@init { gParent.pushMsg("audit erasure executions", state); }
@after { gParent.popMsg(state); }
    : KW_AUDIT KW_ERASURE KW_EXECUTIONS KW_ON KW_TABLE tn=tableName tr=timeRange? bu=byUserClause? fi=forIdentityClause?
    -> ^(TOK_AUDIT_EXEC $tn $tr? $bu? $fi?)
    ;

auditErasureConflicts
@init { gParent.pushMsg("audit erasure conflicts", state); }
@after { gParent.popMsg(state); }
    : KW_AUDIT KW_ERASURE KW_CONFLICTS tr=timeRange?
    -> ^(TOK_AUDIT_CONFLICTS $tr?)
    ;

auditErasureCompliance
@init { gParent.pushMsg("audit erasure compliance", state); }
@after { gParent.popMsg(state); }
    : KW_AUDIT KW_ERASURE KW_COMPLIANCE ao=auditAsOf?
    -> ^(TOK_AUDIT_COMPLIANCE $ao?)
    ;

timeRange
@init { gParent.pushMsg("time range", state); }
@after { gParent.popMsg(state); }
    : (KW_FROM fr=StringLiteral) (KW_UNTIL ut=StringLiteral)?
    -> ^(TOK_TIME_RANGE $fr $ut?)
    | (KW_UNTIL ut=StringLiteral)
    -> ^(TOK_TIME_RANGE $ut)
    ;

auditAsOf
@init { gParent.pushMsg("as of clause", state); }
@after { gParent.popMsg(state); }
    : KW_AS KW_OF ts=StringLiteral
    -> ^(TOK_AS_OF $ts)
    ;

byUserClause
@init { gParent.pushMsg("by user clause", state); }
@after { gParent.popMsg(state); }
    : KW_BY KW_USER u=Identifier
    -> $u
    ;

forIdentityClause
@init { gParent.pushMsg("for identity clause", state); }
@after { gParent.popMsg(state); }
    : KW_FOR KW_IDENTITY id=pidValue
    -> $id
    ;

