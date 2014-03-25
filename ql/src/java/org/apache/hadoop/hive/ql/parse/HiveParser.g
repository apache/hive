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
parser grammar HiveParser;

options
{
tokenVocab=HiveLexer;
output=AST;
ASTLabelType=CommonTree;
backtrack=false;
k=3;
}
import SelectClauseParser, FromClauseParser, IdentifiersParser;

tokens {
TOK_INSERT;
TOK_QUERY;
TOK_SELECT;
TOK_SELECTDI;
TOK_SELEXPR;
TOK_FROM;
TOK_TAB;
TOK_PARTSPEC;
TOK_PARTVAL;
TOK_DIR;
TOK_LOCAL_DIR;
TOK_TABREF;
TOK_SUBQUERY;
TOK_INSERT_INTO;
TOK_DESTINATION;
TOK_ALLCOLREF;
TOK_TABLE_OR_COL;
TOK_FUNCTION;
TOK_FUNCTIONDI;
TOK_FUNCTIONSTAR;
TOK_WHERE;
TOK_OP_EQ;
TOK_OP_NE;
TOK_OP_LE;
TOK_OP_LT;
TOK_OP_GE;
TOK_OP_GT;
TOK_OP_DIV;
TOK_OP_ADD;
TOK_OP_SUB;
TOK_OP_MUL;
TOK_OP_MOD;
TOK_OP_BITAND;
TOK_OP_BITNOT;
TOK_OP_BITOR;
TOK_OP_BITXOR;
TOK_OP_AND;
TOK_OP_OR;
TOK_OP_NOT;
TOK_OP_LIKE;
TOK_TRUE;
TOK_FALSE;
TOK_TRANSFORM;
TOK_SERDE;
TOK_SERDENAME;
TOK_SERDEPROPS;
TOK_EXPLIST;
TOK_ALIASLIST;
TOK_GROUPBY;
TOK_ROLLUP_GROUPBY;
TOK_CUBE_GROUPBY;
TOK_GROUPING_SETS;
TOK_GROUPING_SETS_EXPRESSION;
TOK_HAVING;
TOK_ORDERBY;
TOK_CLUSTERBY;
TOK_DISTRIBUTEBY;
TOK_SORTBY;
TOK_UNION;
TOK_JOIN;
TOK_LEFTOUTERJOIN;
TOK_RIGHTOUTERJOIN;
TOK_FULLOUTERJOIN;
TOK_UNIQUEJOIN;
TOK_CROSSJOIN;
TOK_LOAD;
TOK_EXPORT;
TOK_IMPORT;
TOK_NULL;
TOK_ISNULL;
TOK_ISNOTNULL;
TOK_TINYINT;
TOK_SMALLINT;
TOK_INT;
TOK_BIGINT;
TOK_BOOLEAN;
TOK_FLOAT;
TOK_DOUBLE;
TOK_DATE;
TOK_DATELITERAL;
TOK_DATETIME;
TOK_TIMESTAMP;
TOK_STRING;
TOK_CHAR;
TOK_VARCHAR;
TOK_BINARY;
TOK_DECIMAL;
TOK_LIST;
TOK_STRUCT;
TOK_MAP;
TOK_UNIONTYPE;
TOK_COLTYPELIST;
TOK_CREATEDATABASE;
TOK_CREATETABLE;
TOK_TRUNCATETABLE;
TOK_CREATEINDEX;
TOK_CREATEINDEX_INDEXTBLNAME;
TOK_DEFERRED_REBUILDINDEX;
TOK_DROPINDEX;
TOK_DROPTABLE_PROPERTIES;
TOK_LIKETABLE;
TOK_DESCTABLE;
TOK_DESCFUNCTION;
TOK_ALTERTABLE_PARTITION;
TOK_ALTERTABLE_RENAME;
TOK_ALTERTABLE_ADDCOLS;
TOK_ALTERTABLE_RENAMECOL;
TOK_ALTERTABLE_RENAMEPART;
TOK_ALTERTABLE_REPLACECOLS;
TOK_ALTERTABLE_ADDPARTS;
TOK_ALTERTABLE_DROPPARTS;
TOK_ALTERTABLE_PARTCOLTYPE;
TOK_ALTERTABLE_PROTECTMODE;
TOK_ALTERTABLE_MERGEFILES;
TOK_ALTERTABLE_TOUCH;
TOK_ALTERTABLE_ARCHIVE;
TOK_ALTERTABLE_UNARCHIVE;
TOK_ALTERTABLE_SERDEPROPERTIES;
TOK_ALTERTABLE_SERIALIZER;
TOK_TABLE_PARTITION;
TOK_ALTERTABLE_FILEFORMAT;
TOK_ALTERTABLE_LOCATION;
TOK_ALTERTABLE_PROPERTIES;
TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION;
TOK_ALTERINDEX_REBUILD;
TOK_ALTERINDEX_PROPERTIES;
TOK_MSCK;
TOK_SHOWDATABASES;
TOK_SHOWTABLES;
TOK_SHOWCOLUMNS;
TOK_SHOWFUNCTIONS;
TOK_SHOWPARTITIONS;
TOK_SHOW_CREATETABLE;
TOK_SHOW_TABLESTATUS;
TOK_SHOW_TBLPROPERTIES;
TOK_SHOWLOCKS;
TOK_LOCKTABLE;
TOK_UNLOCKTABLE;
TOK_LOCKDB;
TOK_UNLOCKDB;
TOK_SWITCHDATABASE;
TOK_DROPDATABASE;
TOK_DROPTABLE;
TOK_DATABASECOMMENT;
TOK_TABCOLLIST;
TOK_TABCOL;
TOK_TABLECOMMENT;
TOK_TABLEPARTCOLS;
TOK_TABLEBUCKETS;
TOK_TABLEROWFORMAT;
TOK_TABLEROWFORMATFIELD;
TOK_TABLEROWFORMATCOLLITEMS;
TOK_TABLEROWFORMATMAPKEYS;
TOK_TABLEROWFORMATLINES;
TOK_TABLEROWFORMATNULL;
TOK_TBLORCFILE;
TOK_TBLPARQUETFILE;
TOK_TBLSEQUENCEFILE;
TOK_TBLTEXTFILE;
TOK_TBLRCFILE;
TOK_TABLEFILEFORMAT;
TOK_FILEFORMAT_GENERIC;
TOK_OFFLINE;
TOK_ENABLE;
TOK_DISABLE;
TOK_READONLY;
TOK_NO_DROP;
TOK_STORAGEHANDLER;
TOK_ALTERTABLE_CLUSTER_SORT;
TOK_NOT_CLUSTERED;
TOK_NOT_SORTED;
TOK_TABCOLNAME;
TOK_TABLELOCATION;
TOK_PARTITIONLOCATION;
TOK_TABLEBUCKETSAMPLE;
TOK_TABLESPLITSAMPLE;
TOK_PERCENT;
TOK_LENGTH;
TOK_ROWCOUNT;
TOK_TMP_FILE;
TOK_TABSORTCOLNAMEASC;
TOK_TABSORTCOLNAMEDESC;
TOK_STRINGLITERALSEQUENCE;
TOK_CHARSETLITERAL;
TOK_CREATEFUNCTION;
TOK_DROPFUNCTION;
TOK_CREATEMACRO;
TOK_DROPMACRO;
TOK_TEMPORARY;
TOK_CREATEVIEW;
TOK_DROPVIEW;
TOK_ALTERVIEW_AS;
TOK_ALTERVIEW_PROPERTIES;
TOK_DROPVIEW_PROPERTIES;
TOK_ALTERVIEW_ADDPARTS;
TOK_ALTERVIEW_DROPPARTS;
TOK_ALTERVIEW_RENAME;
TOK_VIEWPARTCOLS;
TOK_EXPLAIN;
TOK_TABLESERIALIZER;
TOK_TABLEPROPERTIES;
TOK_TABLEPROPLIST;
TOK_INDEXPROPERTIES;
TOK_INDEXPROPLIST;
TOK_TABTYPE;
TOK_LIMIT;
TOK_TABLEPROPERTY;
TOK_IFEXISTS;
TOK_IFNOTEXISTS;
TOK_ORREPLACE;
TOK_HINTLIST;
TOK_HINT;
TOK_MAPJOIN;
TOK_STREAMTABLE;
TOK_HOLD_DDLTIME;
TOK_HINTARGLIST;
TOK_USERSCRIPTCOLNAMES;
TOK_USERSCRIPTCOLSCHEMA;
TOK_RECORDREADER;
TOK_RECORDWRITER;
TOK_LEFTSEMIJOIN;
TOK_LATERAL_VIEW;
TOK_LATERAL_VIEW_OUTER;
TOK_TABALIAS;
TOK_ANALYZE;
TOK_CREATEROLE;
TOK_DROPROLE;
TOK_GRANT;
TOK_REVOKE;
TOK_SHOW_GRANT;
TOK_PRIVILEGE_LIST;
TOK_PRIVILEGE;
TOK_PRINCIPAL_NAME;
TOK_USER;
TOK_GROUP;
TOK_ROLE;
TOK_RESOURCE_ALL;
TOK_GRANT_WITH_OPTION;
TOK_GRANT_WITH_ADMIN_OPTION;
TOK_PRIV_ALL;
TOK_PRIV_ALTER_METADATA;
TOK_PRIV_ALTER_DATA;
TOK_PRIV_DELETE;
TOK_PRIV_DROP;
TOK_PRIV_INDEX;
TOK_PRIV_INSERT;
TOK_PRIV_LOCK;
TOK_PRIV_SELECT;
TOK_PRIV_SHOW_DATABASE;
TOK_PRIV_CREATE;
TOK_PRIV_OBJECT;
TOK_PRIV_OBJECT_COL;
TOK_GRANT_ROLE;
TOK_REVOKE_ROLE;
TOK_SHOW_ROLE_GRANT;
TOK_SHOW_ROLES;
TOK_SHOW_SET_ROLE;
TOK_SHOW_ROLE_PRINCIPALS;
TOK_SHOWINDEXES;
TOK_SHOWDBLOCKS;
TOK_INDEXCOMMENT;
TOK_DESCDATABASE;
TOK_DATABASEPROPERTIES;
TOK_DATABASELOCATION;
TOK_DBPROPLIST;
TOK_ALTERDATABASE_PROPERTIES;
TOK_ALTERDATABASE_OWNER;
TOK_TABNAME;
TOK_TABSRC;
TOK_RESTRICT;
TOK_CASCADE;
TOK_TABLESKEWED;
TOK_TABCOLVALUE;
TOK_TABCOLVALUE_PAIR;
TOK_TABCOLVALUES;
TOK_ALTERTABLE_SKEWED;
TOK_ALTERTBLPART_SKEWED_LOCATION;
TOK_SKEWED_LOCATIONS;
TOK_SKEWED_LOCATION_LIST;
TOK_SKEWED_LOCATION_MAP;
TOK_STOREDASDIRS;
TOK_PARTITIONINGSPEC;
TOK_PTBLFUNCTION;
TOK_WINDOWDEF;
TOK_WINDOWSPEC;
TOK_WINDOWVALUES;
TOK_WINDOWRANGE;
TOK_IGNOREPROTECTION;
TOK_EXCHANGEPARTITION;
TOK_SUBQUERY_EXPR;
TOK_SUBQUERY_OP;
TOK_SUBQUERY_OP_NOTIN;
TOK_SUBQUERY_OP_NOTEXISTS;
TOK_DB_TYPE;
TOK_TABLE_TYPE;
TOK_CTE;
TOK_ARCHIVE;
TOK_FILE;
TOK_JAR;
TOK_RESOURCE_URI;
TOK_RESOURCE_LIST;
TOK_COMPACT;
TOK_SHOW_COMPACTIONS;
TOK_SHOW_TRANSACTIONS;
}


// Package headers
@header {
package org.apache.hadoop.hive.ql.parse;

import java.util.Collection;
import java.util.HashMap;
}


@members {
  ArrayList<ParseError> errors = new ArrayList<ParseError>();
  Stack msgs = new Stack<String>();

  private static HashMap<String, String> xlateMap;
  static {
    xlateMap = new HashMap<String, String>();

    // Keywords
    xlateMap.put("KW_TRUE", "TRUE");
    xlateMap.put("KW_FALSE", "FALSE");
    xlateMap.put("KW_ALL", "ALL");
    xlateMap.put("KW_NONE", "NONE");
    xlateMap.put("KW_DEFAULT", "DEFAULT");
    xlateMap.put("KW_AND", "AND");
    xlateMap.put("KW_OR", "OR");
    xlateMap.put("KW_NOT", "NOT");
    xlateMap.put("KW_LIKE", "LIKE");

    xlateMap.put("KW_ASC", "ASC");
    xlateMap.put("KW_DESC", "DESC");
    xlateMap.put("KW_ORDER", "ORDER");
    xlateMap.put("KW_BY", "BY");
    xlateMap.put("KW_GROUP", "GROUP");
    xlateMap.put("KW_WHERE", "WHERE");
    xlateMap.put("KW_FROM", "FROM");
    xlateMap.put("KW_AS", "AS");
    xlateMap.put("KW_SELECT", "SELECT");
    xlateMap.put("KW_DISTINCT", "DISTINCT");
    xlateMap.put("KW_INSERT", "INSERT");
    xlateMap.put("KW_OVERWRITE", "OVERWRITE");
    xlateMap.put("KW_OUTER", "OUTER");
    xlateMap.put("KW_JOIN", "JOIN");
    xlateMap.put("KW_LEFT", "LEFT");
    xlateMap.put("KW_RIGHT", "RIGHT");
    xlateMap.put("KW_FULL", "FULL");
    xlateMap.put("KW_ON", "ON");
    xlateMap.put("KW_PARTITION", "PARTITION");
    xlateMap.put("KW_PARTITIONS", "PARTITIONS");
    xlateMap.put("KW_TABLE", "TABLE");
    xlateMap.put("KW_TABLES", "TABLES");
    xlateMap.put("KW_TBLPROPERTIES", "TBLPROPERTIES");
    xlateMap.put("KW_SHOW", "SHOW");
    xlateMap.put("KW_MSCK", "MSCK");
    xlateMap.put("KW_DIRECTORY", "DIRECTORY");
    xlateMap.put("KW_LOCAL", "LOCAL");
    xlateMap.put("KW_TRANSFORM", "TRANSFORM");
    xlateMap.put("KW_USING", "USING");
    xlateMap.put("KW_CLUSTER", "CLUSTER");
    xlateMap.put("KW_DISTRIBUTE", "DISTRIBUTE");
    xlateMap.put("KW_SORT", "SORT");
    xlateMap.put("KW_UNION", "UNION");
    xlateMap.put("KW_LOAD", "LOAD");
    xlateMap.put("KW_DATA", "DATA");
    xlateMap.put("KW_INPATH", "INPATH");
    xlateMap.put("KW_IS", "IS");
    xlateMap.put("KW_NULL", "NULL");
    xlateMap.put("KW_CREATE", "CREATE");
    xlateMap.put("KW_EXTERNAL", "EXTERNAL");
    xlateMap.put("KW_ALTER", "ALTER");
    xlateMap.put("KW_DESCRIBE", "DESCRIBE");
    xlateMap.put("KW_DROP", "DROP");
    xlateMap.put("KW_RENAME", "RENAME");
    xlateMap.put("KW_TO", "TO");
    xlateMap.put("KW_COMMENT", "COMMENT");
    xlateMap.put("KW_BOOLEAN", "BOOLEAN");
    xlateMap.put("KW_TINYINT", "TINYINT");
    xlateMap.put("KW_SMALLINT", "SMALLINT");
    xlateMap.put("KW_INT", "INT");
    xlateMap.put("KW_BIGINT", "BIGINT");
    xlateMap.put("KW_FLOAT", "FLOAT");
    xlateMap.put("KW_DOUBLE", "DOUBLE");
    xlateMap.put("KW_DATE", "DATE");
    xlateMap.put("KW_DATETIME", "DATETIME");
    xlateMap.put("KW_TIMESTAMP", "TIMESTAMP");
    xlateMap.put("KW_STRING", "STRING");
    xlateMap.put("KW_BINARY", "BINARY");
    xlateMap.put("KW_ARRAY", "ARRAY");
    xlateMap.put("KW_MAP", "MAP");
    xlateMap.put("KW_REDUCE", "REDUCE");
    xlateMap.put("KW_PARTITIONED", "PARTITIONED");
    xlateMap.put("KW_CLUSTERED", "CLUSTERED");
    xlateMap.put("KW_SORTED", "SORTED");
    xlateMap.put("KW_INTO", "INTO");
    xlateMap.put("KW_BUCKETS", "BUCKETS");
    xlateMap.put("KW_ROW", "ROW");
    xlateMap.put("KW_FORMAT", "FORMAT");
    xlateMap.put("KW_DELIMITED", "DELIMITED");
    xlateMap.put("KW_FIELDS", "FIELDS");
    xlateMap.put("KW_TERMINATED", "TERMINATED");
    xlateMap.put("KW_COLLECTION", "COLLECTION");
    xlateMap.put("KW_ITEMS", "ITEMS");
    xlateMap.put("KW_KEYS", "KEYS");
    xlateMap.put("KW_KEY_TYPE", "\$KEY\$");
    xlateMap.put("KW_LINES", "LINES");
    xlateMap.put("KW_STORED", "STORED");
    xlateMap.put("KW_SEQUENCEFILE", "SEQUENCEFILE");
    xlateMap.put("KW_TEXTFILE", "TEXTFILE");
    xlateMap.put("KW_INPUTFORMAT", "INPUTFORMAT");
    xlateMap.put("KW_OUTPUTFORMAT", "OUTPUTFORMAT");
    xlateMap.put("KW_LOCATION", "LOCATION");
    xlateMap.put("KW_TABLESAMPLE", "TABLESAMPLE");
    xlateMap.put("KW_BUCKET", "BUCKET");
    xlateMap.put("KW_OUT", "OUT");
    xlateMap.put("KW_OF", "OF");
    xlateMap.put("KW_CAST", "CAST");
    xlateMap.put("KW_ADD", "ADD");
    xlateMap.put("KW_REPLACE", "REPLACE");
    xlateMap.put("KW_COLUMNS", "COLUMNS");
    xlateMap.put("KW_RLIKE", "RLIKE");
    xlateMap.put("KW_REGEXP", "REGEXP");
    xlateMap.put("KW_TEMPORARY", "TEMPORARY");
    xlateMap.put("KW_FUNCTION", "FUNCTION");
    xlateMap.put("KW_EXPLAIN", "EXPLAIN");
    xlateMap.put("KW_EXTENDED", "EXTENDED");
    xlateMap.put("KW_SERDE", "SERDE");
    xlateMap.put("KW_WITH", "WITH");
    xlateMap.put("KW_SERDEPROPERTIES", "SERDEPROPERTIES");
    xlateMap.put("KW_LIMIT", "LIMIT");
    xlateMap.put("KW_SET", "SET");
    xlateMap.put("KW_PROPERTIES", "TBLPROPERTIES");
    xlateMap.put("KW_VALUE_TYPE", "\$VALUE\$");
    xlateMap.put("KW_ELEM_TYPE", "\$ELEM\$");
    xlateMap.put("KW_DEFINED", "DEFINED");

    // Operators
    xlateMap.put("DOT", ".");
    xlateMap.put("COLON", ":");
    xlateMap.put("COMMA", ",");
    xlateMap.put("SEMICOLON", ");");

    xlateMap.put("LPAREN", "(");
    xlateMap.put("RPAREN", ")");
    xlateMap.put("LSQUARE", "[");
    xlateMap.put("RSQUARE", "]");

    xlateMap.put("EQUAL", "=");
    xlateMap.put("NOTEQUAL", "<>");
    xlateMap.put("EQUAL_NS", "<=>");
    xlateMap.put("LESSTHANOREQUALTO", "<=");
    xlateMap.put("LESSTHAN", "<");
    xlateMap.put("GREATERTHANOREQUALTO", ">=");
    xlateMap.put("GREATERTHAN", ">");

    xlateMap.put("DIVIDE", "/");
    xlateMap.put("PLUS", "+");
    xlateMap.put("MINUS", "-");
    xlateMap.put("STAR", "*");
    xlateMap.put("MOD", "\%");

    xlateMap.put("AMPERSAND", "&");
    xlateMap.put("TILDE", "~");
    xlateMap.put("BITWISEOR", "|");
    xlateMap.put("BITWISEXOR", "^");
    xlateMap.put("CharSetLiteral", "\\'");
  }

  public static Collection<String> getKeywords() {
    return xlateMap.values();
  }

  private static String xlate(String name) {

    String ret = xlateMap.get(name);
    if (ret == null) {
      ret = name;
    }

    return ret;
  }

  @Override
  public Object recoverFromMismatchedSet(IntStream input,
      RecognitionException re, BitSet follow) throws RecognitionException {
    throw re;
  }

  @Override
  public void displayRecognitionError(String[] tokenNames,
      RecognitionException e) {
    errors.add(new ParseError(this, e, tokenNames));
  }

  @Override
  public String getErrorHeader(RecognitionException e) {
    String header = null;
    if (e.charPositionInLine < 0 && input.LT(-1) != null) {
      Token t = input.LT(-1);
      header = "line " + t.getLine() + ":" + t.getCharPositionInLine();
    } else {
      header = super.getErrorHeader(e);
    }

    return header;
  }
  
  @Override
  public String getErrorMessage(RecognitionException e, String[] tokenNames) {
    String msg = null;

    // Translate the token names to something that the user can understand
    String[] xlateNames = new String[tokenNames.length];
    for (int i = 0; i < tokenNames.length; ++i) {
      xlateNames[i] = HiveParser.xlate(tokenNames[i]);
    }

    if (e instanceof NoViableAltException) {
      @SuppressWarnings("unused")
      NoViableAltException nvae = (NoViableAltException) e;
      // for development, can add
      // "decision=<<"+nvae.grammarDecisionDescription+">>"
      // and "(decision="+nvae.decisionNumber+") and
      // "state "+nvae.stateNumber
      msg = "cannot recognize input near"
              + (input.LT(1) != null ? " " + getTokenErrorDisplay(input.LT(1)) : "")
              + (input.LT(2) != null ? " " + getTokenErrorDisplay(input.LT(2)) : "")
              + (input.LT(3) != null ? " " + getTokenErrorDisplay(input.LT(3)) : "");
    } else if (e instanceof MismatchedTokenException) {
      MismatchedTokenException mte = (MismatchedTokenException) e;
      msg = super.getErrorMessage(e, xlateNames) + (input.LT(-1) == null ? "":" near '" + input.LT(-1).getText()) + "'";
    } else if (e instanceof FailedPredicateException) {
      FailedPredicateException fpe = (FailedPredicateException) e;
      msg = "Failed to recognize predicate '" + fpe.token.getText() + "'. Failed rule: '" + fpe.ruleName + "'";
    } else {
      msg = super.getErrorMessage(e, xlateNames);
    }

    if (msgs.size() > 0) {
      msg = msg + " in " + msgs.peek();
    }
    return msg;
  }
  
  public void pushMsg(String msg, RecognizerSharedState state) {
    // ANTLR generated code does not wrap the @init code wit this backtracking check,
    //  even if the matching @after has it. If we have parser rules with that are doing
    // some lookahead with syntactic predicates this can cause the push() and pop() calls
    // to become unbalanced, so make sure both push/pop check the backtracking state.
    if (state.backtracking == 0) {
      msgs.push(msg);
    }
  }

  public void popMsg(RecognizerSharedState state) {
    if (state.backtracking == 0) {
      msgs.pop();
    }
  }

  // counter to generate unique union aliases
  private int aliasCounter;
  
  private String generateUnionAlias() {
    return "_u" + (++aliasCounter);
  }
}

@rulecatch {
catch (RecognitionException e) {
 reportError(e);
  throw e;
}
}

// starting rule
statement
	: explainStatement EOF
	| execStatement EOF
	;

explainStatement
@init { pushMsg("explain statement", state); }
@after { popMsg(state); }
	: KW_EXPLAIN (explainOptions=KW_EXTENDED|explainOptions=KW_FORMATTED|explainOptions=KW_DEPENDENCY|explainOptions=KW_LOGICAL)? execStatement
      -> ^(TOK_EXPLAIN execStatement $explainOptions?)
	;

execStatement
@init { pushMsg("statement", state); }
@after { popMsg(state); }
    : queryStatementExpression[true]
    | loadStatement
    | exportStatement
    | importStatement
    | ddlStatement
    ;

loadStatement
@init { pushMsg("load statement", state); }
@after { popMsg(state); }
    : KW_LOAD KW_DATA (islocal=KW_LOCAL)? KW_INPATH (path=StringLiteral) (isoverwrite=KW_OVERWRITE)? KW_INTO KW_TABLE (tab=tableOrPartition)
    -> ^(TOK_LOAD $path $tab $islocal? $isoverwrite?)
    ;

exportStatement
@init { pushMsg("export statement", state); }
@after { popMsg(state); }
    : KW_EXPORT KW_TABLE (tab=tableOrPartition) KW_TO (path=StringLiteral)
    -> ^(TOK_EXPORT $tab $path)
    ;

importStatement
@init { pushMsg("import statement", state); }
@after { popMsg(state); }
	: KW_IMPORT ((ext=KW_EXTERNAL)? KW_TABLE (tab=tableOrPartition))? KW_FROM (path=StringLiteral) tableLocation?
    -> ^(TOK_IMPORT $path $tab? $ext? tableLocation?)
    ;

ddlStatement
@init { pushMsg("ddl statement", state); }
@after { popMsg(state); }
    : createDatabaseStatement
    | switchDatabaseStatement
    | dropDatabaseStatement
    | createTableStatement
    | dropTableStatement
    | truncateTableStatement
    | alterStatement
    | descStatement
    | showStatement
    | metastoreCheck
    | createViewStatement
    | dropViewStatement
    | createFunctionStatement
    | createMacroStatement
    | createIndexStatement
    | dropIndexStatement
    | dropFunctionStatement
    | dropMacroStatement
    | analyzeStatement
    | lockStatement
    | unlockStatement
    | lockDatabase
    | unlockDatabase
    | createRoleStatement
    | dropRoleStatement
    | grantPrivileges
    | revokePrivileges
    | showGrants
    | showRoleGrants
    | showRolePrincipals
    | showRoles
    | grantRole
    | revokeRole
    | setRole
    | showCurrentRole
    ;

ifExists
@init { pushMsg("if exists clause", state); }
@after { popMsg(state); }
    : KW_IF KW_EXISTS
    -> ^(TOK_IFEXISTS)
    ;

restrictOrCascade
@init { pushMsg("restrict or cascade clause", state); }
@after { popMsg(state); }
    : KW_RESTRICT
    -> ^(TOK_RESTRICT)
    | KW_CASCADE
    -> ^(TOK_CASCADE)
    ;

ifNotExists
@init { pushMsg("if not exists clause", state); }
@after { popMsg(state); }
    : KW_IF KW_NOT KW_EXISTS
    -> ^(TOK_IFNOTEXISTS)
    ;

storedAsDirs
@init { pushMsg("stored as directories", state); }
@after { popMsg(state); }
    : KW_STORED KW_AS KW_DIRECTORIES
    -> ^(TOK_STOREDASDIRS)
    ;

orReplace
@init { pushMsg("or replace clause", state); }
@after { popMsg(state); }
    : KW_OR KW_REPLACE
    -> ^(TOK_ORREPLACE)
    ;

ignoreProtection
@init { pushMsg("ignore protection clause", state); }
@after { popMsg(state); }
        : KW_IGNORE KW_PROTECTION
        -> ^(TOK_IGNOREPROTECTION)
        ;

createDatabaseStatement
@init { pushMsg("create database statement", state); }
@after { popMsg(state); }
    : KW_CREATE (KW_DATABASE|KW_SCHEMA)
        ifNotExists?
        name=identifier
        databaseComment?
        dbLocation?
        (KW_WITH KW_DBPROPERTIES dbprops=dbProperties)?
    -> ^(TOK_CREATEDATABASE $name ifNotExists? dbLocation? databaseComment? $dbprops?)
    ;

dbLocation
@init { pushMsg("database location specification", state); }
@after { popMsg(state); }
    :
      KW_LOCATION locn=StringLiteral -> ^(TOK_DATABASELOCATION $locn)
    ;

dbProperties
@init { pushMsg("dbproperties", state); }
@after { popMsg(state); }
    :
      LPAREN dbPropertiesList RPAREN -> ^(TOK_DATABASEPROPERTIES dbPropertiesList)
    ;

dbPropertiesList
@init { pushMsg("database properties list", state); }
@after { popMsg(state); }
    :
      keyValueProperty (COMMA keyValueProperty)* -> ^(TOK_DBPROPLIST keyValueProperty+)
    ;


switchDatabaseStatement
@init { pushMsg("switch database statement", state); }
@after { popMsg(state); }
    : KW_USE identifier
    -> ^(TOK_SWITCHDATABASE identifier)
    ;

dropDatabaseStatement
@init { pushMsg("drop database statement", state); }
@after { popMsg(state); }
    : KW_DROP (KW_DATABASE|KW_SCHEMA) ifExists? identifier restrictOrCascade?
    -> ^(TOK_DROPDATABASE identifier ifExists? restrictOrCascade?)
    ;

databaseComment
@init { pushMsg("database's comment", state); }
@after { popMsg(state); }
    : KW_COMMENT comment=StringLiteral
    -> ^(TOK_DATABASECOMMENT $comment)
    ;

createTableStatement
@init { pushMsg("create table statement", state); }
@after { popMsg(state); }
    : KW_CREATE (ext=KW_EXTERNAL)? KW_TABLE ifNotExists? name=tableName
      (  like=KW_LIKE likeName=tableName
         tableLocation?
         tablePropertiesPrefixed?
       | (LPAREN columnNameTypeList RPAREN)?
         tableComment?
         tablePartition?
         tableBuckets?
         tableSkewed?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
         (KW_AS selectStatementWithCTE)?
      )
    -> ^(TOK_CREATETABLE $name $ext? ifNotExists?
         ^(TOK_LIKETABLE $likeName?)
         columnNameTypeList?
         tableComment?
         tablePartition?
         tableBuckets?
         tableSkewed?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
         selectStatementWithCTE?
        )
    ;

truncateTableStatement
@init { pushMsg("truncate table statement", state); }
@after { popMsg(state); }
    : KW_TRUNCATE KW_TABLE tablePartitionPrefix (KW_COLUMNS LPAREN columnNameList RPAREN)? -> ^(TOK_TRUNCATETABLE tablePartitionPrefix columnNameList?);

createIndexStatement
@init { pushMsg("create index statement", state);}
@after {popMsg(state);}
    : KW_CREATE KW_INDEX indexName=identifier
      KW_ON KW_TABLE tab=tableName LPAREN indexedCols=columnNameList RPAREN
      KW_AS typeName=StringLiteral
      autoRebuild?
      indexPropertiesPrefixed?
      indexTblName?
      tableRowFormat?
      tableFileFormat?
      tableLocation?
      tablePropertiesPrefixed?
      indexComment?
    ->^(TOK_CREATEINDEX $indexName $typeName $tab $indexedCols
        autoRebuild?
        indexPropertiesPrefixed?
        indexTblName?
        tableRowFormat?
        tableFileFormat?
        tableLocation?
        tablePropertiesPrefixed?
        indexComment?)
    ;

indexComment
@init { pushMsg("comment on an index", state);}
@after {popMsg(state);}
        :
                KW_COMMENT comment=StringLiteral  -> ^(TOK_INDEXCOMMENT $comment)
        ;

autoRebuild
@init { pushMsg("auto rebuild index", state);}
@after {popMsg(state);}
    : KW_WITH KW_DEFERRED KW_REBUILD
    ->^(TOK_DEFERRED_REBUILDINDEX)
    ;

indexTblName
@init { pushMsg("index table name", state);}
@after {popMsg(state);}
    : KW_IN KW_TABLE indexTbl=tableName
    ->^(TOK_CREATEINDEX_INDEXTBLNAME $indexTbl)
    ;

indexPropertiesPrefixed
@init { pushMsg("table properties with prefix", state); }
@after { popMsg(state); }
    :
        KW_IDXPROPERTIES! indexProperties
    ;

indexProperties
@init { pushMsg("index properties", state); }
@after { popMsg(state); }
    :
      LPAREN indexPropertiesList RPAREN -> ^(TOK_INDEXPROPERTIES indexPropertiesList)
    ;

indexPropertiesList
@init { pushMsg("index properties list", state); }
@after { popMsg(state); }
    :
      keyValueProperty (COMMA keyValueProperty)* -> ^(TOK_INDEXPROPLIST keyValueProperty+)
    ;

dropIndexStatement
@init { pushMsg("drop index statement", state);}
@after {popMsg(state);}
    : KW_DROP KW_INDEX ifExists? indexName=identifier KW_ON tab=tableName
    ->^(TOK_DROPINDEX $indexName $tab ifExists?)
    ;

dropTableStatement
@init { pushMsg("drop statement", state); }
@after { popMsg(state); }
    : KW_DROP KW_TABLE ifExists? tableName -> ^(TOK_DROPTABLE tableName ifExists?)
    ;

alterStatement
@init { pushMsg("alter statement", state); }
@after { popMsg(state); }
    : KW_ALTER!
        (
            KW_TABLE! alterTableStatementSuffix
        |
            KW_VIEW! alterViewStatementSuffix
        |
            KW_INDEX! alterIndexStatementSuffix
        |
            KW_DATABASE! alterDatabaseStatementSuffix
        )
    ;

alterTableStatementSuffix
@init { pushMsg("alter table statement", state); }
@after { popMsg(state); }
    : alterStatementSuffixRename
    | alterStatementSuffixAddCol
    | alterStatementSuffixRenameCol
    | alterStatementSuffixDropPartitions
    | alterStatementSuffixAddPartitions
    | alterStatementSuffixTouch
    | alterStatementSuffixArchive
    | alterStatementSuffixUnArchive
    | alterStatementSuffixProperties
    | alterTblPartitionStatement
    | alterStatementSuffixSkewedby
    | alterStatementSuffixExchangePartition
    | alterStatementPartitionKeyType
    ;

alterStatementPartitionKeyType
@init {msgs.push("alter partition key type"); }
@after {msgs.pop();}
	: identifier KW_PARTITION KW_COLUMN LPAREN columnNameType RPAREN
	-> ^(TOK_ALTERTABLE_PARTCOLTYPE identifier columnNameType)
	;

alterViewStatementSuffix
@init { pushMsg("alter view statement", state); }
@after { popMsg(state); }
    : alterViewSuffixProperties
    | alterStatementSuffixRename
        -> ^(TOK_ALTERVIEW_RENAME alterStatementSuffixRename)
    | alterStatementSuffixAddPartitions
        -> ^(TOK_ALTERVIEW_ADDPARTS alterStatementSuffixAddPartitions)
    | alterStatementSuffixDropPartitions
        -> ^(TOK_ALTERVIEW_DROPPARTS alterStatementSuffixDropPartitions)
    | name=tableName KW_AS selectStatementWithCTE
        -> ^(TOK_ALTERVIEW_AS $name selectStatementWithCTE)
    ;

alterIndexStatementSuffix
@init { pushMsg("alter index statement", state); }
@after { popMsg(state); }
    : indexName=identifier
      (KW_ON tableNameId=identifier)
      partitionSpec?
    (
      KW_REBUILD
      ->^(TOK_ALTERINDEX_REBUILD $tableNameId $indexName partitionSpec?)
    |
      KW_SET KW_IDXPROPERTIES
      indexProperties
      ->^(TOK_ALTERINDEX_PROPERTIES $tableNameId $indexName indexProperties)
    )
    ;

alterDatabaseStatementSuffix
@init { pushMsg("alter database statement", state); }
@after { popMsg(state); }
    : alterDatabaseSuffixProperties
    | alterDatabaseSuffixSetOwner
    ;

alterDatabaseSuffixProperties
@init { pushMsg("alter database properties statement", state); }
@after { popMsg(state); }
    : name=identifier KW_SET KW_DBPROPERTIES dbProperties
    -> ^(TOK_ALTERDATABASE_PROPERTIES $name dbProperties)
    ;

alterDatabaseSuffixSetOwner
@init { pushMsg("alter database set owner", state); }
@after { popMsg(state); }
    : dbName=identifier KW_SET KW_OWNER principalName
    -> ^(TOK_ALTERDATABASE_OWNER $dbName principalName)
    ;

alterStatementSuffixRename
@init { pushMsg("rename statement", state); }
@after { popMsg(state); }
    : oldName=identifier KW_RENAME KW_TO newName=identifier
    -> ^(TOK_ALTERTABLE_RENAME $oldName $newName)
    ;

alterStatementSuffixAddCol
@init { pushMsg("add column statement", state); }
@after { popMsg(state); }
    : identifier (add=KW_ADD | replace=KW_REPLACE) KW_COLUMNS LPAREN columnNameTypeList RPAREN
    -> {$add != null}? ^(TOK_ALTERTABLE_ADDCOLS identifier columnNameTypeList)
    ->                 ^(TOK_ALTERTABLE_REPLACECOLS identifier columnNameTypeList)
    ;

alterStatementSuffixRenameCol
@init { pushMsg("rename column name", state); }
@after { popMsg(state); }
    : identifier KW_CHANGE KW_COLUMN? oldName=identifier newName=identifier colType (KW_COMMENT comment=StringLiteral)? alterStatementChangeColPosition?
    ->^(TOK_ALTERTABLE_RENAMECOL identifier $oldName $newName colType $comment? alterStatementChangeColPosition?)
    ;

alterStatementChangeColPosition
    : first=KW_FIRST|KW_AFTER afterCol=identifier
    ->{$first != null}? ^(TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION )
    -> ^(TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION $afterCol)
    ;

alterStatementSuffixAddPartitions
@init { pushMsg("add partition statement", state); }
@after { popMsg(state); }
    : identifier KW_ADD ifNotExists? alterStatementSuffixAddPartitionsElement+
    -> ^(TOK_ALTERTABLE_ADDPARTS identifier ifNotExists? alterStatementSuffixAddPartitionsElement+)
    ;

alterStatementSuffixAddPartitionsElement
    : partitionSpec partitionLocation?
    ;

alterStatementSuffixTouch
@init { pushMsg("touch statement", state); }
@after { popMsg(state); }
    : identifier KW_TOUCH (partitionSpec)*
    -> ^(TOK_ALTERTABLE_TOUCH identifier (partitionSpec)*)
    ;

alterStatementSuffixArchive
@init { pushMsg("archive statement", state); }
@after { popMsg(state); }
    : identifier KW_ARCHIVE (partitionSpec)*
    -> ^(TOK_ALTERTABLE_ARCHIVE identifier (partitionSpec)*)
    ;

alterStatementSuffixUnArchive
@init { pushMsg("unarchive statement", state); }
@after { popMsg(state); }
    : identifier KW_UNARCHIVE (partitionSpec)*
    -> ^(TOK_ALTERTABLE_UNARCHIVE identifier (partitionSpec)*)
    ;

partitionLocation
@init { pushMsg("partition location", state); }
@after { popMsg(state); }
    :
      KW_LOCATION locn=StringLiteral -> ^(TOK_PARTITIONLOCATION $locn)
    ;

alterStatementSuffixDropPartitions
@init { pushMsg("drop partition statement", state); }
@after { popMsg(state); }
    : identifier KW_DROP ifExists? dropPartitionSpec (COMMA dropPartitionSpec)* ignoreProtection?
    -> ^(TOK_ALTERTABLE_DROPPARTS identifier dropPartitionSpec+ ifExists? ignoreProtection?)
    ;

alterStatementSuffixProperties
@init { pushMsg("alter properties statement", state); }
@after { popMsg(state); }
    : name=identifier KW_SET KW_TBLPROPERTIES tableProperties
    -> ^(TOK_ALTERTABLE_PROPERTIES $name tableProperties)
    | name=identifier KW_UNSET KW_TBLPROPERTIES ifExists? tableProperties
    -> ^(TOK_DROPTABLE_PROPERTIES $name tableProperties ifExists?)
    ;

alterViewSuffixProperties
@init { pushMsg("alter view properties statement", state); }
@after { popMsg(state); }
    : name=identifier KW_SET KW_TBLPROPERTIES tableProperties
    -> ^(TOK_ALTERVIEW_PROPERTIES $name tableProperties)
    | name=identifier KW_UNSET KW_TBLPROPERTIES ifExists? tableProperties
    -> ^(TOK_DROPVIEW_PROPERTIES $name tableProperties ifExists?)
    ;

alterStatementSuffixSerdeProperties
@init { pushMsg("alter serdes statement", state); }
@after { popMsg(state); }
    : KW_SET KW_SERDE serdeName=StringLiteral (KW_WITH KW_SERDEPROPERTIES tableProperties)?
    -> ^(TOK_ALTERTABLE_SERIALIZER $serdeName tableProperties?)
    | KW_SET KW_SERDEPROPERTIES tableProperties
    -> ^(TOK_ALTERTABLE_SERDEPROPERTIES tableProperties)
    ;

tablePartitionPrefix
@init {pushMsg("table partition prefix", state);}
@after {popMsg(state);}
  :name=identifier partitionSpec?
  ->^(TOK_TABLE_PARTITION $name partitionSpec?)
  ;

alterTblPartitionStatement
@init {pushMsg("alter table partition statement", state);}
@after {popMsg(state);}
  : tablePartitionPrefix alterTblPartitionStatementSuffix
  -> ^(TOK_ALTERTABLE_PARTITION tablePartitionPrefix alterTblPartitionStatementSuffix)
  ;

alterTblPartitionStatementSuffix
@init {pushMsg("alter table partition statement suffix", state);}
@after {popMsg(state);}
  : alterStatementSuffixFileFormat
  | alterStatementSuffixLocation
  | alterStatementSuffixProtectMode
  | alterStatementSuffixMergeFiles
  | alterStatementSuffixSerdeProperties
  | alterStatementSuffixRenamePart
  | alterStatementSuffixBucketNum
  | alterTblPartitionStatementSuffixSkewedLocation
  | alterStatementSuffixClusterbySortby
  | alterStatementSuffixCompact
  ;

alterStatementSuffixFileFormat
@init {pushMsg("alter fileformat statement", state); }
@after {popMsg(state);}
	: KW_SET KW_FILEFORMAT fileFormat
	-> ^(TOK_ALTERTABLE_FILEFORMAT fileFormat)
	;

alterStatementSuffixClusterbySortby
@init {pushMsg("alter partition cluster by sort by statement", state);}
@after {popMsg(state);}
  : KW_NOT KW_CLUSTERED -> ^(TOK_ALTERTABLE_CLUSTER_SORT TOK_NOT_CLUSTERED)
  | KW_NOT KW_SORTED -> ^(TOK_ALTERTABLE_CLUSTER_SORT TOK_NOT_SORTED)
  | tableBuckets -> ^(TOK_ALTERTABLE_CLUSTER_SORT tableBuckets)
  ;

alterTblPartitionStatementSuffixSkewedLocation
@init {pushMsg("alter partition skewed location", state);}
@after {popMsg(state);}
  : KW_SET KW_SKEWED KW_LOCATION skewedLocations
  -> ^(TOK_ALTERTBLPART_SKEWED_LOCATION skewedLocations)
  ;
  
skewedLocations
@init { pushMsg("skewed locations", state); }
@after { popMsg(state); }
    :
      LPAREN skewedLocationsList RPAREN -> ^(TOK_SKEWED_LOCATIONS skewedLocationsList)
    ;

skewedLocationsList
@init { pushMsg("skewed locations list", state); }
@after { popMsg(state); }
    :
      skewedLocationMap (COMMA skewedLocationMap)* -> ^(TOK_SKEWED_LOCATION_LIST skewedLocationMap+)
    ;

skewedLocationMap
@init { pushMsg("specifying skewed location map", state); }
@after { popMsg(state); }
    :
      key=skewedValueLocationElement EQUAL value=StringLiteral -> ^(TOK_SKEWED_LOCATION_MAP $key $value)
    ;

alterStatementSuffixLocation
@init {pushMsg("alter location", state);}
@after {popMsg(state);}
  : KW_SET KW_LOCATION newLoc=StringLiteral
  -> ^(TOK_ALTERTABLE_LOCATION $newLoc)
  ;

	
alterStatementSuffixSkewedby
@init {pushMsg("alter skewed by statement", state);}
@after{popMsg(state);}
	:name=identifier tableSkewed
	->^(TOK_ALTERTABLE_SKEWED $name tableSkewed)
	|
	name=identifier KW_NOT KW_SKEWED
	->^(TOK_ALTERTABLE_SKEWED $name)
	|
	name=identifier KW_NOT storedAsDirs
	->^(TOK_ALTERTABLE_SKEWED $name storedAsDirs)
	;

alterStatementSuffixExchangePartition
@init {pushMsg("alter exchange partition", state);}
@after{popMsg(state);}
    : name=tableName KW_EXCHANGE partitionSpec KW_WITH KW_TABLE exchangename=tableName
    -> ^(TOK_EXCHANGEPARTITION $name partitionSpec $exchangename)
    ;

alterStatementSuffixProtectMode
@init { pushMsg("alter partition protect mode statement", state); }
@after { popMsg(state); }
    : alterProtectMode
    -> ^(TOK_ALTERTABLE_PROTECTMODE alterProtectMode)
    ;

alterStatementSuffixRenamePart
@init { pushMsg("alter table rename partition statement", state); }
@after { popMsg(state); }
    : KW_RENAME KW_TO partitionSpec
    ->^(TOK_ALTERTABLE_RENAMEPART partitionSpec)
    ;

alterStatementSuffixMergeFiles
@init { pushMsg("", state); }
@after { popMsg(state); }
    : KW_CONCATENATE
    -> ^(TOK_ALTERTABLE_MERGEFILES)
    ;

alterProtectMode
@init { pushMsg("protect mode specification enable", state); }
@after { popMsg(state); }
    : KW_ENABLE alterProtectModeMode  -> ^(TOK_ENABLE alterProtectModeMode)
    | KW_DISABLE alterProtectModeMode  -> ^(TOK_DISABLE alterProtectModeMode)
    ;

alterProtectModeMode
@init { pushMsg("protect mode specification enable", state); }
@after { popMsg(state); }
    : KW_OFFLINE  -> ^(TOK_OFFLINE)
    | KW_NO_DROP KW_CASCADE? -> ^(TOK_NO_DROP KW_CASCADE?)
    | KW_READONLY  -> ^(TOK_READONLY)
    ;

alterStatementSuffixBucketNum
@init { pushMsg("", state); }
@after { popMsg(state); }
    : KW_INTO num=Number KW_BUCKETS
    -> ^(TOK_TABLEBUCKETS $num)
    ;

alterStatementSuffixCompact
@init { msgs.push("compaction request"); }
@after { msgs.pop(); }
    : KW_COMPACT compactType=StringLiteral
    -> ^(TOK_COMPACT $compactType)
    ;


fileFormat
@init { pushMsg("file format specification", state); }
@after { popMsg(state); }
    : KW_SEQUENCEFILE  -> ^(TOK_TBLSEQUENCEFILE)
    | KW_TEXTFILE  -> ^(TOK_TBLTEXTFILE)
    | KW_RCFILE  -> ^(TOK_TBLRCFILE)
    | KW_ORCFILE -> ^(TOK_TBLORCFILE)
    | KW_PARQUETFILE -> ^(TOK_TBLPARQUETFILE)
    | KW_INPUTFORMAT inFmt=StringLiteral KW_OUTPUTFORMAT outFmt=StringLiteral (KW_INPUTDRIVER inDriver=StringLiteral KW_OUTPUTDRIVER outDriver=StringLiteral)?
      -> ^(TOK_TABLEFILEFORMAT $inFmt $outFmt $inDriver? $outDriver?)
    | genericSpec=identifier -> ^(TOK_FILEFORMAT_GENERIC $genericSpec)
    ;

tabTypeExpr
@init { pushMsg("specifying table types", state); }
@after { popMsg(state); }

   : identifier (DOT^ (KW_ELEM_TYPE | KW_KEY_TYPE | KW_VALUE_TYPE | identifier))*
   ;

descTabTypeExpr
@init { pushMsg("specifying describe table types", state); }
@after { popMsg(state); }

   : identifier (DOT^ (KW_ELEM_TYPE | KW_KEY_TYPE | KW_VALUE_TYPE | identifier))* identifier?
   ;

partTypeExpr
@init { pushMsg("specifying table partitions", state); }
@after { popMsg(state); }
    :  tabTypeExpr partitionSpec? -> ^(TOK_TABTYPE tabTypeExpr partitionSpec?)
    ;

descPartTypeExpr
@init { pushMsg("specifying describe table partitions", state); }
@after { popMsg(state); }
    :  descTabTypeExpr partitionSpec? -> ^(TOK_TABTYPE descTabTypeExpr partitionSpec?)
    ;

descStatement
@init { pushMsg("describe statement", state); }
@after { popMsg(state); }
    : (KW_DESCRIBE|KW_DESC) (descOptions=KW_FORMATTED|descOptions=KW_EXTENDED|descOptions=KW_PRETTY)? (parttype=descPartTypeExpr) -> ^(TOK_DESCTABLE $parttype $descOptions?)
    | (KW_DESCRIBE|KW_DESC) KW_FUNCTION KW_EXTENDED? (name=descFuncNames) -> ^(TOK_DESCFUNCTION $name KW_EXTENDED?)
    | (KW_DESCRIBE|KW_DESC) KW_DATABASE KW_EXTENDED? (dbName=identifier) -> ^(TOK_DESCDATABASE $dbName KW_EXTENDED?)
    ;

analyzeStatement
@init { pushMsg("analyze statement", state); }
@after { popMsg(state); }
    : KW_ANALYZE KW_TABLE (parttype=tableOrPartition) KW_COMPUTE KW_STATISTICS ((noscan=KW_NOSCAN) | (partialscan=KW_PARTIALSCAN) | (KW_FOR KW_COLUMNS statsColumnName=columnNameList))? -> ^(TOK_ANALYZE $parttype $noscan? $partialscan? $statsColumnName?)
    ;

showStatement
@init { pushMsg("show statement", state); }
@after { popMsg(state); }
    : KW_SHOW (KW_DATABASES|KW_SCHEMAS) (KW_LIKE showStmtIdentifier)? -> ^(TOK_SHOWDATABASES showStmtIdentifier?)
    | KW_SHOW KW_TABLES ((KW_FROM|KW_IN) db_name=identifier)? (KW_LIKE showStmtIdentifier|showStmtIdentifier)?  -> ^(TOK_SHOWTABLES (TOK_FROM $db_name)? showStmtIdentifier?)
    | KW_SHOW KW_COLUMNS (KW_FROM|KW_IN) tabname=tableName ((KW_FROM|KW_IN) db_name=identifier)? 
    -> ^(TOK_SHOWCOLUMNS $db_name? $tabname)
    | KW_SHOW KW_FUNCTIONS showFunctionIdentifier?  -> ^(TOK_SHOWFUNCTIONS showFunctionIdentifier?)
    | KW_SHOW KW_PARTITIONS tabName=tableName partitionSpec? -> ^(TOK_SHOWPARTITIONS $tabName partitionSpec?) 
    | KW_SHOW KW_CREATE KW_TABLE tabName=tableName -> ^(TOK_SHOW_CREATETABLE $tabName)
    | KW_SHOW KW_TABLE KW_EXTENDED ((KW_FROM|KW_IN) db_name=identifier)? KW_LIKE showStmtIdentifier partitionSpec?
    -> ^(TOK_SHOW_TABLESTATUS showStmtIdentifier $db_name? partitionSpec?)
    | KW_SHOW KW_TBLPROPERTIES tblName=identifier (LPAREN prptyName=StringLiteral RPAREN)? -> ^(TOK_SHOW_TBLPROPERTIES $tblName $prptyName?)
    | KW_SHOW KW_LOCKS (parttype=partTypeExpr)? (isExtended=KW_EXTENDED)? -> ^(TOK_SHOWLOCKS $parttype? $isExtended?)
    | KW_SHOW KW_LOCKS KW_DATABASE (dbName=Identifier) (isExtended=KW_EXTENDED)? -> ^(TOK_SHOWDBLOCKS $dbName $isExtended?)
    | KW_SHOW (showOptions=KW_FORMATTED)? (KW_INDEX|KW_INDEXES) KW_ON showStmtIdentifier ((KW_FROM|KW_IN) db_name=identifier)?
    -> ^(TOK_SHOWINDEXES showStmtIdentifier $showOptions? $db_name?)
    | KW_SHOW KW_COMPACTIONS -> ^(TOK_SHOW_COMPACTIONS)
    | KW_SHOW KW_TRANSACTIONS -> ^(TOK_SHOW_TRANSACTIONS)
    ;

lockStatement
@init { pushMsg("lock statement", state); }
@after { popMsg(state); }
    : KW_LOCK KW_TABLE tableName partitionSpec? lockMode -> ^(TOK_LOCKTABLE tableName lockMode partitionSpec?)
    ;

lockDatabase
@init { pushMsg("lock database statement", state); }
@after { popMsg(state); }
    : KW_LOCK KW_DATABASE (dbName=Identifier) lockMode -> ^(TOK_LOCKDB $dbName lockMode)
    ;

lockMode
@init { pushMsg("lock mode", state); }
@after { popMsg(state); }
    : KW_SHARED | KW_EXCLUSIVE
    ;

unlockStatement
@init { pushMsg("unlock statement", state); }
@after { popMsg(state); }
    : KW_UNLOCK KW_TABLE tableName partitionSpec?  -> ^(TOK_UNLOCKTABLE tableName partitionSpec?)
    ;

unlockDatabase
@init { pushMsg("unlock database statement", state); }
@after { popMsg(state); }
    : KW_UNLOCK KW_DATABASE (dbName=Identifier) -> ^(TOK_UNLOCKDB $dbName)
    ;

createRoleStatement
@init { pushMsg("create role", state); }
@after { popMsg(state); }
    : KW_CREATE KW_ROLE roleName=identifier
    -> ^(TOK_CREATEROLE $roleName)
    ;

dropRoleStatement
@init {pushMsg("drop role", state);}
@after {popMsg(state);}
    : KW_DROP KW_ROLE roleName=identifier
    -> ^(TOK_DROPROLE $roleName)
    ;

grantPrivileges
@init {pushMsg("grant privileges", state);}
@after {popMsg(state);}
    : KW_GRANT privList=privilegeList
      privilegeObject?
      KW_TO principalSpecification
      withGrantOption?
    -> ^(TOK_GRANT $privList principalSpecification privilegeObject? withGrantOption?)
    ;

revokePrivileges
@init {pushMsg("revoke privileges", state);}
@afer {popMsg(state);}
    : KW_REVOKE privilegeList privilegeObject? KW_FROM principalSpecification
    -> ^(TOK_REVOKE privilegeList principalSpecification privilegeObject?)
    ;

grantRole
@init {pushMsg("grant role", state);}
@after {popMsg(state);}
    : KW_GRANT KW_ROLE? identifier (COMMA identifier)* KW_TO principalSpecification withAdminOption?
    -> ^(TOK_GRANT_ROLE principalSpecification withAdminOption? identifier+)
    ;

revokeRole
@init {pushMsg("revoke role", state);}
@after {popMsg(state);}
    : KW_REVOKE KW_ROLE? identifier (COMMA identifier)* KW_FROM principalSpecification withAdminOption?
    -> ^(TOK_REVOKE_ROLE principalSpecification withAdminOption? identifier+)
    ;

showRoleGrants
@init {pushMsg("show role grants", state);}
@after {popMsg(state);}
    : KW_SHOW KW_ROLE KW_GRANT principalName
    -> ^(TOK_SHOW_ROLE_GRANT principalName)
    ;


showRoles
@init {pushMsg("show roles", state);}
@after {popMsg(state);}
    : KW_SHOW KW_ROLES
    -> ^(TOK_SHOW_ROLES)
    ;

showCurrentRole
@init {pushMsg("show current role", state);}
@after {popMsg(state);}
    : KW_SHOW KW_CURRENT KW_ROLES
    -> ^(TOK_SHOW_SET_ROLE)
    ;

setRole
@init {pushMsg("set role", state);}
@after {popMsg(state);}
    : KW_SET KW_ROLE roleName=identifier
    -> ^(TOK_SHOW_SET_ROLE $roleName)
    ;

showGrants
@init {pushMsg("show grants", state);}
@after {popMsg(state);}
    : KW_SHOW KW_GRANT principalName? (KW_ON privilegeIncludeColObject)?
    -> ^(TOK_SHOW_GRANT principalName? privilegeIncludeColObject?)
    ;

showRolePrincipals
@init {pushMsg("show role principals", state);}
@after {popMsg(state);}
    : KW_SHOW KW_PRINCIPALS roleName=identifier
    -> ^(TOK_SHOW_ROLE_PRINCIPALS $roleName)
    ;


privilegeIncludeColObject
@init {pushMsg("privilege object including columns", state);}
@after {popMsg(state);}
    : KW_ALL -> ^(TOK_RESOURCE_ALL)
    | privObjectType identifier (LPAREN cols=columnNameList RPAREN)? partitionSpec?
    -> ^(TOK_PRIV_OBJECT_COL identifier privObjectType $cols? partitionSpec?)
    ;

privilegeObject
@init {pushMsg("privilege subject", state);}
@after {popMsg(state);}
    : KW_ON privObjectType identifier partitionSpec?
    -> ^(TOK_PRIV_OBJECT identifier privObjectType partitionSpec?)
    ;


// database or table type. Type is optional, default type is table
privObjectType
@init {pushMsg("privilege object type type", state);}
@after {popMsg(state);}
    : KW_DATABASE -> ^(TOK_DB_TYPE)
    | KW_TABLE? -> ^(TOK_TABLE_TYPE)
    ;


privilegeList
@init {pushMsg("grant privilege list", state);}
@after {popMsg(state);}
    : privlegeDef (COMMA privlegeDef)*
    -> ^(TOK_PRIVILEGE_LIST privlegeDef+)
    ;

privlegeDef
@init {pushMsg("grant privilege", state);}
@after {popMsg(state);}
    : privilegeType (LPAREN cols=columnNameList RPAREN)?
    -> ^(TOK_PRIVILEGE privilegeType $cols?)
    ;

privilegeType
@init {pushMsg("privilege type", state);}
@after {popMsg(state);}
    : KW_ALL -> ^(TOK_PRIV_ALL)
    | KW_ALTER -> ^(TOK_PRIV_ALTER_METADATA)
    | KW_UPDATE -> ^(TOK_PRIV_ALTER_DATA)
    | KW_CREATE -> ^(TOK_PRIV_CREATE)
    | KW_DROP -> ^(TOK_PRIV_DROP)
    | KW_INDEX -> ^(TOK_PRIV_INDEX)
    | KW_LOCK -> ^(TOK_PRIV_LOCK)
    | KW_SELECT -> ^(TOK_PRIV_SELECT)
    | KW_SHOW_DATABASE -> ^(TOK_PRIV_SHOW_DATABASE)
    | KW_INSERT -> ^(TOK_PRIV_INSERT)
    | KW_DELETE -> ^(TOK_PRIV_DELETE)
    ;

principalSpecification
@init { pushMsg("user/group/role name list", state); }
@after { popMsg(state); }
    : principalName (COMMA principalName)* -> ^(TOK_PRINCIPAL_NAME principalName+)
    ;

principalName
@init {pushMsg("user|group|role name", state);}
@after {popMsg(state);}
    : KW_USER identifier -> ^(TOK_USER identifier)
    | KW_GROUP identifier -> ^(TOK_GROUP identifier)
    | KW_ROLE identifier -> ^(TOK_ROLE identifier)
    ;

withGrantOption
@init {pushMsg("with grant option", state);}
@after {popMsg(state);}
    : KW_WITH KW_GRANT KW_OPTION
    -> ^(TOK_GRANT_WITH_OPTION)
    ;

withAdminOption
@init {pushMsg("with admin option", state);}
@after {popMsg(state);}
    : KW_WITH KW_ADMIN KW_OPTION
    -> ^(TOK_GRANT_WITH_ADMIN_OPTION)
    ;

metastoreCheck
@init { pushMsg("metastore check statement", state); }
@after { popMsg(state); }
    : KW_MSCK (repair=KW_REPAIR)? (KW_TABLE table=identifier partitionSpec? (COMMA partitionSpec)*)?
    -> ^(TOK_MSCK $repair? ($table partitionSpec*)?)
    ;

resourceList
@init { pushMsg("resource list", state); }
@after { popMsg(state); }
  :
  resource (COMMA resource)* -> ^(TOK_RESOURCE_LIST resource+)
  ;

resource
@init { pushMsg("resource", state); }
@after { popMsg(state); }
  :
  resType=resourceType resPath=StringLiteral -> ^(TOK_RESOURCE_URI $resType $resPath)
  ;

resourceType
@init { pushMsg("resource type", state); }
@after { popMsg(state); }
  :
  KW_JAR -> ^(TOK_JAR)
  |
  KW_FILE -> ^(TOK_FILE)
  |
  KW_ARCHIVE -> ^(TOK_ARCHIVE)
  ;

createFunctionStatement
@init { pushMsg("create function statement", state); }
@after { popMsg(state); }
    : KW_CREATE (temp=KW_TEMPORARY)? KW_FUNCTION functionIdentifier KW_AS StringLiteral
      (KW_USING rList=resourceList)?
    -> {$temp != null}? ^(TOK_CREATEFUNCTION functionIdentifier StringLiteral $rList? TOK_TEMPORARY)
    ->                  ^(TOK_CREATEFUNCTION functionIdentifier StringLiteral $rList?)
    ;

dropFunctionStatement
@init { pushMsg("drop function statement", state); }
@after { popMsg(state); }
    : KW_DROP (temp=KW_TEMPORARY)? KW_FUNCTION ifExists? functionIdentifier
    -> {$temp != null}? ^(TOK_DROPFUNCTION functionIdentifier ifExists? TOK_TEMPORARY)
    ->                  ^(TOK_DROPFUNCTION functionIdentifier ifExists?)
    ;

createMacroStatement
@init { pushMsg("create macro statement", state); }
@after { popMsg(state); }
    : KW_CREATE KW_TEMPORARY KW_MACRO Identifier
      LPAREN columnNameTypeList? RPAREN expression
    -> ^(TOK_CREATEMACRO Identifier columnNameTypeList? expression)
    ;

dropMacroStatement
@init { pushMsg("drop macro statement", state); }
@after { popMsg(state); }
    : KW_DROP KW_TEMPORARY KW_MACRO ifExists? Identifier
    -> ^(TOK_DROPMACRO Identifier ifExists?)
    ;

createViewStatement
@init {
    pushMsg("create view statement", state);
}
@after { popMsg(state); }
    : KW_CREATE (orReplace)? KW_VIEW (ifNotExists)? name=tableName
        (LPAREN columnNameCommentList RPAREN)? tableComment? viewPartition?
        tablePropertiesPrefixed?
        KW_AS
        selectStatementWithCTE
    -> ^(TOK_CREATEVIEW $name orReplace?
         ifNotExists?
         columnNameCommentList?
         tableComment?
         viewPartition?
         tablePropertiesPrefixed?
         selectStatementWithCTE
        )
    ;

viewPartition
@init { pushMsg("view partition specification", state); }
@after { popMsg(state); }
    : KW_PARTITIONED KW_ON LPAREN columnNameList RPAREN
    -> ^(TOK_VIEWPARTCOLS columnNameList)
    ;

dropViewStatement
@init { pushMsg("drop view statement", state); }
@after { popMsg(state); }
    : KW_DROP KW_VIEW ifExists? viewName -> ^(TOK_DROPVIEW viewName ifExists?)
    ;

showFunctionIdentifier
@init { pushMsg("identifier for show function statement", state); }
@after { popMsg(state); }
    : functionIdentifier
    | StringLiteral
    ;

showStmtIdentifier
@init { pushMsg("identifier for show statement", state); }
@after { popMsg(state); }
    : identifier
    | StringLiteral
    ;

tableComment
@init { pushMsg("table's comment", state); }
@after { popMsg(state); }
    :
      KW_COMMENT comment=StringLiteral  -> ^(TOK_TABLECOMMENT $comment)
    ;

tablePartition
@init { pushMsg("table partition specification", state); }
@after { popMsg(state); }
    : KW_PARTITIONED KW_BY LPAREN columnNameTypeList RPAREN
    -> ^(TOK_TABLEPARTCOLS columnNameTypeList)
    ;

tableBuckets
@init { pushMsg("table buckets specification", state); }
@after { popMsg(state); }
    :
      KW_CLUSTERED KW_BY LPAREN bucketCols=columnNameList RPAREN (KW_SORTED KW_BY LPAREN sortCols=columnNameOrderList RPAREN)? KW_INTO num=Number KW_BUCKETS
    -> ^(TOK_TABLEBUCKETS $bucketCols $sortCols? $num)
    ;

tableSkewed
@init { pushMsg("table skewed specification", state); }
@after { popMsg(state); }
    :
     KW_SKEWED KW_BY LPAREN skewedCols=columnNameList RPAREN KW_ON LPAREN (skewedValues=skewedValueElement) RPAREN (storedAsDirs)?
    -> ^(TOK_TABLESKEWED $skewedCols $skewedValues storedAsDirs?)
    ;

rowFormat
@init { pushMsg("serde specification", state); }
@after { popMsg(state); }
    : rowFormatSerde -> ^(TOK_SERDE rowFormatSerde)
    | rowFormatDelimited -> ^(TOK_SERDE rowFormatDelimited)
    |   -> ^(TOK_SERDE)
    ;

recordReader
@init { pushMsg("record reader specification", state); }
@after { popMsg(state); }
    : KW_RECORDREADER StringLiteral -> ^(TOK_RECORDREADER StringLiteral)
    |   -> ^(TOK_RECORDREADER)
    ;

recordWriter
@init { pushMsg("record writer specification", state); }
@after { popMsg(state); }
    : KW_RECORDWRITER StringLiteral -> ^(TOK_RECORDWRITER StringLiteral)
    |   -> ^(TOK_RECORDWRITER)
    ;

rowFormatSerde
@init { pushMsg("serde format specification", state); }
@after { popMsg(state); }
    : KW_ROW KW_FORMAT KW_SERDE name=StringLiteral (KW_WITH KW_SERDEPROPERTIES serdeprops=tableProperties)?
    -> ^(TOK_SERDENAME $name $serdeprops?)
    ;

rowFormatDelimited
@init { pushMsg("serde properties specification", state); }
@after { popMsg(state); }
    :
      KW_ROW KW_FORMAT KW_DELIMITED tableRowFormatFieldIdentifier? tableRowFormatCollItemsIdentifier? tableRowFormatMapKeysIdentifier? tableRowFormatLinesIdentifier? tableRowNullFormat?
    -> ^(TOK_SERDEPROPS tableRowFormatFieldIdentifier? tableRowFormatCollItemsIdentifier? tableRowFormatMapKeysIdentifier? tableRowFormatLinesIdentifier? tableRowNullFormat?)
    ;

tableRowFormat
@init { pushMsg("table row format specification", state); }
@after { popMsg(state); }
    :
      rowFormatDelimited
    -> ^(TOK_TABLEROWFORMAT rowFormatDelimited)
    | rowFormatSerde
    -> ^(TOK_TABLESERIALIZER rowFormatSerde)
    ;

tablePropertiesPrefixed
@init { pushMsg("table properties with prefix", state); }
@after { popMsg(state); }
    :
        KW_TBLPROPERTIES! tableProperties
    ;

tableProperties
@init { pushMsg("table properties", state); }
@after { popMsg(state); }
    :
      LPAREN tablePropertiesList RPAREN -> ^(TOK_TABLEPROPERTIES tablePropertiesList)
    ;

tablePropertiesList
@init { pushMsg("table properties list", state); }
@after { popMsg(state); }
    :
      keyValueProperty (COMMA keyValueProperty)* -> ^(TOK_TABLEPROPLIST keyValueProperty+)
    |
      keyProperty (COMMA keyProperty)* -> ^(TOK_TABLEPROPLIST keyProperty+)
    ;

keyValueProperty
@init { pushMsg("specifying key/value property", state); }
@after { popMsg(state); }
    :
      key=StringLiteral EQUAL value=StringLiteral -> ^(TOK_TABLEPROPERTY $key $value)
    ;

keyProperty
@init { pushMsg("specifying key property", state); }
@after { popMsg(state); }
    :
      key=StringLiteral -> ^(TOK_TABLEPROPERTY $key TOK_NULL)
    ;

tableRowFormatFieldIdentifier
@init { pushMsg("table row format's field separator", state); }
@after { popMsg(state); }
    :
      KW_FIELDS KW_TERMINATED KW_BY fldIdnt=StringLiteral (KW_ESCAPED KW_BY fldEscape=StringLiteral)?
    -> ^(TOK_TABLEROWFORMATFIELD $fldIdnt $fldEscape?)
    ;

tableRowFormatCollItemsIdentifier
@init { pushMsg("table row format's column separator", state); }
@after { popMsg(state); }
    :
      KW_COLLECTION KW_ITEMS KW_TERMINATED KW_BY collIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATCOLLITEMS $collIdnt)
    ;

tableRowFormatMapKeysIdentifier
@init { pushMsg("table row format's map key separator", state); }
@after { popMsg(state); }
    :
      KW_MAP KW_KEYS KW_TERMINATED KW_BY mapKeysIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATMAPKEYS $mapKeysIdnt)
    ;

tableRowFormatLinesIdentifier
@init { pushMsg("table row format's line separator", state); }
@after { popMsg(state); }
    :
      KW_LINES KW_TERMINATED KW_BY linesIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATLINES $linesIdnt)
    ;

tableRowNullFormat
@init { pushMsg("table row format's null specifier", state); }
@after { popMsg(state); }
    :
      KW_NULL KW_DEFINED KW_AS nullIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATNULL $nullIdnt)
    ;
tableFileFormat
@init { pushMsg("table file format specification", state); }
@after { popMsg(state); }
    :
      KW_STORED KW_AS KW_SEQUENCEFILE  -> TOK_TBLSEQUENCEFILE
      | KW_STORED KW_AS KW_TEXTFILE  -> TOK_TBLTEXTFILE
      | KW_STORED KW_AS KW_RCFILE  -> TOK_TBLRCFILE
      | KW_STORED KW_AS KW_ORCFILE -> TOK_TBLORCFILE
      | KW_STORED KW_AS KW_PARQUETFILE -> TOK_TBLPARQUETFILE
      | KW_STORED KW_AS KW_INPUTFORMAT inFmt=StringLiteral KW_OUTPUTFORMAT outFmt=StringLiteral (KW_INPUTDRIVER inDriver=StringLiteral KW_OUTPUTDRIVER outDriver=StringLiteral)?
      -> ^(TOK_TABLEFILEFORMAT $inFmt $outFmt $inDriver? $outDriver?)
      | KW_STORED KW_BY storageHandler=StringLiteral
         (KW_WITH KW_SERDEPROPERTIES serdeprops=tableProperties)?
      -> ^(TOK_STORAGEHANDLER $storageHandler $serdeprops?)
      | KW_STORED KW_AS genericSpec=identifier
      -> ^(TOK_FILEFORMAT_GENERIC $genericSpec)
    ;

tableLocation
@init { pushMsg("table location specification", state); }
@after { popMsg(state); }
    :
      KW_LOCATION locn=StringLiteral -> ^(TOK_TABLELOCATION $locn)
    ;

columnNameTypeList
@init { pushMsg("column name type list", state); }
@after { popMsg(state); }
    : columnNameType (COMMA columnNameType)* -> ^(TOK_TABCOLLIST columnNameType+)
    ;

columnNameColonTypeList
@init { pushMsg("column name type list", state); }
@after { popMsg(state); }
    : columnNameColonType (COMMA columnNameColonType)* -> ^(TOK_TABCOLLIST columnNameColonType+)
    ;

columnNameList
@init { pushMsg("column name list", state); }
@after { popMsg(state); }
    : columnName (COMMA columnName)* -> ^(TOK_TABCOLNAME columnName+)
    ;

columnName
@init { pushMsg("column name", state); }
@after { popMsg(state); }
    :
      identifier
    ;

columnNameOrderList
@init { pushMsg("column name order list", state); }
@after { popMsg(state); }
    : columnNameOrder (COMMA columnNameOrder)* -> ^(TOK_TABCOLNAME columnNameOrder+)
    ;

skewedValueElement
@init { pushMsg("skewed value element", state); }
@after { popMsg(state); }
    : 
      skewedColumnValues
     | skewedColumnValuePairList
    ;

skewedColumnValuePairList
@init { pushMsg("column value pair list", state); }
@after { popMsg(state); }
    : skewedColumnValuePair (COMMA skewedColumnValuePair)* -> ^(TOK_TABCOLVALUE_PAIR skewedColumnValuePair+)
    ;

skewedColumnValuePair
@init { pushMsg("column value pair", state); }
@after { popMsg(state); }
    : 
      LPAREN colValues=skewedColumnValues RPAREN 
      -> ^(TOK_TABCOLVALUES $colValues)
    ;

skewedColumnValues
@init { pushMsg("column values", state); }
@after { popMsg(state); }
    : skewedColumnValue (COMMA skewedColumnValue)* -> ^(TOK_TABCOLVALUE skewedColumnValue+)
    ;

skewedColumnValue
@init { pushMsg("column value", state); }
@after { popMsg(state); }
    :
      constant
    ;

skewedValueLocationElement
@init { pushMsg("skewed value location element", state); }
@after { popMsg(state); }
    : 
      skewedColumnValue
     | skewedColumnValuePair
    ;
    
columnNameOrder
@init { pushMsg("column name order", state); }
@after { popMsg(state); }
    : identifier (asc=KW_ASC | desc=KW_DESC)?
    -> {$desc == null}? ^(TOK_TABSORTCOLNAMEASC identifier)
    ->                  ^(TOK_TABSORTCOLNAMEDESC identifier)
    ;

columnNameCommentList
@init { pushMsg("column name comment list", state); }
@after { popMsg(state); }
    : columnNameComment (COMMA columnNameComment)* -> ^(TOK_TABCOLNAME columnNameComment+)
    ;

columnNameComment
@init { pushMsg("column name comment", state); }
@after { popMsg(state); }
    : colName=identifier (KW_COMMENT comment=StringLiteral)?
    -> ^(TOK_TABCOL $colName TOK_NULL $comment?)
    ;

columnRefOrder
@init { pushMsg("column order", state); }
@after { popMsg(state); }
    : expression (asc=KW_ASC | desc=KW_DESC)?
    -> {$desc == null}? ^(TOK_TABSORTCOLNAMEASC expression)
    ->                  ^(TOK_TABSORTCOLNAMEDESC expression)
    ;

columnNameType
@init { pushMsg("column specification", state); }
@after { popMsg(state); }
    : colName=identifier colType (KW_COMMENT comment=StringLiteral)?
    -> {$comment == null}? ^(TOK_TABCOL $colName colType)
    ->                     ^(TOK_TABCOL $colName colType $comment)
    ;

columnNameColonType
@init { pushMsg("column specification", state); }
@after { popMsg(state); }
    : colName=identifier COLON colType (KW_COMMENT comment=StringLiteral)?
    -> {$comment == null}? ^(TOK_TABCOL $colName colType)
    ->                     ^(TOK_TABCOL $colName colType $comment)
    ;

colType
@init { pushMsg("column type", state); }
@after { popMsg(state); }
    : type
    ;

colTypeList
@init { pushMsg("column type list", state); }
@after { popMsg(state); }
    : colType (COMMA colType)* -> ^(TOK_COLTYPELIST colType+)
    ;

type
    : primitiveType
    | listType
    | structType
    | mapType
    | unionType;

primitiveType
@init { pushMsg("primitive type specification", state); }
@after { popMsg(state); }
    : KW_TINYINT       ->    TOK_TINYINT
    | KW_SMALLINT      ->    TOK_SMALLINT
    | KW_INT           ->    TOK_INT
    | KW_BIGINT        ->    TOK_BIGINT
    | KW_BOOLEAN       ->    TOK_BOOLEAN
    | KW_FLOAT         ->    TOK_FLOAT
    | KW_DOUBLE        ->    TOK_DOUBLE
    | KW_DATE          ->    TOK_DATE
    | KW_DATETIME      ->    TOK_DATETIME
    | KW_TIMESTAMP     ->    TOK_TIMESTAMP
    | KW_STRING        ->    TOK_STRING
    | KW_BINARY        ->    TOK_BINARY
    | KW_DECIMAL (LPAREN prec=Number (COMMA scale=Number)? RPAREN)? -> ^(TOK_DECIMAL $prec? $scale?)
    | KW_VARCHAR LPAREN length=Number RPAREN      ->    ^(TOK_VARCHAR $length)
    | KW_CHAR LPAREN length=Number RPAREN      ->    ^(TOK_CHAR $length)
    ;

listType
@init { pushMsg("list type", state); }
@after { popMsg(state); }
    : KW_ARRAY LESSTHAN type GREATERTHAN   -> ^(TOK_LIST type)
    ;

structType
@init { pushMsg("struct type", state); }
@after { popMsg(state); }
    : KW_STRUCT LESSTHAN columnNameColonTypeList GREATERTHAN -> ^(TOK_STRUCT columnNameColonTypeList)
    ;

mapType
@init { pushMsg("map type", state); }
@after { popMsg(state); }
    : KW_MAP LESSTHAN left=primitiveType COMMA right=type GREATERTHAN
    -> ^(TOK_MAP $left $right)
    ;

unionType
@init { pushMsg("uniontype type", state); }
@after { popMsg(state); }
    : KW_UNIONTYPE LESSTHAN colTypeList GREATERTHAN -> ^(TOK_UNIONTYPE colTypeList)
    ;

setOperator
@init { pushMsg("set operator", state); }
@after { popMsg(state); }
    : KW_UNION KW_ALL -> ^(TOK_UNION)
    ;

queryStatementExpression[boolean topLevel]
    :
    /* Would be nice to do this as a gated semantic perdicate
       But the predicate gets pushed as a lookahead decision.
       Calling rule doesnot know about topLevel
    */
    (w=withClause {topLevel}?)?
    queryStatementExpressionBody[topLevel] {
      if ($w.tree != null) {
      adaptor.addChild($queryStatementExpressionBody.tree, $w.tree);
      }
    }
    ->  queryStatementExpressionBody
    ;

queryStatementExpressionBody[boolean topLevel]
    :
    fromStatement[topLevel]
    | regularBody[topLevel]
    ;

withClause
  :
  KW_WITH cteStatement (COMMA cteStatement)* -> ^(TOK_CTE cteStatement+)
;

cteStatement
   :
   identifier KW_AS LPAREN queryStatementExpression[false] RPAREN
   -> ^(TOK_SUBQUERY queryStatementExpression identifier)
;

fromStatement[boolean topLevel]
: (singleFromStatement  -> singleFromStatement)
	(u=setOperator r=singleFromStatement
	  -> ^($u {$fromStatement.tree} $r)
	)*
	 -> {u != null && topLevel}? ^(TOK_QUERY
	       ^(TOK_FROM
	         ^(TOK_SUBQUERY
	           {$fromStatement.tree}
	            {adaptor.create(Identifier, generateUnionAlias())}
	           )
	        )
	       ^(TOK_INSERT 
	          ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
	          ^(TOK_SELECT ^(TOK_SELEXPR TOK_ALLCOLREF))
	        )
	      )
    -> {$fromStatement.tree}
	;


singleFromStatement
    :
    fromClause
    ( b+=body )+ -> ^(TOK_QUERY fromClause body+)
    ;

regularBody[boolean topLevel]
   :
   i=insertClause
   s=selectStatement[topLevel]
     {$s.tree.getChild(1).replaceChildren(0, 0, $i.tree);} -> {$s.tree}
   |
   selectStatement[topLevel]
   ;

 selectStatement[boolean topLevel]
 : (singleSelectStatement -> singleSelectStatement)
   (u=setOperator b=singleSelectStatement
       -> ^($u {$selectStatement.tree} $b)
   )*
   -> {u != null && topLevel}? ^(TOK_QUERY
         ^(TOK_FROM
           ^(TOK_SUBQUERY
             {$selectStatement.tree}
              {adaptor.create(Identifier, generateUnionAlias())}
             )
          )
         ^(TOK_INSERT 
            ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
            ^(TOK_SELECT ^(TOK_SELEXPR TOK_ALLCOLREF))
          )
        )
    -> {$selectStatement.tree}
 ;

singleSelectStatement
   :
   selectClause
   fromClause?
   whereClause?
   groupByClause?
   havingClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   window_clause?
   limitClause? -> ^(TOK_QUERY fromClause? ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
                     selectClause whereClause? groupByClause? havingClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? window_clause? limitClause?))
   ;

selectStatementWithCTE
    :
    (w=withClause)?
    selectStatement[true] {
      if ($w.tree != null) {
      adaptor.addChild($selectStatement.tree, $w.tree);
      }
    }
    ->  selectStatement
    ;

body
   :
   insertClause
   selectClause
   lateralView?
   whereClause?
   groupByClause?
   havingClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   window_clause?
   limitClause? -> ^(TOK_INSERT insertClause
                     selectClause lateralView? whereClause? groupByClause? havingClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? window_clause? limitClause?)
   |
   selectClause
   lateralView?
   whereClause?
   groupByClause?
   havingClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   window_clause?
   limitClause? -> ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
                     selectClause lateralView? whereClause? groupByClause? havingClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? window_clause? limitClause?)
   ;

insertClause
@init { pushMsg("insert clause", state); }
@after { popMsg(state); }
   :
     KW_INSERT KW_OVERWRITE destination ifNotExists? -> ^(TOK_DESTINATION destination ifNotExists?)
   | KW_INSERT KW_INTO KW_TABLE tableOrPartition
       -> ^(TOK_INSERT_INTO tableOrPartition)
   ;

destination
@init { pushMsg("destination specification", state); }
@after { popMsg(state); }
   :
     KW_LOCAL KW_DIRECTORY StringLiteral tableRowFormat? tableFileFormat? -> ^(TOK_LOCAL_DIR StringLiteral tableRowFormat? tableFileFormat?)
   | KW_DIRECTORY StringLiteral -> ^(TOK_DIR StringLiteral)
   | KW_TABLE tableOrPartition -> tableOrPartition
   ;

limitClause
@init { pushMsg("limit clause", state); }
@after { popMsg(state); }
   :
   KW_LIMIT num=Number -> ^(TOK_LIMIT $num)
   ;
