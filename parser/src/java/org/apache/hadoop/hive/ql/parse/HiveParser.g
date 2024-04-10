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
tokenVocab=HiveLexerParent;
output=AST;
ASTLabelType=ASTNode;
backtrack=false;
k=3;
}

import AlterClauseParser, SelectClauseParser, FromClauseParser, IdentifiersParser, ResourcePlanParser, CreateDDLParser, PrepareStatementParser;

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
TOK_TABREF;
TOK_SUBQUERY;
TOK_PREPARE;
TOK_EXECUTE_PARAM_LIST;
TOK_INSERT_INTO;
TOK_DESTINATION;
TOK_ALLCOLREF;
TOK_SETCOLREF;
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
TOK_UNKNOWN;
TOK_TRANSFORM;
TOK_SERDE;
TOK_SERDENAME;
TOK_SERDEPROPS;
TOK_EXPLIST;
TOK_ALIAS;
TOK_ALIASLIST;
TOK_GROUPBY;
TOK_ROLLUP_GROUPBY;
TOK_CUBE_GROUPBY;
TOK_GROUPING_SETS;
TOK_GROUPING_SETS_EXPRESSION;
TOK_HAVING;
TOK_QUALIFY;
TOK_ORDERBY;
TOK_NULLS_FIRST;
TOK_NULLS_LAST;
TOK_CLUSTERBY;
TOK_DISTRIBUTEBY;
TOK_SORTBY;
TOK_UNIONALL;
TOK_UNIONDISTINCT;
TOK_INTERSECTALL;
TOK_INTERSECTDISTINCT;
TOK_EXCEPTALL;
TOK_EXCEPTDISTINCT;
TOK_JOIN;
TOK_LEFTOUTERJOIN;
TOK_RIGHTOUTERJOIN;
TOK_FULLOUTERJOIN;
TOK_UNIQUEJOIN;
TOK_CROSSJOIN;
TOK_LOAD;
TOK_EXPORT;
TOK_IMPORT;
TOK_REPLICATION;
TOK_METADATA;
TOK_NULL;
TOK_NOT_NULL;
TOK_UNIQUE;
TOK_PRIMARY_KEY;
TOK_FOREIGN_KEY;
TOK_DEFAULT_VALUE;
TOK_CHECK_CONSTRAINT;
TOK_VALIDATE;
TOK_NOVALIDATE;
TOK_RELY;
TOK_NORELY;
TOK_CONSTRAINT_NAME;
TOK_TINYINT;
TOK_SMALLINT;
TOK_INT;
TOK_BIGINT;
TOK_BOOLEAN;
TOK_FLOAT;
TOK_REAL;
TOK_DOUBLE;
TOK_DATE;
TOK_DATELITERAL;
TOK_DATETIME;
TOK_TIMESTAMP;
TOK_TIMESTAMPLITERAL;
TOK_TIMESTAMPLOCALTZ;
TOK_TIMESTAMPLOCALTZLITERAL;
TOK_INTERVAL_YEAR_MONTH;
TOK_INTERVAL_YEAR_MONTH_LITERAL;
TOK_INTERVAL_DAY_TIME;
TOK_INTERVAL_DAY_TIME_LITERAL;
TOK_INTERVAL_YEAR_LITERAL;
TOK_INTERVAL_MONTH_LITERAL;
TOK_INTERVAL_DAY_LITERAL;
TOK_INTERVAL_HOUR_LITERAL;
TOK_INTERVAL_MINUTE_LITERAL;
TOK_INTERVAL_SECOND_LITERAL;
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
TOK_CREATEDATACONNECTOR;
TOK_CREATETABLE;
TOK_TRUNCATETABLE;
TOK_LIKEFILE;
TOK_LIKETABLE;
TOK_DATACONNECTOR;
TOK_DATACONNECTORCOMMENT;
TOK_DATACONNECTORTYPE;
TOK_DATACONNECTORURL;
TOK_DATACONNECTOROWNER;
TOK_DATACONNECTORPROPERTIES;
TOK_DROPDATACONNECTOR;
TOK_DESCTABLE;
TOK_DESCFUNCTION;
TOK_ALTERTABLE;
TOK_ALTERTABLE_RENAME;
TOK_ALTERTABLE_ADDCOLS;
TOK_ALTERTABLE_RENAMECOL;
TOK_ALTERTABLE_RENAMEPART;
TOK_ALTERTABLE_REPLACECOLS;
TOK_ALTERTABLE_ADDPARTS;
TOK_ALTERTABLE_DROPPARTS;
TOK_ALTERTABLE_PARTCOLTYPE;
TOK_ALTERTABLE_MERGEFILES;
TOK_ALTERPARTITION_MERGEFILES;
TOK_ALTERTABLE_TOUCH;
TOK_ALTERTABLE_ARCHIVE;
TOK_ALTERTABLE_UNARCHIVE;
TOK_ALTERTABLE_SETSERDEPROPERTIES;
TOK_ALTERPARTITION_SETSERDEPROPERTIES;
TOK_ALTERTABLE_UNSETSERDEPROPERTIES;
TOK_ALTERPARTITION_UNSETSERDEPROPERTIES;
TOK_ALTERTABLE_SERIALIZER;
TOK_ALTERPARTITION_SERIALIZER;
TOK_ALTERTABLE_UPDATECOLSTATS;
TOK_ALTERPARTITION_UPDATECOLSTATS;
TOK_ALTERTABLE_UPDATESTATS;
TOK_ALTERPARTITION_UPDATESTATS;
TOK_TABLE_PARTITION;
TOK_ALTERTABLE_FILEFORMAT;
TOK_ALTERPARTITION_FILEFORMAT;
TOK_ALTERTABLE_LOCATION;
TOK_ALTERPARTITION_LOCATION;
TOK_ALTERTABLE_PROPERTIES;
TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION;
TOK_ALTERTABLE_DROPPROPERTIES;
TOK_ALTERTABLE_SKEWED;
TOK_ALTERTABLE_EXCHANGEPARTITION;
TOK_ALTERTABLE_SKEWED_LOCATION;
TOK_ALTERTABLE_BUCKETS;
TOK_ALTERPARTITION_BUCKETS;
TOK_ALTERTABLE_CLUSTER_SORT;
TOK_ALTERTABLE_COMPACT;
TOK_COMPACT_POOL;
TOK_COMPACT_ID;
TOK_ALTERTABLE_DROPCONSTRAINT;
TOK_ALTERTABLE_ADDCONSTRAINT;
TOK_ALTERTABLE_UPDATECOLUMNS;
TOK_ALTERTABLE_OWNER;
TOK_ALTERTABLE_SETPARTSPEC;
TOK_ALTERTABLE_EXECUTE;
TOK_ALTERTABLE_CREATE_BRANCH;
TOK_ALTERTABLE_DROP_BRANCH;
TOK_ALTERTABLE_RENAME_BRANCH;
TOK_ALTERTABLE_CREATE_TAG;
TOK_ALTERTABLE_DROP_TAG;
TOK_RETAIN;
TOK_WITH_SNAPSHOT_RETENTION;
TOK_ALTERTABLE_CONVERT;
TOK_MSCK;
TOK_SHOWDATABASES;
TOK_SHOWDATACONNECTORS;
TOK_SHOWTABLES;
TOK_SHOWCOLUMNS;
TOK_SHOWFUNCTIONS;
TOK_SHOWPARTITIONS;
TOK_SHOW_CREATEDATABASE;
TOK_SHOW_CREATETABLE;
TOK_SHOW_TABLESTATUS;
TOK_SHOW_TBLPROPERTIES;
TOK_SHOWLOCKS;
TOK_SHOWCONF;
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
TOK_TABLEPARTCOLSBYSPEC;
TOK_TABLEPARTCOLNAMES;
TOK_TABLEROWFORMAT;
TOK_TABLEROWFORMATFIELD;
TOK_TABLEROWFORMATCOLLITEMS;
TOK_TABLEROWFORMATMAPKEYS;
TOK_TABLEROWFORMATLINES;
TOK_TABLEROWFORMATNULL;
TOK_TABLEFILEFORMAT;
TOK_FILEFORMAT_GENERIC;
TOK_OFFLINE;
TOK_ENABLE;
TOK_DISABLE;
TOK_READONLY;
TOK_NO_DROP;
TOK_STORAGEHANDLER;
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
TOK_RELOADFUNCTIONS;
TOK_CREATEMACRO;
TOK_DROPMACRO;
TOK_TEMPORARY;
TOK_CREATEVIEW;
TOK_DROPVIEW;
TOK_ALTERVIEW;
TOK_ALTERVIEW_PROPERTIES;
TOK_ALTERVIEW_DROPPROPERTIES;
TOK_ALTERVIEW_ADDPARTS;
TOK_ALTERVIEW_DROPPARTS;
TOK_ALTERVIEW_RENAME;
TOK_ALTERVIEW_AS;
TOK_CREATE_MATERIALIZED_VIEW;
TOK_DROP_MATERIALIZED_VIEW;
TOK_ALTER_MATERIALIZED_VIEW;
TOK_ALTER_MATERIALIZED_VIEW_REWRITE;
TOK_ALTER_MATERIALIZED_VIEW_REBUILD;
TOK_CREATE_SCHEDULED_QUERY;
TOK_ALTER_SCHEDULED_QUERY;
TOK_DROP_SCHEDULED_QUERY;
TOK_REWRITE_ENABLED;
TOK_REWRITE_DISABLED;
TOK_VIEWPARTCOLS;
TOK_VIEWCLUSTERCOLS;
TOK_VIEWDISTRIBUTECOLS;
TOK_VIEWSORTCOLS;
TOK_EXPLAIN;
TOK_DDL;
TOK_EXPLAIN_SQ_REWRITE;
TOK_TABLESERIALIZER;
TOK_TABLEPROPERTIES;
TOK_TABLEPROPLIST;
TOK_TABTYPE;
TOK_LIMIT;
TOK_OFFSET;
TOK_TABLEPROPERTY;
TOK_IFEXISTS;
TOK_IFNOTEXISTS;
TOK_ORREPLACE;
TOK_USERSCRIPTCOLNAMES;
TOK_USERSCRIPTCOLSCHEMA;
TOK_RECORDREADER;
TOK_RECORDWRITER;
TOK_LEFTSEMIJOIN;
TOK_LEFTANTISEMIJOIN;
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
TOK_ADMIN_OPTION_FOR;
TOK_GRANT_OPTION_FOR;
TOK_PRIV_ALL;
TOK_PRIV_ALTER_METADATA;
TOK_PRIV_ALTER_DATA;
TOK_PRIV_DELETE;
TOK_PRIV_DROP;
TOK_PRIV_INSERT;
TOK_PRIV_LOCK;
TOK_PRIV_SELECT;
TOK_PRIV_SHOW_DATABASE;
TOK_PRIV_CREATE;
TOK_PRIV_OBJECT;
TOK_PRIV_OBJECT_COL;
TOK_GRANT_ROLE;
TOK_REVOKE_ROLE;
TOK_SET_ROLE;
TOK_SHOW_ROLE_GRANT;
TOK_SHOW_ROLES;
TOK_SHOW_CURRENT_ROLE;
TOK_SHOW_ROLE_PRINCIPALS;
TOK_SHOWDBLOCKS;
TOK_DESCDATABASE;
TOK_DATABASEPROPERTIES;
TOK_DATABASELOCATION;
TOK_DATABASE_MANAGEDLOCATION;
TOK_DBPROPLIST;
TOK_ALTERDATABASE_PROPERTIES;
TOK_ALTERDATABASE_OWNER;
TOK_ALTERDATABASE_LOCATION;
TOK_ALTERDATABASE_MANAGEDLOCATION;
TOK_ALTERDATACONNECTOR_PROPERTIES;
TOK_ALTERDATACONNECTOR_OWNER;
TOK_ALTERDATACONNECTOR_URL;
TOK_DESCDATACONNECTOR;
TOK_DBNAME;
TOK_TABNAME;
TOK_TABSRC;
TOK_RESTRICT;
TOK_CASCADE;
TOK_FORCE;
TOK_TABLESKEWED;
TOK_TABCOLVALUE;
TOK_TABCOLVALUE_PAIR;
TOK_TABCOLVALUES;
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
TOK_SHOW_COMPACTIONS;
TOK_SHOW_TRANSACTIONS;
TOK_COMPACTION_TYPE;
TOK_COMPACTION_STATUS;
TOK_DELETE_FROM;
TOK_UPDATE_TABLE;
TOK_SET_COLUMNS_CLAUSE;
TOK_COL_NAME;
TOK_URI_TYPE;
TOK_SERVER_TYPE;
TOK_SHOWVIEWS;
TOK_SHOWMATERIALIZEDVIEWS;
TOK_START_TRANSACTION;
TOK_ISOLATION_LEVEL;
TOK_ISOLATION_SNAPSHOT;
TOK_TXN_ACCESS_MODE;
TOK_TXN_READ_ONLY;
TOK_TXN_READ_WRITE;
TOK_COMMIT;
TOK_ROLLBACK;
TOK_SET_AUTOCOMMIT;
TOK_CACHE_METADATA;
TOK_ABORT_TRANSACTIONS;
TOK_ABORT_COMPACTIONS;
TOK_MERGE;
TOK_MATCHED;
TOK_NOT_MATCHED;
TOK_UPDATE;
TOK_DELETE;
TOK_REPL_DUMP;
TOK_REPL_LOAD;
TOK_REPL_STATUS;
TOK_REPL_CONFIG;
TOK_REPL_CONFIG_LIST;
TOK_REPL_TABLES;
TOK_REPL_TABLES_LIST;
TOK_TO;
TOK_ONLY;
TOK_SUMMARY;
TOK_OPERATOR;
TOK_EXPRESSION;
TOK_DETAIL;
TOK_BLOCKING;
TOK_KILL_QUERY;
TOK_CREATE_RP;
TOK_SHOW_RP;
TOK_ALTER_RP_ENABLE;
TOK_ALTER_RP_DISABLE;
TOK_ALTER_RP_RENAME;
TOK_ALTER_RP_SET;
TOK_ALTER_RP_UNSET;
TOK_ALTER_RP_REPLACE;
TOK_ALTER_RP_VALIDATE;
TOK_DROP_RP;
TOK_ACTIVATE;
TOK_QUERY_PARALLELISM;
TOK_RENAME;
TOK_DEFAULT_POOL;
TOK_CREATE_TRIGGER;
TOK_ALTER_TRIGGER;
TOK_DROP_TRIGGER;
TOK_TRIGGER_EXPRESSION;
TOK_CREATE_POOL;
TOK_ALTER_POOL;
TOK_ALTER_POOL_ADD_TRIGGER;
TOK_ALTER_POOL_DROP_TRIGGER;
TOK_DROP_POOL;
TOK_ALLOC_FRACTION;
TOK_SCHEDULING_POLICY;
TOK_PATH;
TOK_CREATE_MAPPING;
TOK_ALTER_MAPPING;
TOK_DROP_MAPPING;
TOK_ADD_TRIGGER;
TOK_REPLACE;
TOK_LIKERP;
TOK_UNMANAGED;
TOK_INPUTFORMAT;
TOK_WITHIN_GROUP;
TOK_CRON;
TOK_EXECUTED_AS;
TOK_EXECUTE;
TOK_SCHEDULE;
TOK_EVERY;
TOK_REMOTE;
TOK_PARAMETER;
TOK_PARAMETER_IDX;
TOK_RESPECT_NULLS;
TOK_IGNORE_NULLS;
TOK_IDENTITY;
TOK_YEAR;
TOK_MONTH;
TOK_DAY;
TOK_HOUR;
TOK_TRUNCATE;
TOK_BUCKET;
TOK_AS_OF_TIME;
TOK_AS_OF_VERSION;
TOK_FROM_VERSION;
TOK_AS_OF_TAG;
}


// Package headers
@header {
package org.apache.hadoop.hive.ql.parse;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
}


@members {
  ArrayList<ParseError> errors = new ArrayList<ParseError>();
  Stack msgs = new Stack<String>();

  private static HashMap<String, String> xlateMap;
  static {
    //this is used to support auto completion in CLI
    xlateMap = new HashMap<String, String>();

    // Keywords
    xlateMap.put("KW_TRUE", "TRUE");
    xlateMap.put("KW_FALSE", "FALSE");
    xlateMap.put("KW_UNKNOWN", "UNKNOWN");
    xlateMap.put("KW_ALL", "ALL");
    xlateMap.put("KW_NONE", "NONE");
    xlateMap.put("KW_AND", "AND");
    xlateMap.put("KW_OR", "OR");
    xlateMap.put("KW_NOT", "NOT");
    xlateMap.put("KW_LIKE", "LIKE");

    xlateMap.put("KW_ASC", "ASC");
    xlateMap.put("KW_DESC", "DESC");
    xlateMap.put("KW_NULLS", "NULLS");
    xlateMap.put("KW_LAST", "LAST");
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
    xlateMap.put("KW_SYNC", "SYNC");
    xlateMap.put("KW_UNION", "UNION");
    xlateMap.put("KW_INTERSECT", "INTERSECT");
    xlateMap.put("KW_EXCEPT", "EXCEPT");
    xlateMap.put("KW_LOAD", "LOAD");
    xlateMap.put("KW_DATA", "DATA");
    xlateMap.put("KW_OPTIMIZE", "OPTIMIZE");
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
    xlateMap.put("KW_REAL", "REAL");
    xlateMap.put("KW_DOUBLE", "DOUBLE");
    xlateMap.put("KW_PRECISION", "PRECISION");
    xlateMap.put("KW_DATE", "DATE");
    xlateMap.put("KW_DATETIME", "DATETIME");
    xlateMap.put("KW_TIMESTAMP", "TIMESTAMP");
    xlateMap.put("KW_TIMESTAMPLOCALTZ", "TIMESTAMPLOCALTZ");
    xlateMap.put("KW_TIME", "TIME");
    xlateMap.put("KW_ZONE", "ZONE");
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
    xlateMap.put("KW_MANAGEDLOCATION", "MANAGEDLOCATION");
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
    xlateMap.put("KW_FUNCTIONS", "FUNCTIONS");
    xlateMap.put("KW_EXPLAIN", "EXPLAIN");
    xlateMap.put("KW_DDL", "DDL");
    xlateMap.put("KW_EXTENDED", "EXTENDED");
    xlateMap.put("KW_DEBUG", "DEBUG");
    xlateMap.put("KW_SERDE", "SERDE");
    xlateMap.put("KW_WITH", "WITH");
    xlateMap.put("KW_SERDEPROPERTIES", "SERDEPROPERTIES");
    xlateMap.put("KW_LIMIT", "LIMIT");
    xlateMap.put("KW_OFFSET", "OFFSET");
    xlateMap.put("KW_SET", "SET");
    xlateMap.put("KW_PROPERTIES", "TBLPROPERTIES");
    xlateMap.put("KW_VALUE_TYPE", "\$VALUE\$");
    xlateMap.put("KW_ELEM_TYPE", "\$ELEM\$");
    xlateMap.put("KW_DEFINED", "DEFINED");
    xlateMap.put("KW_SUBQUERY", "SUBQUERY");
    xlateMap.put("KW_REWRITE", "REWRITE");
    xlateMap.put("KW_UPDATE", "UPDATE");
    xlateMap.put("KW_VALUES", "VALUES");
    xlateMap.put("KW_PURGE", "PURGE");
    xlateMap.put("KW_UNIQUE", "UNIQUE");
    xlateMap.put("KW_PRIMARY", "PRIMARY");
    xlateMap.put("KW_FOREIGN", "FOREIGN");
    xlateMap.put("KW_KEY", "KEY");
    xlateMap.put("KW_REFERENCES", "REFERENCES");
    xlateMap.put("KW_CONSTRAINT", "CONSTRAINT");
    xlateMap.put("KW_ENABLE", "ENABLE");
    xlateMap.put("KW_DISABLE", "DISABLE");
    xlateMap.put("KW_VALIDATE", "VALIDATE");
    xlateMap.put("KW_NOVALIDATE", "NOVALIDATE");
    xlateMap.put("KW_RELY", "RELY");
    xlateMap.put("KW_NORELY", "NORELY");
    xlateMap.put("KW_ABORT", "ABORT");
    xlateMap.put("KW_TRANSACTIONS", "TRANSACTIONS");
    xlateMap.put("KW_COMPACTIONS", "COMPACTIONS");
    xlateMap.put("KW_COMPACT", "COMPACT");
    xlateMap.put("KW_WAIT", "WAIT");
    xlateMap.put("KW_KILL", "KILL");
    xlateMap.put("KW_QUERY", "QUERY");
    xlateMap.put("KW_RESOURCE", "RESOURCE");
    xlateMap.put("KW_PLAN", "PLAN");
    xlateMap.put("KW_QUERY_PARALLELISM", "QUERY_PARALLELISM");
    xlateMap.put("KW_PLANS", "PLANS");
    xlateMap.put("KW_ACTIVATE", "ACTIVATE");
    xlateMap.put("KW_DEFAULT", "DEFAULT");
    xlateMap.put("KW_CHECK", "CHECK");
    xlateMap.put("KW_POOL", "POOL");
    xlateMap.put("KW_COMPACT_ID", "COMPACTIONID");
    xlateMap.put("KW_MOVE", "MOVE");
    xlateMap.put("KW_DO", "DO");
    xlateMap.put("KW_ALLOC_FRACTION", "ALLOC_FRACTION");
    xlateMap.put("KW_SCHEDULING_POLICY", "SCHEDULING_POLICY");
    xlateMap.put("KW_PATH", "PATH");
    xlateMap.put("KW_AST", "AST");
    xlateMap.put("KW_TRANSACTIONAL", "TRANSACTIONAL");
    xlateMap.put("KW_MANAGED", "MANAGED");
    xlateMap.put("KW_LEADING", "LEADING");
    xlateMap.put("KW_TRAILING", "TRAILING");
    xlateMap.put("KW_BOTH", "BOTH");

    xlateMap.put("KW_TYPE", "TYPE");
    xlateMap.put("KW_DATACONNECTOR", "CONNECTOR");
    xlateMap.put("KW_DATACONNECTORS", "CONNECTORS");
    xlateMap.put("KW_REMOTE", "REMOTE");
    xlateMap.put("KW_SPEC", "SPEC");
    xlateMap.put("KW_YEAR", "YEAR");
    xlateMap.put("KW_MONTH", "MONTH");
    xlateMap.put("KW_DAY", "DAY");
    xlateMap.put("KW_HOUR", "HOUR");
    xlateMap.put("KW_BUCKET", "BUCKET");
    xlateMap.put("KW_TRUNCATE", "TRUNCATE");

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
      Object o = msgs.pop();
    }
  }

  // counter to generate unique union aliases
  private int aliasCounter;
  private String generateUnionAlias() {
    return "__u" + (++aliasCounter);
  }
  private char [] excludedCharForColumnName = {'.', ':'};
  private boolean containExcludedCharForCreateTableColumnName(String input) {
    for(char c : excludedCharForColumnName) {
      if(input.indexOf(c)>-1) {
        return true;
      }
    }
    return false;
  }
  private CommonTree throwSetOpException() throws RecognitionException {
    throw new FailedPredicateException(input, "orderByClause clusterByClause distributeByClause sortByClause limitClause can only be applied to the whole union.", "");
  }
  private CommonTree throwColumnNameException() throws RecognitionException {
    throw new FailedPredicateException(input, Arrays.toString(excludedCharForColumnName) + " can not be used in column name in create table statement.", "");
  }
  private Configuration hiveConf;
  public void setHiveConf(Configuration hiveConf) {
    this.hiveConf = hiveConf;
  }
  protected boolean nullsLast() {
    if(hiveConf == null){
      return HiveConf.ConfVars.HIVE_DEFAULT_NULLS_LAST.defaultBoolVal;
    }
    return HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_DEFAULT_NULLS_LAST);
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
	: KW_EXPLAIN (
	    explainOption* execStatement -> ^(TOK_EXPLAIN execStatement explainOption*)
        |
        KW_REWRITE queryStatementExpression -> ^(TOK_EXPLAIN_SQ_REWRITE queryStatementExpression)
      )
	;

explainOption
@init { msgs.push("explain option"); }
@after { msgs.pop(); }
    : KW_EXTENDED
    | KW_FORMATTED
    | KW_DEPENDENCY
    | KW_CBO (KW_COST | KW_JOINCOST)?
    | KW_LOGICAL
    | KW_AUTHORIZATION
    | KW_ANALYZE
    | KW_REOPTIMIZATION
    | KW_LOCKS
    | KW_AST
    | (KW_VECTORIZATION vectorizationOnly? vectorizatonDetail?)
    | KW_DEBUG
    | KW_DDL
    ;

vectorizationOnly
@init { pushMsg("vectorization's only clause", state); }
@after { popMsg(state); }
    : KW_ONLY
    -> ^(TOK_ONLY)
    ;

vectorizatonDetail
@init { pushMsg("vectorization's detail level clause", state); }
@after { popMsg(state); }
    : KW_SUMMARY
    -> ^(TOK_SUMMARY)
    | KW_OPERATOR
    -> ^(TOK_OPERATOR)
    | KW_EXPRESSION
    -> ^(TOK_EXPRESSION)
    | KW_DETAIL
    -> ^(TOK_DETAIL)
    ;

execStatement
@init { pushMsg("statement", state); }
@after { popMsg(state); }
    : queryStatementExpression
    | loadStatement
    | exportStatement
    | importStatement
    | replDumpStatement
    | replLoadStatement
    | replStatusStatement
    | ddlStatement
    | deleteStatement
    | updateStatement
    | sqlTransactionStatement
    | mergeStatement
    | prepareStatement
    | executeStatement
    ;

loadStatement
@init { pushMsg("load statement", state); }
@after { popMsg(state); }
    : KW_LOAD KW_DATA (islocal=KW_LOCAL)? KW_INPATH (path=StringLiteral) (isoverwrite=KW_OVERWRITE)? KW_INTO KW_TABLE (tab=tableOrPartition) inputFileFormat?
    -> ^(TOK_LOAD $path $tab $islocal? $isoverwrite? inputFileFormat?)
    ;

replicationClause
@init { pushMsg("replication clause", state); }
@after { popMsg(state); }
    : KW_FOR (isMetadataOnly=KW_METADATA)? KW_REPLICATION LPAREN (replId=StringLiteral) RPAREN
    -> ^(TOK_REPLICATION $replId $isMetadataOnly?)
    ;

exportStatement
@init { pushMsg("export statement", state); }
@after { popMsg(state); }
    : KW_EXPORT
      KW_TABLE (tab=tableOrPartition)
      KW_TO (path=StringLiteral)
      replicationClause?
    -> ^(TOK_EXPORT $tab $path replicationClause?)
    ;

importStatement
@init { pushMsg("import statement", state); }
@after { popMsg(state); }
       : KW_IMPORT
         ((ext=KW_EXTERNAL)? KW_TABLE (tab=tableOrPartition))?
         KW_FROM (path=StringLiteral)
         tableLocation?
    -> ^(TOK_IMPORT $path $tab? $ext? tableLocation?)
    ;

replDumpStatement
@init { pushMsg("Replication dump statement", state); }
@after { popMsg(state); }
      : KW_REPL KW_DUMP
        (dbPolicy=replDbPolicy)
        (KW_REPLACE oldDbPolicy=replDbPolicy)?
        (KW_WITH replConf=replConfigs)?
    -> ^(TOK_REPL_DUMP $dbPolicy ^(TOK_REPLACE $oldDbPolicy)? $replConf?)
    ;

replDbPolicy
@init { pushMsg("Repl dump DB replication policy", state); }
@after { popMsg(state); }
    :
      (dbName=identifier) (DOT tablePolicy=replTableLevelPolicy)? -> $dbName $tablePolicy?
    ;

replLoadStatement
@init { pushMsg("Replication load statement", state); }
@after { popMsg(state); }
      : KW_REPL KW_LOAD
      (sourceDbPolicy=replDbPolicy)
      (KW_INTO dbName=identifier)?
      (KW_WITH replConf=replConfigs)?
      -> ^(TOK_REPL_LOAD $sourceDbPolicy ^(TOK_DBNAME $dbName)? $replConf?)
      ;

replConfigs
@init { pushMsg("Repl configurations", state); }
@after { popMsg(state); }
    :
      LPAREN replConfigsList RPAREN -> ^(TOK_REPL_CONFIG replConfigsList)
    ;

replConfigsList
@init { pushMsg("Repl configurations list", state); }
@after { popMsg(state); }
    :
      keyValueProperty (COMMA keyValueProperty)* -> ^(TOK_REPL_CONFIG_LIST keyValueProperty+)
    ;

replTableLevelPolicy
@init { pushMsg("Replication table level policy definition", state); }
@after { popMsg(state); }
    :
      ((replTablesIncludeList=StringLiteral) (DOT replTablesExcludeList=StringLiteral)?)
      -> ^(TOK_REPL_TABLES $replTablesIncludeList $replTablesExcludeList?)
    ;

replStatusStatement
@init { pushMsg("replication status statement", state); }
@after { popMsg(state); }
      : KW_REPL KW_STATUS
        (dbName=identifier)
        (KW_WITH replConf=replConfigs)?
      -> ^(TOK_REPL_STATUS $dbName $replConf?)
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
    | createMaterializedViewStatement
    | createScheduledQueryStatement
    | alterScheduledQueryStatement
    | dropScheduledQueryStatement
    | dropViewStatement
    | dropMaterializedViewStatement
    | createFunctionStatement
    | createMacroStatement
    | dropFunctionStatement
    | reloadFunctionsStatement
    | dropMacroStatement
    | analyzeStatement
    | lockStatement
    | unlockStatement
    | lockDatabase
    | unlockDatabase
    | createRoleStatement
    | dropRoleStatement
    | (grantPrivileges) => grantPrivileges
    | (revokePrivileges) => revokePrivileges
    | showGrants
    | showRoleGrants
    | showRolePrincipals
    | showRoles
    | grantRole
    | revokeRole
    | setRole
    | showCurrentRole
    | abortTransactionStatement
    | abortCompactionStatement
    | killQueryStatement
    | resourcePlanDdlStatements
    | createDataConnectorStatement
    | dropDataConnectorStatement
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

force
@init { msgs.push("force clause"); }
@after { msgs.pop(); }
    : KW_FORCE
    -> ^(TOK_FORCE)
    ;

rewriteEnabled
@init { pushMsg("rewrite enabled clause", state); }
@after { popMsg(state); }
    : KW_ENABLE KW_REWRITE
    -> ^(TOK_REWRITE_ENABLED)
    ;

rewriteDisabled
@init { pushMsg("rewrite disabled clause", state); }
@after { popMsg(state); }
    : KW_DISABLE KW_REWRITE
    -> ^(TOK_REWRITE_DISABLED)
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

createDatabaseStatement
@init { pushMsg("create database statement", state); }
@after { popMsg(state); }
    : KW_CREATE (KW_DATABASE|KW_SCHEMA)
        ifNotExists?
        name=identifier
        databaseComment?
        dbLocation?
        dbManagedLocation?
        (KW_WITH KW_DBPROPERTIES dbprops=dbProperties)?
    -> ^(TOK_CREATEDATABASE $name ifNotExists? dbLocation? dbManagedLocation? databaseComment? $dbprops?)

    | KW_CREATE KW_REMOTE (KW_DATABASE|KW_SCHEMA)
        ifNotExists?
        name=identifier
        databaseComment?
        dbConnectorName
        (KW_WITH KW_DBPROPERTIES dbprops=dbProperties)?
    -> ^(TOK_CREATEDATABASE $name ifNotExists? databaseComment? $dbprops? dbConnectorName)
    ;

dbLocation
@init { pushMsg("database location specification", state); }
@after { popMsg(state); }
    :
      KW_LOCATION locn=StringLiteral -> ^(TOK_DATABASELOCATION $locn)
    ;

dbManagedLocation
@init { pushMsg("database managed location specification", state); }
@after { popMsg(state); }
    :
      KW_MANAGEDLOCATION locn=StringLiteral -> ^(TOK_DATABASE_MANAGEDLOCATION $locn)
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

dbConnectorName
@init { pushMsg("remote database using connector", state); }
@after { popMsg(state); }
    :
      KW_USING dcName=identifier -> ^(TOK_DATACONNECTOR $dcName)
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

truncateTableStatement
@init { pushMsg("truncate table statement", state); }
@after { popMsg(state); }
    : KW_TRUNCATE KW_TABLE? tablePartitionPrefix (KW_COLUMNS LPAREN columnNameList RPAREN)? force?
    -> ^(TOK_TRUNCATETABLE tablePartitionPrefix columnNameList? force?);

dropTableStatement
@init { pushMsg("drop statement", state); }
@after { popMsg(state); }
    : KW_DROP KW_TABLE ifExists? tableName KW_PURGE? replicationClause?
    -> ^(TOK_DROPTABLE tableName ifExists? KW_PURGE? replicationClause?)
    ;

inputFileFormat
@init { pushMsg("Load Data input file format specification", state); }
@after { popMsg(state); }
    : KW_INPUTFORMAT inFmt=StringLiteral KW_SERDE serdeCls=StringLiteral
      -> ^(TOK_INPUTFORMAT $inFmt $serdeCls)
    ;

tabTypeExpr
@init { pushMsg("specifying table types", state); }
@after { popMsg(state); }
   : identifier (DOT^ identifier)?
   (identifier (DOT^
   (
   (KW_ELEM_TYPE) => KW_ELEM_TYPE
   |
   (KW_KEY_TYPE) => KW_KEY_TYPE
   |
   (KW_VALUE_TYPE) => KW_VALUE_TYPE
   | identifier
   ))*
   )?
   ;

partTypeExpr
@init { pushMsg("specifying table partitions", state); }
@after { popMsg(state); }
    :  tabTypeExpr partitionSpec? -> ^(TOK_TABTYPE tabTypeExpr partitionSpec?)
    ;

tabPartColTypeExpr
@init { pushMsg("specifying table partitions columnName", state); }
@after { popMsg(state); }
    :  tableName partitionSpec? extColumnName? -> ^(TOK_TABTYPE tableName partitionSpec? extColumnName?)
    ;

descStatement
@init { pushMsg("describe statement", state); }
@after { popMsg(state); }
    :
    (KW_DESCRIBE|KW_DESC)
    (
    (KW_DATABASE|KW_SCHEMA) => (KW_DATABASE|KW_SCHEMA) KW_EXTENDED? (dbName=identifier) -> ^(TOK_DESCDATABASE $dbName KW_EXTENDED?)
    |
    (KW_DATACONNECTOR) => (KW_DATACONNECTOR) KW_EXTENDED? (dcName=identifier) -> ^(TOK_DESCDATACONNECTOR $dcName KW_EXTENDED?)
    |
    (KW_FUNCTION) => KW_FUNCTION KW_EXTENDED? (name=descFuncNames) -> ^(TOK_DESCFUNCTION $name KW_EXTENDED?)
    |
    (KW_FORMATTED|KW_EXTENDED) => ((descOptions=KW_FORMATTED|descOptions=KW_EXTENDED) parttype=tabPartColTypeExpr) -> ^(TOK_DESCTABLE $parttype $descOptions)
    |
    parttype=tabPartColTypeExpr -> ^(TOK_DESCTABLE $parttype)
    )
    ;

analyzeStatement
@init { pushMsg("analyze statement", state); }
@after { popMsg(state); }
    : KW_ANALYZE KW_TABLE (parttype=tableOrPartition)
      (
      (KW_COMPUTE) => KW_COMPUTE KW_STATISTICS ((noscan=KW_NOSCAN)
                                                      | (KW_FOR KW_COLUMNS (statsColumnName=columnNameList)?))?
      -> ^(TOK_ANALYZE $parttype $noscan? KW_COLUMNS? $statsColumnName?)
      |
      (KW_CACHE) => KW_CACHE KW_METADATA -> ^(TOK_CACHE_METADATA $parttype)
      )
    ;

showStatement
@init { pushMsg("show statement", state); }
@after { popMsg(state); }
    : KW_SHOW (KW_DATABASES|KW_SCHEMAS) (KW_LIKE showStmtIdentifier)? -> ^(TOK_SHOWDATABASES showStmtIdentifier?)
    | KW_SHOW (isExtended=KW_EXTENDED)? KW_TABLES ((KW_FROM|KW_IN) db_name=identifier)? (filter=showTablesFilterExpr)?
    -> ^(TOK_SHOWTABLES (TOK_FROM $db_name)? $filter? $isExtended?)
    | KW_SHOW KW_VIEWS ((KW_FROM|KW_IN) db_name=identifier)? (KW_LIKE showStmtIdentifier|showStmtIdentifier)?  -> ^(TOK_SHOWVIEWS (TOK_FROM $db_name)? showStmtIdentifier?)
    | KW_SHOW KW_MATERIALIZED KW_VIEWS ((KW_FROM|KW_IN) db_name=identifier)? (KW_LIKE showStmtIdentifier|showStmtIdentifier)?  -> ^(TOK_SHOWMATERIALIZEDVIEWS (TOK_FROM $db_name)? showStmtIdentifier?)
    | KW_SHOW KW_SORTED? KW_COLUMNS (KW_FROM|KW_IN) tableName ((KW_FROM|KW_IN) db_name=identifier)? (KW_LIKE showStmtIdentifier|showStmtIdentifier)?
    -> ^(TOK_SHOWCOLUMNS tableName (TOK_FROM $db_name)? showStmtIdentifier? KW_SORTED?)
    | KW_SHOW KW_FUNCTIONS (KW_LIKE showFunctionIdentifier)?  -> ^(TOK_SHOWFUNCTIONS KW_LIKE? showFunctionIdentifier?)
    | KW_SHOW KW_PARTITIONS tabName=tableName partitionSpec? whereClause? orderByClause? limitClause? -> ^(TOK_SHOWPARTITIONS $tabName partitionSpec? whereClause? orderByClause? limitClause?)
    | KW_SHOW KW_CREATE (
        (KW_DATABASE|KW_SCHEMA) => (KW_DATABASE|KW_SCHEMA) db_name=identifier -> ^(TOK_SHOW_CREATEDATABASE $db_name)
        |
        KW_TABLE tabName=tableName -> ^(TOK_SHOW_CREATETABLE $tabName)
      )
    | KW_SHOW KW_TABLE KW_EXTENDED ((KW_FROM|KW_IN) db_name=identifier)? KW_LIKE showStmtIdentifier partitionSpec?
    -> ^(TOK_SHOW_TABLESTATUS showStmtIdentifier $db_name? partitionSpec?)
    | KW_SHOW KW_TBLPROPERTIES tableName (LPAREN prptyName=StringLiteral RPAREN)? -> ^(TOK_SHOW_TBLPROPERTIES tableName $prptyName?)
    | KW_SHOW KW_LOCKS
      (
      (KW_DATABASE|KW_SCHEMA) => (KW_DATABASE|KW_SCHEMA) (dbName=identifier) (isExtended=KW_EXTENDED)? -> ^(TOK_SHOWDBLOCKS $dbName $isExtended?)
      |
      (parttype=partTypeExpr)? (isExtended=KW_EXTENDED)? -> ^(TOK_SHOWLOCKS $parttype? $isExtended?)
      )
    | KW_SHOW KW_COMPACTIONS
      (
      (KW_COMPACT_ID) => compactionId -> ^(TOK_SHOW_COMPACTIONS compactionId)
      |
      (KW_DATABASE|KW_SCHEMA) => (KW_DATABASE|KW_SCHEMA) (dbName=identifier) compactionPool? compactionType? compactionStatus? orderByClause? limitClause? -> ^(TOK_SHOW_COMPACTIONS $dbName compactionPool? compactionType? compactionStatus? orderByClause? limitClause?)
      |
      (parttype=partTypeExpr)? compactionPool? compactionType? compactionStatus? orderByClause? limitClause? -> ^(TOK_SHOW_COMPACTIONS $parttype? compactionPool? compactionType? compactionStatus? orderByClause? limitClause?)
      )
    | KW_SHOW KW_TRANSACTIONS -> ^(TOK_SHOW_TRANSACTIONS)
    | KW_SHOW KW_CONF StringLiteral -> ^(TOK_SHOWCONF StringLiteral)
    | KW_SHOW KW_RESOURCE
      (
        (KW_PLAN rp_name=identifier -> ^(TOK_SHOW_RP $rp_name))
        | (KW_PLANS -> ^(TOK_SHOW_RP))
      )
    | KW_SHOW (KW_DATACONNECTORS) -> ^(TOK_SHOWDATACONNECTORS)
    ;

showTablesFilterExpr
@init { pushMsg("show tables filter expr", state); }
@after { popMsg(state); }
    : KW_WHERE identifier EQUAL StringLiteral
    -> ^(TOK_TABLE_TYPE identifier StringLiteral)
    | KW_LIKE showStmtIdentifier|showStmtIdentifier
    -> showStmtIdentifier
    ;

lockStatement
@init { pushMsg("lock statement", state); }
@after { popMsg(state); }
    : KW_LOCK KW_TABLE tableName partitionSpec? lockMode -> ^(TOK_LOCKTABLE tableName lockMode partitionSpec?)
    ;

lockDatabase
@init { pushMsg("lock database statement", state); }
@after { popMsg(state); }
    : KW_LOCK (KW_DATABASE|KW_SCHEMA) (dbName=identifier) lockMode -> ^(TOK_LOCKDB $dbName lockMode)
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
    : KW_UNLOCK (KW_DATABASE|KW_SCHEMA) (dbName=identifier) -> ^(TOK_UNLOCKDB $dbName)
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
    : KW_REVOKE grantOptionFor? privilegeList privilegeObject? KW_FROM principalSpecification
    -> ^(TOK_REVOKE privilegeList principalSpecification privilegeObject? grantOptionFor?)
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
    : KW_REVOKE adminOptionFor? KW_ROLE? identifier (COMMA identifier)* KW_FROM principalSpecification
    -> ^(TOK_REVOKE_ROLE principalSpecification adminOptionFor? identifier+)
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
    -> ^(TOK_SHOW_CURRENT_ROLE)
    ;

setRole
@init {pushMsg("set role", state);}
@after {popMsg(state);}
    : KW_SET KW_ROLE
    (
    (KW_ALL) => (all=KW_ALL) -> ^(TOK_SET_ROLE Identifier[$all.text])
    |
    (KW_NONE) => (none=KW_NONE) -> ^(TOK_SET_ROLE Identifier[$none.text])
    |
    identifier -> ^(TOK_SET_ROLE identifier)
    )
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
    : (KW_ALL) => KW_ALL -> ^(TOK_RESOURCE_ALL)
    | privObjectCols -> ^(TOK_PRIV_OBJECT_COL privObjectCols)
    ;

privilegeObject
@init {pushMsg("privilege object", state);}
@after {popMsg(state);}
    : KW_ON privObject -> ^(TOK_PRIV_OBJECT privObject)
    ;

// database or table type. Type is optional, default type is table
privObject
    : (KW_DATABASE|KW_SCHEMA) identifier -> ^(TOK_DB_TYPE identifier)
    | KW_TABLE? tableName partitionSpec? -> ^(TOK_TABLE_TYPE tableName partitionSpec?)
    | KW_URI (path=StringLiteral) ->  ^(TOK_URI_TYPE $path)
    | KW_SERVER identifier -> ^(TOK_SERVER_TYPE identifier)
    ;

privObjectCols
    : (KW_DATABASE|KW_SCHEMA) identifier -> ^(TOK_DB_TYPE identifier)
    | KW_TABLE? tableName (LPAREN cols=columnNameList RPAREN)? partitionSpec? -> ^(TOK_TABLE_TYPE tableName $cols? partitionSpec?)
    | KW_URI (path=StringLiteral) ->  ^(TOK_URI_TYPE $path)
    | KW_SERVER identifier -> ^(TOK_SERVER_TYPE identifier)
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
    : KW_USER principalIdentifier -> ^(TOK_USER principalIdentifier)
    | KW_GROUP principalIdentifier -> ^(TOK_GROUP principalIdentifier)
    | KW_ROLE identifier -> ^(TOK_ROLE identifier)
    ;

withGrantOption
@init {pushMsg("with grant option", state);}
@after {popMsg(state);}
    : KW_WITH KW_GRANT KW_OPTION
    -> ^(TOK_GRANT_WITH_OPTION)
    ;

grantOptionFor
@init {pushMsg("grant option for", state);}
@after {popMsg(state);}
    : KW_GRANT KW_OPTION KW_FOR
    -> ^(TOK_GRANT_OPTION_FOR)
;

adminOptionFor
@init {pushMsg("admin option for", state);}
@after {popMsg(state);}
    : KW_ADMIN KW_OPTION KW_FOR
    -> ^(TOK_ADMIN_OPTION_FOR)
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
    : KW_MSCK (repair=KW_REPAIR)?
      (KW_TABLE tableName
        (opt=(KW_ADD|KW_DROP|KW_SYNC) (parts=KW_PARTITIONS) (partitionSelectorSpec)?)?)
    -> ^(TOK_MSCK $repair? tableName? $opt? (partitionSelectorSpec)?)
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

reloadFunctionsStatement
@init { pushMsg("reload functions statement", state); }
@after { popMsg(state); }
    : KW_RELOAD (KW_FUNCTIONS|KW_FUNCTION) -> ^(TOK_RELOADFUNCTIONS);

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
    | KW_PARTITIONED KW_ON KW_SPEC LPAREN (spec = partitionTransformSpec) RPAREN
    -> ^(TOK_TABLEPARTCOLSBYSPEC $spec)
    ;

viewOrganization
@init { pushMsg("view organization specification", state); }
@after { popMsg(state); }
    : viewClusterSpec
    | viewComplexSpec
    ;

viewClusterSpec
@init { pushMsg("view cluster specification", state); }
@after { popMsg(state); }
    : KW_CLUSTERED KW_ON LPAREN columnNameList RPAREN
    -> ^(TOK_VIEWCLUSTERCOLS columnNameList)
    ;

viewComplexSpec
@init { pushMsg("view complex specification", state); }
@after { popMsg(state); }
    : viewDistSpec viewSortSpec
    ;

viewDistSpec
@init { pushMsg("view distribute specification", state); }
@after { popMsg(state); }
    : KW_DISTRIBUTED KW_ON LPAREN colList=columnNameList RPAREN
    -> ^(TOK_VIEWDISTRIBUTECOLS $colList)
    ;

viewSortSpec
@init { pushMsg("view sort specification", state); }
@after { popMsg(state); }
    : KW_SORTED KW_ON LPAREN colList=columnNameList RPAREN
    -> ^(TOK_VIEWSORTCOLS $colList)
    ;

dropViewStatement
@init { pushMsg("drop view statement", state); }
@after { popMsg(state); }
    : KW_DROP KW_VIEW ifExists? viewName -> ^(TOK_DROPVIEW viewName ifExists?)
    ;

createMaterializedViewStatement
@init {
    pushMsg("create materialized view statement", state);
}
@after { popMsg(state); }
    : KW_CREATE KW_MATERIALIZED KW_VIEW (ifNotExists)? name=tableName
        rewriteDisabled? tableComment? viewPartition? viewOrganization?
        tableRowFormat? tableFileFormat? tableLocation?
        tablePropertiesPrefixed? KW_AS selectStatementWithCTE
    -> ^(TOK_CREATE_MATERIALIZED_VIEW $name
         ifNotExists?
         rewriteDisabled?
         tableComment?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         viewPartition?
         viewOrganization?
         tablePropertiesPrefixed?
         selectStatementWithCTE
        )
    ;

dropMaterializedViewStatement
@init { pushMsg("drop materialized view statement", state); }
@after { popMsg(state); }
    : KW_DROP KW_MATERIALIZED KW_VIEW ifExists? viewName -> ^(TOK_DROP_MATERIALIZED_VIEW viewName ifExists?)
    ;

createScheduledQueryStatement
@init { pushMsg("create scheduled query statement", state); }
@after { popMsg(state); }
    : KW_CREATE KW_SCHEDULED KW_QUERY name=identifier
        scheduleSpec
        executedAsSpec?
        enableSpecification?
        definedAsSpec
    -> ^(TOK_CREATE_SCHEDULED_QUERY
            $name
            scheduleSpec
            executedAsSpec?
            enableSpecification?
            definedAsSpec
        )
    ;

dropScheduledQueryStatement
@init { pushMsg("drop scheduled query statement", state); }
@after { popMsg(state); }
    : KW_DROP KW_SCHEDULED KW_QUERY name=identifier
    -> ^(TOK_DROP_SCHEDULED_QUERY
            $name
        )
    ;


alterScheduledQueryStatement
@init { pushMsg("alter scheduled query statement", state); }
@after { popMsg(state); }
    : KW_ALTER KW_SCHEDULED KW_QUERY name=identifier
            mod=alterScheduledQueryChange
    -> ^(TOK_ALTER_SCHEDULED_QUERY
            $name
            $mod
        )
    ;

alterScheduledQueryChange
@init { pushMsg("alter scheduled query change", state); }
@after { popMsg(state); }
    : scheduleSpec
    | executedAsSpec
    | enableSpecification
    | definedAsSpec
    | KW_EXECUTE -> ^(TOK_EXECUTE)
    ;

scheduleSpec
@init { pushMsg("schedule specification", state); }
@after { popMsg(state); }
        : KW_CRON cronString=StringLiteral -> ^(TOK_CRON $cronString)
        | KW_EVERY value=Number? qualifier=intervalQualifiers
        ((KW_AT|KW_OFFSET KW_BY) offsetTs=StringLiteral)? -> ^(TOK_SCHEDULE ^(TOK_EVERY $value?) $qualifier $offsetTs?)
        ;

executedAsSpec
@init { pushMsg("executedAs specification", state); }
@after { popMsg(state); }
        : KW_EXECUTED KW_AS executedAs=StringLiteral -> ^(TOK_EXECUTED_AS $executedAs)
        ;

definedAsSpec
@init { pushMsg("definedAs specification", state); }
@after { popMsg(state); }
        : KW_DEFINED? KW_AS statement -> ^(TOK_QUERY statement)
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

createTablePartitionSpec
@init { pushMsg("create table partition specification", state); }
@after { popMsg(state); }
    : KW_PARTITIONED KW_BY LPAREN (opt1 = createTablePartitionColumnTypeSpec | opt2 = createTablePartitionColumnSpec) RPAREN
    -> {$opt1.tree != null}? $opt1
    -> $opt2
    | KW_PARTITIONED KW_BY KW_SPEC LPAREN (spec = partitionTransformSpec) RPAREN
    -> ^(TOK_TABLEPARTCOLSBYSPEC $spec)
    ;

createTablePartitionColumnTypeSpec
@init { pushMsg("create table partition specification", state); }
@after { popMsg(state); }
    : columnNameTypeConstraint (COMMA columnNameTypeConstraint)*
    -> ^(TOK_TABLEPARTCOLS columnNameTypeConstraint+)
    ;

createTablePartitionColumnSpec
@init { pushMsg("create table partition specification", state); }
@after { popMsg(state); }
    : columnName (COMMA columnName)*
    -> ^(TOK_TABLEPARTCOLNAMES columnName+)
    ;

partitionTransformSpec
@init { pushMsg("create table partition by specification", state); }
@after { popMsg(state); }
    : columnNameTransformConstraint (COMMA columnNameTransformConstraint)*
    -> columnNameTransformConstraint+
    ;

columnNameTransformConstraint
@init { pushMsg("column transform specification", state); }
@after { popMsg(state); }
    : partitionTransformType
    -> ^(TOK_TABCOL partitionTransformType)
    ;

partitionTransformType
@init {pushMsg("partitition transform type specification", state); }
@after { popMsg(state); }
    : columnName
    -> {containExcludedCharForCreateTableColumnName($columnName.text)}? {throwColumnNameException()}
    ->  ^(TOK_IDENTITY columnName)
    | KW_YEAR LPAREN columnName RPAREN
    -> {containExcludedCharForCreateTableColumnName($columnName.text)}? {throwColumnNameException()}
    ->  ^(TOK_YEAR columnName)
    | KW_MONTH LPAREN columnName RPAREN
    -> {containExcludedCharForCreateTableColumnName($columnName.text)}? {throwColumnNameException()}
    ->  ^(TOK_MONTH columnName)
    | KW_DAY LPAREN columnName RPAREN
    -> {containExcludedCharForCreateTableColumnName($columnName.text)}? {throwColumnNameException()}
    ->  ^(TOK_DAY columnName)
    | KW_HOUR LPAREN columnName RPAREN
    -> {containExcludedCharForCreateTableColumnName($columnName.text)}? {throwColumnNameException()}
    ->  ^(TOK_HOUR columnName)
    | KW_TRUNCATE LPAREN value = Number COMMA columnName RPAREN
    -> {containExcludedCharForCreateTableColumnName($columnName.text)}? {throwColumnNameException()}
    ->  ^(TOK_TRUNCATE $value columnName)
    | KW_BUCKET LPAREN value = Number COMMA columnName RPAREN
    -> {containExcludedCharForCreateTableColumnName($columnName.text)}? {throwColumnNameException()}
    ->  ^(TOK_BUCKET $value columnName)
    ;

tableBuckets
@init { pushMsg("table buckets specification", state); }
@after { popMsg(state); }
    :
      KW_CLUSTERED KW_BY LPAREN bucketCols=columnNameList RPAREN (KW_SORTED KW_BY LPAREN sortCols=columnNameOrderList RPAREN)? KW_INTO num=Number KW_BUCKETS
    -> ^(TOK_ALTERTABLE_BUCKETS $bucketCols $sortCols? $num)
    ;

tableImplBuckets
@init { pushMsg("implicit table buckets specification", state); }
@after { popMsg(state); }
    :
      KW_CLUSTERED KW_INTO num=Number KW_BUCKETS
    -> ^(TOK_ALTERTABLE_BUCKETS $num)
    ;

tableSkewed
@init { pushMsg("table skewed specification", state); }
@after { popMsg(state); }
    :
     KW_SKEWED KW_BY LPAREN skewedCols=columnNameList RPAREN KW_ON LPAREN (skewedValues=skewedValueElement) RPAREN ((storedAsDirs) => storedAsDirs)?
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
      (KW_STORED KW_AS KW_INPUTFORMAT) => KW_STORED KW_AS KW_INPUTFORMAT inFmt=StringLiteral KW_OUTPUTFORMAT outFmt=StringLiteral (KW_INPUTDRIVER inDriver=StringLiteral KW_OUTPUTDRIVER outDriver=StringLiteral)?
      -> ^(TOK_TABLEFILEFORMAT $inFmt $outFmt $inDriver? $outDriver?)
      | KW_STORED KW_BY storageHandler=StringLiteral
         (KW_WITH KW_SERDEPROPERTIES serdeprops=tableProperties)?
         (KW_STORED KW_AS fileformat=identifier)?
      -> ^(TOK_STORAGEHANDLER $storageHandler $serdeprops? ^(TOK_FILEFORMAT_GENERIC $fileformat)?)
      | KW_STORED KW_BY genericSpec=identifier
         (KW_WITH KW_SERDEPROPERTIES serdeprops=tableProperties)?
         (KW_STORED KW_AS fileformat=identifier)?
      -> ^(TOK_STORAGEHANDLER $genericSpec $serdeprops? ^(TOK_FILEFORMAT_GENERIC $fileformat)?)
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

columnNameTypeOrConstraintList
@init { pushMsg("column name type and constraints list", state); }
@after { popMsg(state); }
    : columnNameTypeOrConstraint (COMMA columnNameTypeOrConstraint)* -> ^(TOK_TABCOLLIST columnNameTypeOrConstraint+)
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

extColumnName
@init { pushMsg("column name for complex types", state); }
@after { popMsg(state); }
    :
      identifier (DOT^ ((KW_ELEM_TYPE) => KW_ELEM_TYPE | (KW_KEY_TYPE) => KW_KEY_TYPE | (KW_VALUE_TYPE) => KW_VALUE_TYPE | identifier))*
    ;

columnNameOrderList
@init { pushMsg("column name order list", state); }
@after { popMsg(state); }
    : columnNameOrder (COMMA columnNameOrder)* -> ^(TOK_TABCOLNAME columnNameOrder+)
    ;

columnParenthesesList
@init { pushMsg("column parentheses list", state); }
@after { popMsg(state); }
    : LPAREN! columnNameList RPAREN!
    ;

enableValidateSpecification
@init { pushMsg("enable specification", state); }
@after { popMsg(state); }
    : enableSpecification validateSpecification?
    | enforcedSpecification
    ;

enableSpecification
@init { pushMsg("enable specification", state); }
@after { popMsg(state); }
    : KW_ENABLE -> ^(TOK_ENABLE)
    | KW_DISABLE -> ^(TOK_DISABLE)
    ;

validateSpecification
@init { pushMsg("validate specification", state); }
@after { popMsg(state); }
    : KW_VALIDATE -> ^(TOK_VALIDATE)
    | KW_NOVALIDATE -> ^(TOK_NOVALIDATE)
    ;

enforcedSpecification
@init { pushMsg("enforced specification", state); }
@after { popMsg(state); }
    : KW_ENFORCED -> ^(TOK_ENABLE)
    | KW_NOT KW_ENFORCED -> ^(TOK_DISABLE)
    ;

relySpecification
@init { pushMsg("rely specification", state); }
@after { popMsg(state); }
    :  KW_RELY -> ^(TOK_RELY)
    |  KW_NORELY -> ^(TOK_NORELY)
    ;

createConstraint
@init { pushMsg("pk or uk or nn constraint", state); }
@after { popMsg(state); }
    : (KW_CONSTRAINT constraintName=identifier)? tableLevelConstraint constraintOptsCreate?
    -> {$constraintName.tree != null}?
            ^({$tableLevelConstraint.tree} ^(TOK_CONSTRAINT_NAME $constraintName) constraintOptsCreate?)
    -> ^({$tableLevelConstraint.tree} constraintOptsCreate?)
    ;

alterConstraintWithName
@init { pushMsg("pk or uk or nn constraint with name", state); }
@after { popMsg(state); }
    : KW_CONSTRAINT constraintName=identifier tableLevelConstraint constraintOptsAlter?
    ->^({$tableLevelConstraint.tree} ^(TOK_CONSTRAINT_NAME $constraintName) constraintOptsAlter?)
    ;

tableLevelConstraint
    : pkUkConstraint
    | checkConstraint
    ;

pkUkConstraint
@init { pushMsg("pk or uk table level constraint", state); }
@after { popMsg(state); }
    : tableConstraintType pkCols=columnParenthesesList
    -> ^(tableConstraintType $pkCols)
    ;

checkConstraint
@init { pushMsg("CHECK constraint", state); }
@after { popMsg(state); }
    : KW_CHECK LPAREN expression RPAREN
    -> ^(TOK_CHECK_CONSTRAINT expression)
    ;

createForeignKey
@init { pushMsg("foreign key", state); }
@after { popMsg(state); }
    : (KW_CONSTRAINT constraintName=identifier)? KW_FOREIGN KW_KEY fkCols=columnParenthesesList  KW_REFERENCES tabName=tableName parCols=columnParenthesesList constraintOptsCreate?
    -> {$constraintName.tree != null}?
            ^(TOK_FOREIGN_KEY ^(TOK_CONSTRAINT_NAME $constraintName) $fkCols $tabName $parCols constraintOptsCreate?)
    -> ^(TOK_FOREIGN_KEY $fkCols $tabName $parCols constraintOptsCreate?)
    ;

alterForeignKeyWithName
@init { pushMsg("foreign key with key name", state); }
@after { popMsg(state); }
    : KW_CONSTRAINT constraintName=identifier KW_FOREIGN KW_KEY fkCols=columnParenthesesList  KW_REFERENCES tabName=tableName parCols=columnParenthesesList constraintOptsAlter?
    -> ^(TOK_FOREIGN_KEY ^(TOK_CONSTRAINT_NAME $constraintName) $fkCols $tabName $parCols constraintOptsAlter?)
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

orderSpecification
@init { pushMsg("order specification", state); }
@after { popMsg(state); }
    : KW_ASC | KW_DESC ;

nullOrdering
@init { pushMsg("nulls ordering", state); }
@after { popMsg(state); }
    : KW_NULLS KW_FIRST -> ^(TOK_NULLS_FIRST)
    | KW_NULLS KW_LAST -> ^(TOK_NULLS_LAST)
    ;

columnNameOrder
@init { pushMsg("column name order", state); }
@after { popMsg(state); }
    : identifier orderSpec=orderSpecification? nullSpec=nullOrdering?
    -> {$orderSpec.tree == null && $nullSpec.tree == null}?
            ^(TOK_TABSORTCOLNAMEASC ^(TOK_NULLS_FIRST identifier))
    -> {$orderSpec.tree == null}?
            ^(TOK_TABSORTCOLNAMEASC ^($nullSpec identifier))
    -> {$nullSpec.tree == null && $orderSpec.tree.getType()==HiveParser.KW_ASC}?
            ^(TOK_TABSORTCOLNAMEASC ^(TOK_NULLS_FIRST identifier))
    -> {$nullSpec.tree == null && $orderSpec.tree.getType()==HiveParser.KW_DESC}?
            ^(TOK_TABSORTCOLNAMEDESC ^(TOK_NULLS_LAST identifier))
    -> {$orderSpec.tree.getType()==HiveParser.KW_ASC}?
            ^(TOK_TABSORTCOLNAMEASC ^($nullSpec identifier))
    -> ^(TOK_TABSORTCOLNAMEDESC ^($nullSpec identifier))
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

orderSpecificationRewrite
@init { pushMsg("order specification", state); }
@after { popMsg(state); }
    : KW_ASC -> ^(TOK_TABSORTCOLNAMEASC)
    | KW_DESC -> ^(TOK_TABSORTCOLNAMEDESC)
    ;

columnRefOrder
@init { pushMsg("column order", state); }
@after { popMsg(state); }
    : expression orderSpec=orderSpecificationRewrite? nullSpec=nullOrdering?
    // ORDER not present, NULLS ORDER not present and default is NULLS LAST ex.: ORDER BY col0
    -> {$orderSpec.tree == null && $nullSpec.tree == null && nullsLast()}?
            ^(TOK_TABSORTCOLNAMEASC ^(TOK_NULLS_LAST expression))
    // ORDER not present, NULLS ORDER not present and default is NULLS FIRST ex.: ORDER BY col0
    -> {$orderSpec.tree == null && $nullSpec.tree == null}?
            ^(TOK_TABSORTCOLNAMEASC ^(TOK_NULLS_FIRST expression))
    // ORDER not present but NULLS ORDER present ex.: ORDER BY col0 NULLS FIRST
    -> {$orderSpec.tree == null}?
            ^(TOK_TABSORTCOLNAMEASC ^($nullSpec expression))
    // ASC ORDER present but NULLS ORDER not present and default is NULLS LAST ex.: ORDER BY col0 ASC
    -> {$nullSpec.tree == null && $orderSpec.tree.getType() == HiveParser.TOK_TABSORTCOLNAMEASC && nullsLast()}?
            ^($orderSpec ^(TOK_NULLS_LAST expression))
    // DESC ORDER present but NULLS ORDER not present and default is NULLS LAST ex.: ORDER BY col0 DESC
    -> {$nullSpec.tree == null && $orderSpec.tree.getType() == HiveParser.TOK_TABSORTCOLNAMEDESC && nullsLast()}?
            ^($orderSpec ^(TOK_NULLS_FIRST expression))
    // ASC ORDER present, NULLS ORDER not present and default is NULLS FIRST ex.: ORDER BY col0 ASC
    -> {$nullSpec.tree == null && $orderSpec.tree.getType() == HiveParser.TOK_TABSORTCOLNAMEASC}?
            ^($orderSpec ^(TOK_NULLS_FIRST expression))
    // DESC ORDER present, NULLS ORDER not present and default is NULLS FIRST ex.: ORDER BY col0 DESC
    -> {$nullSpec.tree == null && $orderSpec.tree.getType() == HiveParser.TOK_TABSORTCOLNAMEDESC}?
            ^($orderSpec ^(TOK_NULLS_LAST expression))
    // both ORDER and NULLS ORDER present ex.: ORDER BY col0 ASC NULLS LAST
    -> ^($orderSpec ^($nullSpec expression))
    ;

columnNameType
@init { pushMsg("column specification", state); }
@after { popMsg(state); }
    : colName=identifier colType (KW_COMMENT comment=StringLiteral)?
    -> {containExcludedCharForCreateTableColumnName($colName.text)}? {throwColumnNameException()}
    -> {$comment == null}? ^(TOK_TABCOL $colName colType)
    ->                     ^(TOK_TABCOL $colName colType $comment)
    ;

columnNameTypeOrConstraint
@init { pushMsg("column name or constraint", state); }
@after { popMsg(state); }
    : ( tableConstraint )
    | ( columnNameTypeConstraint )
    ;

tableConstraint
@init { pushMsg("table constraint", state); }
@after { popMsg(state); }
    : ( createForeignKey )
    | ( createConstraint )
    ;

columnNameTypeConstraint
@init { pushMsg("column specification", state); }
@after { popMsg(state); }
    : colName=identifier colType columnConstraint[$colName.tree]? (KW_COMMENT comment=StringLiteral)?
    -> {containExcludedCharForCreateTableColumnName($colName.text)}? {throwColumnNameException()}
    -> ^(TOK_TABCOL $colName colType $comment? columnConstraint?)
    ;

columnConstraint[CommonTree fkColName]
@init { pushMsg("column constraint", state); }
@after { popMsg(state); }
    : ( foreignKeyConstraint[$fkColName] )
    | ( colConstraint )
    ;

foreignKeyConstraint[CommonTree fkColName]
@init { pushMsg("column constraint", state); }
@after { popMsg(state); }
    : (KW_CONSTRAINT constraintName=identifier)? KW_REFERENCES tabName=tableName LPAREN colName=columnName RPAREN constraintOptsCreate?
    -> {$constraintName.tree != null}?
            ^(TOK_FOREIGN_KEY ^(TOK_CONSTRAINT_NAME $constraintName) ^(TOK_TABCOLNAME {$fkColName}) $tabName ^(TOK_TABCOLNAME $colName) constraintOptsCreate?)
    -> ^(TOK_FOREIGN_KEY ^(TOK_TABCOLNAME {$fkColName}) $tabName ^(TOK_TABCOLNAME $colName) constraintOptsCreate?)
    ;

colConstraint
@init { pushMsg("column constraint", state); }
@after { popMsg(state); }
    : (KW_CONSTRAINT constraintName=identifier)? columnConstraintType constraintOptsCreate?
    -> {$constraintName.tree != null}?
            ^({$columnConstraintType.tree} ^(TOK_CONSTRAINT_NAME $constraintName) constraintOptsCreate?)
    -> ^({$columnConstraintType.tree} constraintOptsCreate?)
    ;

alterColumnConstraint[CommonTree fkColName]
@init { pushMsg("alter column constraint", state); }
@after { popMsg(state); }
    : ( alterForeignKeyConstraint[$fkColName] )
    | ( alterColConstraint )
    ;

alterForeignKeyConstraint[CommonTree fkColName]
@init { pushMsg("alter column constraint", state); }
@after { popMsg(state); }
    : (KW_CONSTRAINT constraintName=identifier)? KW_REFERENCES tabName=tableName LPAREN colName=columnName RPAREN constraintOptsAlter?
    -> {$constraintName.tree != null}?
            ^(TOK_FOREIGN_KEY ^(TOK_CONSTRAINT_NAME $constraintName) ^(TOK_TABCOLNAME {$fkColName}) $tabName ^(TOK_TABCOLNAME $colName) constraintOptsAlter?)
    -> ^(TOK_FOREIGN_KEY ^(TOK_TABCOLNAME {$fkColName}) $tabName ^(TOK_TABCOLNAME $colName) constraintOptsAlter?)
    ;

alterColConstraint
@init { pushMsg("alter column constraint", state); }
@after { popMsg(state); }
    : (KW_CONSTRAINT constraintName=identifier)? columnConstraintType constraintOptsAlter?
    -> {$constraintName.tree != null}?
            ^({$columnConstraintType.tree} ^(TOK_CONSTRAINT_NAME $constraintName) constraintOptsAlter?)
    -> ^({$columnConstraintType.tree} constraintOptsAlter?)
    ;

columnConstraintType
    : KW_NOT KW_NULL       ->    TOK_NOT_NULL
    | KW_DEFAULT defaultVal->    ^(TOK_DEFAULT_VALUE defaultVal)
    | checkConstraint
    | tableConstraintType
    ;

defaultVal
    : constant
    | function
    | castExpression
    ;

tableConstraintType
    : KW_PRIMARY KW_KEY    ->    TOK_PRIMARY_KEY
    | KW_UNIQUE            ->    TOK_UNIQUE
    ;

constraintOptsCreate
    : enableValidateSpecification relySpecification?
    ;

constraintOptsAlter
    : enableValidateSpecification relySpecification?
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
    | KW_REAL         ->     TOK_FLOAT
    | KW_DOUBLE KW_PRECISION?       ->    TOK_DOUBLE
    | KW_DATE          ->    TOK_DATE
    | KW_DATETIME      ->    TOK_DATETIME
    | KW_TIMESTAMP     ->    TOK_TIMESTAMP
    | KW_TIMESTAMPLOCALTZ   ->    TOK_TIMESTAMPLOCALTZ
    //| KW_TIMESTAMPTZ   ->    TOK_TIMESTAMPTZ
    | KW_TIMESTAMP KW_WITH KW_LOCAL KW_TIME KW_ZONE -> TOK_TIMESTAMPLOCALTZ
    //| KW_TIMESTAMP KW_WITH KW_TIME KW_ZONE -> TOK_TIMESTAMPTZ
    // Uncomment to allow intervals as table column types
    //| KW_INTERVAL KW_YEAR KW_TO KW_MONTH -> TOK_INTERVAL_YEAR_MONTH
    //| KW_INTERVAL KW_DAY KW_TO KW_SECOND -> TOK_INTERVAL_DAY_TIME
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
    : KW_UNION KW_ALL -> ^(TOK_UNIONALL)
    | KW_UNION KW_DISTINCT? -> ^(TOK_UNIONDISTINCT)
    | KW_INTERSECT KW_ALL -> ^(TOK_INTERSECTALL)
    | KW_INTERSECT KW_DISTINCT? -> ^(TOK_INTERSECTDISTINCT)
    | KW_EXCEPT KW_ALL -> ^(TOK_EXCEPTALL)
    | KW_EXCEPT KW_DISTINCT? -> ^(TOK_EXCEPTDISTINCT)
    | KW_MINUS KW_ALL -> ^(TOK_EXCEPTALL)
    | KW_MINUS KW_DISTINCT? -> ^(TOK_EXCEPTDISTINCT)
    ;

queryStatementExpression
    :
    /* Would be nice to do this as a gated semantic perdicate
       But the predicate gets pushed as a lookahead decision.
       Calling rule doesnot know about topLevel
    */
    (w=withClause)?
    queryStatementExpressionBody {
      if ($w.tree != null) {
      $queryStatementExpressionBody.tree.insertChild(0, $w.tree);
      }
    }
    ->  queryStatementExpressionBody
    ;

queryStatementExpressionBody
    :
    fromStatement
    | regularBody
    ;

withClause
  :
  KW_WITH cteStatement (COMMA cteStatement)* -> ^(TOK_CTE cteStatement+)
;

cteStatement
   :
   identifier (LPAREN colAliases=columnNameList RPAREN)? KW_AS LPAREN queryStatementExpression RPAREN
   -> ^(TOK_SUBQUERY queryStatementExpression identifier $colAliases?)
;

fromStatement
: (singleFromStatement  -> singleFromStatement)
	(u=setOperator r=singleFromStatement
	  -> ^($u {$fromStatement.tree} $r)
	)*
	 -> {u != null}? ^(TOK_QUERY
	       ^(TOK_FROM
	         ^(TOK_SUBQUERY
	           {$fromStatement.tree}
	            {adaptor.create(Identifier, generateUnionAlias())}
	           )
	        )
            ^(TOK_INSERT
	          ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
	          ^(TOK_SELECT ^(TOK_SELEXPR TOK_SETCOLREF))
	        )
	      )
    -> {$fromStatement.tree}
	;


singleFromStatement
    :
    fromClause
    ( b+=body )+ -> ^(TOK_QUERY fromClause body+)
    ;

/*
The valuesClause rule below ensures that the parse tree for
"insert into table FOO values (1,2),(3,4)" looks the same as
"insert into table FOO select a,b from (values(1,2),(3,4)) as BAR(a,b)" which itself is made to look
very similar to the tree for "insert into table FOO select a,b from BAR".
*/
regularBody
   :
   i=insertClause
   (
   s=selectStatement {$s.tree.getFirstChildWithType(TOK_INSERT).replaceChildren(0, 0, $i.tree);}
   -> {$s.tree}
   )
   |
   selectStatement
   ;

atomSelectStatement
   :
   s=selectClause
   f=fromClause?
   w=whereClause?
   g=groupByClause?
   h=havingClause?
   win=window_clause?
   q=qualifyClause?
   -> ^(TOK_QUERY $f? ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
                     $s $w? $g? $h? $win? $q?))
   |
   LPAREN! selectStatement RPAREN!
   |
   valuesSource
   ;

selectStatement
   :
   a=atomSelectStatement
   set=setOpSelectStatement[$atomSelectStatement.tree]?
   o=orderByClause?
   c=clusterByClause?
   d=distributeByClause?
   sort=sortByClause?
   l=limitClause?
   {
   if(set == null){
   $a.tree.getFirstChildWithType(TOK_INSERT).addChild($o.tree);
   $a.tree.getFirstChildWithType(TOK_INSERT).addChild($c.tree);
   $a.tree.getFirstChildWithType(TOK_INSERT).addChild($d.tree);
   $a.tree.getFirstChildWithType(TOK_INSERT).addChild($sort.tree);
   $a.tree.getFirstChildWithType(TOK_INSERT).addChild($l.tree);
   }
   }
   -> {set == null}?
      {$a.tree}
   -> {o==null && c==null && d==null && sort==null && l==null}?
      {$set.tree}
   -> ^(TOK_QUERY
          ^(TOK_FROM
            ^(TOK_SUBQUERY
              {$set.tree}
              {adaptor.create(Identifier, generateUnionAlias())}
             )
          )
          ^(TOK_INSERT
             ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
             ^(TOK_SELECT ^(TOK_SELEXPR TOK_SETCOLREF))
             $o? $c? $d? $sort? $l?
          )
      )
   ;

setOpSelectStatement[CommonTree t]
   :
   (u=setOperator b=atomSelectStatement
   -> {$setOpSelectStatement.tree != null && ((CommonTree)u.getTree()).getType()==HiveParser.TOK_UNIONDISTINCT}?
      ^(TOK_QUERY
          ^(TOK_FROM
            ^(TOK_SUBQUERY
              ^(TOK_UNIONALL {$setOpSelectStatement.tree} $b)
              {adaptor.create(Identifier, generateUnionAlias())}
             )
          )
          ^(TOK_INSERT
             ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
             ^(TOK_SELECTDI ^(TOK_SELEXPR TOK_SETCOLREF))
          )
       )
   -> {$setOpSelectStatement.tree != null && ((CommonTree)u.getTree()).getType()!=HiveParser.TOK_UNIONDISTINCT}?
      ^($u {$setOpSelectStatement.tree} $b)
   -> {$setOpSelectStatement.tree == null && ((CommonTree)u.getTree()).getType()==HiveParser.TOK_UNIONDISTINCT}?
      ^(TOK_QUERY
          ^(TOK_FROM
            ^(TOK_SUBQUERY
              ^(TOK_UNIONALL {$t} $b)
              {adaptor.create(Identifier, generateUnionAlias())}
             )
           )
          ^(TOK_INSERT
            ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
            ^(TOK_SELECTDI ^(TOK_SELEXPR TOK_SETCOLREF))
         )
       )
   -> ^($u {$t} $b)
   )+
   -> {$setOpSelectStatement.tree.getChild(0).getType()==HiveParser.TOK_UNIONALL
   ||$setOpSelectStatement.tree.getChild(0).getType()==HiveParser.TOK_INTERSECTDISTINCT
   ||$setOpSelectStatement.tree.getChild(0).getType()==HiveParser.TOK_INTERSECTALL
   ||$setOpSelectStatement.tree.getChild(0).getType()==HiveParser.TOK_EXCEPTDISTINCT
   ||$setOpSelectStatement.tree.getChild(0).getType()==HiveParser.TOK_EXCEPTALL}?
      ^(TOK_QUERY
          ^(TOK_FROM
            ^(TOK_SUBQUERY
              {$setOpSelectStatement.tree}
              {adaptor.create(Identifier, generateUnionAlias())}
             )
          )
          ^(TOK_INSERT
             ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
             ^(TOK_SELECT ^(TOK_SELEXPR TOK_SETCOLREF))
          )
       )
   -> {$setOpSelectStatement.tree}
   ;

selectStatementWithCTE
    :
    (w=withClause)?
    selectStatement {
      if ($w.tree != null) {
      $selectStatement.tree.insertChild(0, $w.tree);
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
   window_clause?
   qualifyClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   limitClause? -> ^(TOK_INSERT insertClause
                     selectClause lateralView? whereClause? groupByClause? havingClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? window_clause? qualifyClause? limitClause?)
   |
   selectClause
   lateralView?
   whereClause?
   groupByClause?
   havingClause?
   window_clause?
   qualifyClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   limitClause? -> ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
                     selectClause lateralView? whereClause? groupByClause? havingClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? window_clause? qualifyClause? limitClause?)
   ;

insertClause
@init { pushMsg("insert clause", state); }
@after { popMsg(state); }
   :
     KW_INSERT KW_OVERWRITE destination ifNotExists? -> ^(TOK_DESTINATION destination ifNotExists?)
   | KW_INSERT KW_INTO KW_TABLE? tableOrPartition (LPAREN targetCols=columnNameList RPAREN)?
       -> ^(TOK_INSERT_INTO tableOrPartition $targetCols?)
   ;

destination
@init { pushMsg("destination specification", state); }
@after { popMsg(state); }
   :
     (local = KW_LOCAL)? KW_DIRECTORY StringLiteral tableRowFormat? tableFileFormat?
       -> ^(TOK_DIR StringLiteral $local? tableRowFormat? tableFileFormat?)
   | KW_TABLE tableOrPartition -> tableOrPartition
   ;

limitClause
@init { pushMsg("limit clause", state); }
@after { popMsg(state); }
   :
   KW_LIMIT ((offset=Number COMMA)? num=Number) -> ^(TOK_LIMIT ($offset)? $num)
   | KW_LIMIT num=Number KW_OFFSET offset=Number -> ^(TOK_LIMIT ($offset)? $num)
   ;

//DELETE FROM <tableName> WHERE ...;
deleteStatement
@init { pushMsg("delete statement", state); }
@after { popMsg(state); }
   :
   KW_DELETE KW_FROM tableName (whereClause)? -> ^(TOK_DELETE_FROM tableName whereClause?)
   ;

/*SET <columName> = (3 + col2)*/
columnAssignmentClause
   :
   | tableOrColumn EQUAL^ precedencePlusExpressionOrDefault
   ;

precedencePlusExpressionOrDefault
    :
    (KW_DEFAULT (~DOT|EOF)) => defaultValue
    | precedencePlusExpression
    ;


/*SET col1 = 5, col2 = (4 + col4), ...*/
setColumnsClause
   :
   KW_SET columnAssignmentClause (COMMA columnAssignmentClause)* -> ^(TOK_SET_COLUMNS_CLAUSE columnAssignmentClause* )
   ;

/*
  UPDATE <table>
  SET col1 = val1, col2 = val2... WHERE ...
*/
updateStatement
@init { pushMsg("update statement", state); }
@after { popMsg(state); }
   :
   KW_UPDATE tableName setColumnsClause whereClause? -> ^(TOK_UPDATE_TABLE tableName setColumnsClause whereClause?)
   ;

/*
BEGIN user defined transaction boundaries; follows SQL 2003 standard exactly except for addition of
"setAutoCommitStatement" which is not in the standard doc but is supported by most SQL engines.
*/
sqlTransactionStatement
@init { pushMsg("transaction statement", state); }
@after { popMsg(state); }
  :
  startTransactionStatement
	|	commitStatement
	|	rollbackStatement
	| setAutoCommitStatement
	;

startTransactionStatement
  :
  KW_START KW_TRANSACTION ( transactionMode  ( COMMA transactionMode  )* )? -> ^(TOK_START_TRANSACTION transactionMode*)
  ;

transactionMode
  :
  isolationLevel
  | transactionAccessMode -> ^(TOK_TXN_ACCESS_MODE transactionAccessMode)
  ;

transactionAccessMode
  :
  KW_READ KW_ONLY -> TOK_TXN_READ_ONLY
  | KW_READ KW_WRITE -> TOK_TXN_READ_WRITE
  ;

isolationLevel
  :
  KW_ISOLATION KW_LEVEL levelOfIsolation -> ^(TOK_ISOLATION_LEVEL levelOfIsolation)
  ;

/*READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SERIALIZABLE may be supported later*/
levelOfIsolation
  :
  KW_SNAPSHOT -> TOK_ISOLATION_SNAPSHOT
  ;

commitStatement
  :
  KW_COMMIT ( KW_WORK )? -> TOK_COMMIT
  ;

rollbackStatement
  :
  KW_ROLLBACK ( KW_WORK )? -> TOK_ROLLBACK
  ;
setAutoCommitStatement
  :
  KW_SET KW_AUTOCOMMIT booleanValueTok -> ^(TOK_SET_AUTOCOMMIT booleanValueTok)
  ;
/*
END user defined transaction boundaries
*/

abortTransactionStatement
@init { pushMsg("abort transactions statement", state); }
@after { popMsg(state); }
  :
  KW_ABORT KW_TRANSACTIONS ( Number )+ -> ^(TOK_ABORT_TRANSACTIONS ( Number )+)
  ;

abortCompactionStatement
@init { pushMsg("abort compactions statement", state); }
@after { popMsg(state); }
  :

       KW_ABORT KW_COMPACTIONS ( Number )+ -> ^(TOK_ABORT_COMPACTIONS ( Number )+)
  ;
/*
BEGIN SQL Merge statement
*/
mergeStatement
@init { pushMsg("MERGE statement", state); }
@after { popMsg(state); }
   :
   KW_MERGE QUERY_HINT? KW_INTO tableName (KW_AS? identifier)? KW_USING joinSourcePart KW_ON expression whenClauses
     -> ^(TOK_MERGE ^(TOK_TABREF tableName identifier?) joinSourcePart expression QUERY_HINT? whenClauses)
   ;
/*
Allow 0,1 or 2 WHEN MATCHED clauses and 0 or 1 WHEN NOT MATCHED
Each WHEN clause may have AND <boolean predicate>.
If 2 WHEN MATCHED clauses are present, 1 must be UPDATE the other DELETE and the 1st one
must have AND <boolean predicate>
*/
whenClauses
   :
   (whenMatchedAndClause|whenMatchedThenClause)* whenNotMatchedClause?
   ;
whenNotMatchedClause
@init { pushMsg("WHEN NOT MATCHED clause", state); }
@after { popMsg(state); }
   :
  KW_WHEN KW_NOT KW_MATCHED (KW_AND expression)? KW_THEN KW_INSERT (targetCols=columnParenthesesList)? KW_VALUES valueRowConstructor ->
    ^(TOK_NOT_MATCHED ^(TOK_INSERT $targetCols? valueRowConstructor) expression?)
  ;
whenMatchedAndClause
@init { pushMsg("WHEN MATCHED AND clause", state); }
@after { popMsg(state); }
  :
  KW_WHEN KW_MATCHED KW_AND expression KW_THEN updateOrDelete ->
    ^(TOK_MATCHED updateOrDelete expression)
  ;
whenMatchedThenClause
@init { pushMsg("WHEN MATCHED THEN clause", state); }
@after { popMsg(state); }
  :
  KW_WHEN KW_MATCHED KW_THEN updateOrDelete ->
     ^(TOK_MATCHED updateOrDelete)
  ;
updateOrDelete
   :
   KW_UPDATE setColumnsClause -> ^(TOK_UPDATE setColumnsClause)
   |
   KW_DELETE -> TOK_DELETE
   ;
/*
END SQL Merge statement
*/

killQueryStatement
@init { pushMsg("kill query statement", state); }
@after { popMsg(state); }
  :
  KW_KILL KW_QUERY ( StringLiteral )+ -> ^(TOK_KILL_QUERY ( StringLiteral )+)
  ;

/*
BEGIN SHOW COMPACTIONS statement
*/
compactionId
  : KW_COMPACT_ID EQUAL compactId=Number -> ^(TOK_COMPACT_ID $compactId)
  ;
compactionPool
  : KW_POOL poolName=StringLiteral -> ^(TOK_COMPACT_POOL $poolName)
  ;
compactionType
  : KW_TYPE compactType=StringLiteral -> ^(TOK_COMPACTION_TYPE $compactType)
  ;
compactionStatus
  : KW_STATUS status=StringLiteral -> ^(TOK_COMPACTION_STATUS $status)
  ;
/*
END SHOW COMPACTIONS statement
*/