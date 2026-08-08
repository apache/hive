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
package org.apache.hive.beeline;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;

import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

public class HiveSqlHighlighter implements Highlighter {

  static final AttributedStyle KEYWORD_STYLE =
      AttributedStyle.DEFAULT.foreground(AttributedStyle.CYAN).bold();
  static final AttributedStyle TYPE_STYLE =
      AttributedStyle.DEFAULT.foreground(AttributedStyle.BLUE).bold();
  static final AttributedStyle CONSTANT_STYLE =
      AttributedStyle.DEFAULT.foreground(AttributedStyle.MAGENTA).bold();
  static final AttributedStyle FUNCTION_STYLE =
      AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW);
  static final AttributedStyle STRING_STYLE =
      AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN);
  static final AttributedStyle NUMBER_STYLE =
      AttributedStyle.DEFAULT.foreground(AttributedStyle.MAGENTA);
  static final AttributedStyle COMMENT_STYLE =
      AttributedStyle.DEFAULT.faint();
  static final AttributedStyle TABLE_STYLE =
      AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW);
  static final AttributedStyle COLUMN_STYLE =
      AttributedStyle.DEFAULT.foreground(AttributedStyle.WHITE);
  static final AttributedStyle DEFAULT_STYLE = AttributedStyle.DEFAULT;

  // Keywords after which an identifier is (most likely) a table/relation name.
  // Used by the positional table-vs-column heuristic.
  static final Set<String> TABLE_CONTEXT = immutableUpper(
      "FROM", "JOIN", "INTO", "UPDATE", "TABLE", "DESCRIBE", "TRUNCATE");

  // ---- Hive data types (matched before the generic keyword set) ------------
  static final Set<String> TYPES = immutableUpper(
      "TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE",
      "DECIMAL", "NUMERIC", "DEC", "REAL", "PRECISION", "BOOLEAN", "STRING",
      "CHAR", "VARCHAR", "BINARY", "DATE", "DATETIME", "TIMESTAMP",
      "TIMESTAMPLOCALTZ", "INTERVAL", "ARRAY", "MAP", "STRUCT", "UNIONTYPE",
      "VARIANT", "LONG");

  static final Set<String> CONSTANTS = immutableUpper(
      "TRUE", "FALSE", "NULL", "UNKNOWN");

  private static final String KEYWORD_LIST =
      "ABORT,ACTIVATE,ACTIVE,ADD,ADMIN,AFTER,ALL,ALLOC_FRACTION,ALTER,ANALYZE,"
      + "AND,ANY,APPLICATION,ARCHIVE,AS,ASC,AST,AT,AUTHORIZATION,AUTOCOMMIT,"
      + "BEFORE,BETWEEN,BOTH,BRANCH,BUCKET,BUCKETS,BY,CACHE,CASCADE,CASE,CAST,"
      + "CATALOG,CATALOGS,CBO,CHANGE,CHECK,CLUSTER,CLUSTERED,CLUSTERSTATUS,"
      + "COLLECTION,COLUMN,COLUMNS,COMMENT,COMMIT,COMPACT,COMPACTIONID,"
      + "COMPACTIONS,COMPUTE,CONCATENATE,CONF,CONNECTOR,CONNECTORS,CONSTRAINT,"
      + "CONTINUE,CONVERT,COST,CREATE,CRON,CROSS,CUBE,CURRENT,CURRENT_DATE,"
      + "CURRENT_TIMESTAMP,CURSOR,DATA,DATABASE,DATABASES,DAY,DAYOFWEEK,DAYS,"
      + "DBPROPERTIES,DCPROPERTIES,DDL,DEBUG,DEFAULT,DEFERRED,DEFINED,DELETE,"
      + "DELIMITED,DEPENDENCY,DESC,DESCRIBE,DETAIL,DIRECTORIES,DIRECTORY,"
      + "DISABLE,DISABLED,DISTINCT,DISTRIBUTE,DISTRIBUTED,DO,DROP,DUMP,ELSE,"
      + "ENABLE,ENABLED,END,ENFORCED,ESCAPED,EVERY,EXCEPT,EXCHANGE,EXCLUSIVE,"
      + "EXECUTE,EXECUTED,EXISTS,EXPIRE_SNAPSHOTS,EXPLAIN,EXPORT,EXPRESSION,"
      + "EXTENDED,EXTERNAL,EXTRACT,FETCH,FIELDS,FILE,FILEFORMAT,FIRST,FLOOR,"
      + "FOLLOWING,FOR,FORCE,FOREIGN,FORMAT,FORMATTED,FROM,FULL,FUNCTION,"
      + "FUNCTIONS,GRANT,GROUP,GROUPING,HAVING,HOUR,HOURS,IDXPROPERTIES,IF,"
      + "IGNORE,IMPORT,IN,INDEX,INDEXES,INNER,INPATH,INPUTDRIVER,INPUTFORMAT,"
      + "INSERT,INTERSECT,INTO,IS,ISOLATION,ITEMS,JAR,JOIN,JOINCOST,KEY,KEYS,"
      + "KILL,LAST,LATERAL,LEADING,LEFT,LESS,LEVEL,LIKE,LIMIT,LINES,LOAD,LOCAL,"
      + "LOCALLY,LOCATION,LOCK,LOCKS,LOGICAL,MACRO,MANAGED,MANAGEDLOCATION,"
      + "MANAGEMENT,MAPJOIN,MAPPING,MATCHED,MATERIALIZED,MERGE,METADATA,MINUS,"
      + "MINUTE,MINUTES,MONTH,MONTHS,MORE,MOVE,MSCK,NONE,NORELY,NOSCAN,NOT,"
      + "NOVALIDATE,NULLS,OF,OFFSET,OLDER,ON,ONLY,OPERATOR,OPTIMIZE,OPTION,OR,"
      + "ORDER,ORDERED,OUT,OUTER,OUTPUTDRIVER,OUTPUTFORMAT,OVER,OVERWRITE,OWNER,"
      + "PARTITION,PARTITIONED,PARTITIONS,PATH,PERCENT,PKFK_JOIN,PLAN,PLANS,"
      + "PLUS,POOL,PRECEDING,PREPARE,PRESERVE,PRIMARY,PRINCIPALS,PROCEDURE,"
      + "PROPERTIES,PURGE,QUALIFY,QUARTER,QUERY,QUERY_PARALLELISM,RANGE,READ,"
      + "READS,REBUILD,RECORDREADER,RECORDWRITER,REDUCE,REFERENCES,REGEXP,"
      + "RELOAD,RELY,REMOTE,RENAME,REOPTIMIZATION,REPAIR,REPL,REPLACE,"
      + "REPLICATION,RESOURCE,RESPECT,RESTRICT,RETAIN,RETENTION,REVOKE,REWRITE,"
      + "RIGHT,RLIKE,ROLE,ROLES,ROLLBACK,ROLLUP,ROW,ROWS,SCHEDULED,"
      + "SCHEDULING_POLICY,SCHEMA,SCHEMAS,SECOND,SECONDS,SELECT,SEMI,SERDE,"
      + "SERDEPROPERTIES,SERVER,SET,SETS,SET_CURRENT_SNAPSHOT,SHARED,SHOW,"
      + "SHOW_DATABASE,SKEWED,SNAPSHOT,SNAPSHOTS,SOME,SORT,SORTED,SPEC,SSL,"
      + "START,STATISTICS,STATUS,STORED,STREAMTABLE,SUMMARY,SYNC,SYSTEM_TIME,"
      + "SYSTEM_VERSION,TABLE,TABLES,TABLESAMPLE,TAG,TBLPROPERTIES,TEMPORARY,"
      + "TERMINATED,THAN,THEN,TIME,TO,TOUCH,TRAILING,TRANSACTION,TRANSACTIONAL,"
      + "TRANSACTIONS,TRANSFORM,TRIGGER,TRIM,TRUNCATE,TYPE,UNARCHIVE,UNBOUNDED,"
      + "UNDO,UNION,UNIQUE,UNIQUEJOIN,UNLOCK,UNMANAGED,UNSET,UNSIGNED,UPDATE,"
      + "URI,URL,USE,USER,USING,UTC,UTC_TMESTAMP,VALIDATE,VALUES,VECTORIZATION,"
      + "VIEW,VIEWS,WAIT,WEEK,WEEKS,WHEN,WHERE,WHILE,WINDOW,WITH,WITHIN,WORK,"
      + "WORKLOAD,WRITE,YEAR,YEARS,ZONE,ZORDER";

  static final Set<String> KEYWORDS;
  static {
    Set<String> kw = new HashSet<>();
    for (String k : KEYWORD_LIST.split(",")) {
      String t = k.trim().toUpperCase();
      if (!t.isEmpty() && !TYPES.contains(t) && !CONSTANTS.contains(t)) {
        kw.add(t);
      }
    }
    KEYWORDS = Collections.unmodifiableSet(kw);
  }

  private final BooleanSupplier enabled;

  public HiveSqlHighlighter(BooleanSupplier enabled) {
    this.enabled = enabled;
  }

  @Override
  public AttributedString highlight(LineReader reader, String buffer) {
    if (buffer == null) {
      return new AttributedString("");
    }
    if (enabled != null && !enabled.getAsBoolean()) {
      return new AttributedString(buffer);
    }
    return highlight(buffer);
  }

  AttributedString highlight(String buffer) {
    AttributedStringBuilder sb = new AttributedStringBuilder();
    Context ctx = new Context();
    int n = buffer.length();
    int i = 0;
    while (i < n) {
      i = scanToken(sb, buffer, i, ctx);
    }
    return sb.toAttributedString();
  }

  /** Carry-over state between tokens: the last structural keyword seen. */
  private static final class Context {
    private String prevKw = "";
  }

  /** Append the single token starting at {@code i}; returns the index just past it. */
  private int scanToken(AttributedStringBuilder sb, String buf, int i, Context ctx) {
    char c = buf.charAt(i);
    if (isLineCommentAt(buf, i)) {
      int end = lineCommentEnd(buf, i);
      sb.append(buf.substring(i, end), COMMENT_STYLE);
      return end;
    }
    if (isBlockCommentAt(buf, i)) {
      int end = blockCommentEnd(buf, i);
      sb.append(buf.substring(i, end), COMMENT_STYLE);
      return end;
    }
    if (c == '\'' || c == '"') {
      int end = scanString(buf, i, c);
      sb.append(buf.substring(i, end), STRING_STYLE);
      return end;
    }
    if (c == '`') {
      int end = scanQuotedIdentifier(buf, i);
      sb.append(buf.substring(i, end), DEFAULT_STYLE);
      return end;
    }
    if (isNumberStartAt(buf, i)) {
      int end = scanNumber(buf, i);
      sb.append(buf.substring(i, end), NUMBER_STYLE);
      return end;
    }
    if (isIdentStart(c)) {
      return scanWord(sb, buf, i, ctx);
    }
    sb.append(c);
    if (c == '(' || c == ';') {
      ctx.prevKw = "";
    }
    return i + 1;
  }

  /** Append an identifier/keyword token, update table-context, and return the end index. */
  private int scanWord(AttributedStringBuilder sb, String buf, int i, Context ctx) {
    int n = buf.length();
    int end = i + 1;
    while (end < n && isIdentPart(buf.charAt(end))) {
      end++;
    }
    String word = buf.substring(i, end);
    String upper = word.toUpperCase();
    sb.append(word, styleForWord(upper, buf, end, ctx.prevKw));
    if (KEYWORDS.contains(upper)) {
      ctx.prevKw = upper;
    }
    return end;
  }

  private AttributedStyle styleForWord(String upper, String buffer, int wordEnd, String prevKw) {
    if (CONSTANTS.contains(upper)) {
      return CONSTANT_STYLE;
    }
    if (TYPES.contains(upper)) {
      return TYPE_STYLE;
    }
    if (KEYWORDS.contains(upper)) {
      return KEYWORD_STYLE;
    }
    // Plain identifier: classify as table, function, or column.
    // A table name follows FROM/JOIN/INTO/UPDATE/TABLE/... (takes precedence so
    // that e.g. CREATE TABLE t (...) colors t as a table, not a function).
    if (TABLE_CONTEXT.contains(prevKw)) {
      return TABLE_STYLE;
    }
    int j = wordEnd;
    while (j < buffer.length() && Character.isWhitespace(buffer.charAt(j))) {
      j++;
    }
    if (j < buffer.length()) {
      char next = buffer.charAt(j);
      if (next == '(') {
        return FUNCTION_STYLE;   // identifier immediately before '(' is a call
      }
      if (next == '.') {
        return TABLE_STYLE;      // qualifier in alias.col / db.tbl
      }
    }
    return COLUMN_STYLE;
  }

  private static int scanString(String s, int start, char quote) {
    int n = s.length();
    int i = start + 1;
    while (i < n) {
      char c = s.charAt(i);
      if (c == '\\') {
        i += 2;
        continue;
      }
      if (c == quote) {
        return i + 1;
      }
      i++;
    }
    return n;
  }

  private static int scanNumber(String s, int start) {
    int n = s.length();
    int i = start;
    boolean seenExp = false;
    while (i < n) {
      char c = s.charAt(i);
      if (isDigit(c) || c == '.') {
        i++;
      } else if ((c == 'e' || c == 'E') && !seenExp) {
        seenExp = true;
        i++;
        if (i < n && (s.charAt(i) == '+' || s.charAt(i) == '-')) {
          i++;
        }
      } else {
        break;
      }
    }
    return i;
  }

  private static boolean isLineCommentAt(String s, int i) {
    return s.charAt(i) == '-' && i + 1 < s.length() && s.charAt(i + 1) == '-';
  }

  private static boolean isBlockCommentAt(String s, int i) {
    return s.charAt(i) == '/' && i + 1 < s.length() && s.charAt(i + 1) == '*';
  }

  private static boolean isNumberStartAt(String s, int i) {
    char c = s.charAt(i);
    return isDigit(c) || (c == '.' && i + 1 < s.length() && isDigit(s.charAt(i + 1)));
  }

  private static int lineCommentEnd(String s, int i) {
    int end = s.indexOf('\n', i);
    return end < 0 ? s.length() : end;
  }

  private static int blockCommentEnd(String s, int i) {
    int end = s.indexOf("*/", i + 2);
    return end < 0 ? s.length() : end + 2;
  }

  private static int scanQuotedIdentifier(String s, int start) {
    int n = s.length();
    int end = start + 1;
    while (end < n && s.charAt(end) != '`') {
      end++;
    }
    return Math.min(end + 1, n);
  }

  private static boolean isDigit(char c) {
    return c >= '0' && c <= '9';
  }

  private static boolean isIdentStart(char c) {
    return Character.isLetter(c) || c == '_';
  }

  private static boolean isIdentPart(char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }

  private static Set<String> immutableUpper(String... words) {
    Set<String> s = new HashSet<>();
    for (String w : words) {
      s.add(w.toUpperCase());
    }
    return Collections.unmodifiableSet(s);
  }

  @Override
  public void setErrorPattern(Pattern errorPattern) {
    // No-op: this highlighter colorizes tokens itself and does not use JLine's
    // parser-error highlighting hooks.
  }

  @Override
  public void setErrorIndex(int errorIndex) {
    // No-op: see setErrorPattern.
  }
}
