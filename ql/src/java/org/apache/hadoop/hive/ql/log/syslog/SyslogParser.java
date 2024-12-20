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
package org.apache.hadoop.hive.ql.log.syslog;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.hive.common.type.Timestamp;

/**
 * A Syslog protocol parser.
 * It should be capable of parsing RFC 3164 (BSD syslog) streams as well as RFC 5424 (defined in 2009.).
 * Adapted from https://github.com/spotify/flume-syslog-source2 and modified it for several assumptions
 * about the way hive logs using syslog format (specifically RFC5424).
 *
 * This implementation also parses structured data, returns all parsed fields as map and also un-escapes messages.
 * This parser also gracefully handles some corner cases where 'msg' can be empty or line can start with '&lt;' but not
 * a valid RFC5424 format etc.
 *
 * Assumption:
 * 1) This parser assumes the linebreaks '\n' in stack traces for example are replaced by '\r' to make single
 * line message. The reader will do replacement of '\r' with '\n' at the time of read.
 * 2) This parser assumes structured data values are html escaped. So it will html unescape when parsing structured
 * data. (hive writes log lines directly to stderr that look like rfc5424 layout starting with '&lt;' so the expectation
 * from log4j2 is to escape those lines using html escaping).
 * 3) Read event returns List&lt;Object&gt; conforming to sys.logs table schema in hive. The schema for sys.logs table is
 * expected to be (facility STRING, severity STRING, version STRING, ts TIMESTAMP, hostname STRING, app_name STRING,
 * proc_id STRING, msg_id STRING, structured_data map&lt;STRING,STRING&gt;, msg BINARY, unmatched BINARY)
 * 4) Timestamps are in UTC
 *
 * This parser is tested with Log4j2's RFC5424 layout generated using the following properties
 * appenders = console
 * appender.console.layout.type = Rfc5424Layout
 * appender.console.layout.appName = ${env:APP_NAME}
 * appender.console.layout.facility = USER
 * appender.console.layout.includeMDC = true
 * appender.console.layout.mdcId = mdc
 * appender.console.layout.messageId = ${env:MSG_ID}
 * appender.console.layout.newLine = true
 * appender.console.layout.newLineEscape = \\r
 * appender.console.layout.exceptionPattern = %ex{full}
 * appender.console.layout.loggerfields.type = LoggerFields
 * appender.console.layout.loggerfields.pairs1.type = KeyValuePair
 * appender.console.layout.loggerfields.pairs1.key = level
 * appender.console.layout.loggerfields.pairs1.value = %p
 * appender.console.layout.loggerfields.pairs2.type = KeyValuePair
 * appender.console.layout.loggerfields.pairs2.key = thread
 * appender.console.layout.loggerfields.pairs2.value = %enc{%t}
 * appender.console.layout.loggerfields.pairs3.type = KeyValuePair
 * appender.console.layout.loggerfields.pairs3.key = class
 * appender.console.layout.loggerfields.pairs3.value = %c{2}
 */
public class SyslogParser implements Closeable {
  // RFC 5424 section 6.

  // SYSLOG-MSG  format
  // PRI VERSION SP TIMESTAMP SP HOSTNAME SP APP-NAME SP PROCID SP MSGID SP STRUCTURED-DATA [SP MSG]

  // facility + severity forms PRI
  // Version 0 is the RFC 3164 format, 1 for RFC 5424 format.

  // Read event returns the following schema
  // facility STRING
  // severity STRING
  // version STRING
  // ts TIMESTAMP
  // hostname STRING
  // app_name STRING
  // proc_id STRING
  // msg_id STRING
  // structured_data map<STRING,STRING>
  // msg BINARY
  // unmatched BINARY
  private final static int EXPECTED_COLUMNS = 11;

  private final static String[] FACILITIES = new String[]{"KERN", "USER", "MAIL", "DAEMON", "AUTH", "SYSLOG", "LPR", "NEWS",
    "UUCP", "CRON", "AUTHPRIV", "FTP", "NTP", "AUDIT", "ALERT", "CLOCK", "LOCAL0", "LOCAL1", "LOCAL2", "LOCAL3",
    "LOCAL4", "LOCAL5", "LOCAL6", "LOCAL7"};

  // As defined in RFC 5424.
  private final static int MAX_SUPPORTED_VERSION = 1;

  private InputStream in;
  private boolean parseTag;
  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
  private static final Charset UTF8 = StandardCharsets.UTF_8;
  private Charset charset;

  /// Push back buffer. -1 indicates that it is empty.
  private int pushBack = -1;


  public SyslogParser() {
    this(null);
  }

  /**
   * Construct a new Syslog protocol parser.
   * Tags are parsed, and the encoding is assumed to be UTF-8.
   */
  public SyslogParser(InputStream in) {
    this(in, true, UTF8);
  }

  /**
   * Construct a new Syslog protocol parser.
   *
   * @param in       the stream to read data from. The InputStream#read()
   *                 function is heavily used, so make sure it is buffered.
   * @param parseTag true to parse the "tag[pid]:", false to leave it as
   *                 part of the message body.
   * @param encoding the encoding to use for various string conversions,
   *                 most notably the hostname.
   */
  public SyslogParser(InputStream in, boolean parseTag, Charset encoding) {
    this.in = in;
    this.parseTag = parseTag;
    this.charset = encoding;
  }

  /**
   * Free the resources used by this parser.
   * Note that the parser cannot be reused. Closes the underlying input
   * stream.
   */
  public void close() throws IOException {
    in.close();
  }

  /**
   * Read the next Syslog message from the stream.
   *
   * @return a parsed map of object, or null on EOF.
   * @throws IOException if the underlying stream fails, or unexpected
   * bytes are seen.
   */
  public List<Object> readEvent() throws IOException {
    int priority;
    int c = read(false);
    List<Object> row = new ArrayList<>(EXPECTED_COLUMNS);
    // Return null on initial EOF.
    if (c == -1) {
      return null;
    }

    if (c == '<') {
      priority = readInt();
      if (priority == -1) {
        return unmatchedEvent(c);
      }
      expect('>');
    } else {
      return unmatchedEvent(c);
    }

    int version = 0;
    Calendar cal;

    if (Character.isDigit(peek())) {
      // Assume ISO date and time
      int y = readInt();

      c = read(true);

      if (c == ' ') {
        // Assume this is a RFC 5424 message.
        version = y;

        if (version > MAX_SUPPORTED_VERSION) {
          throw new IOException("Unsupported syslog version: " + version);
        }

        skipSpaces();
        y = readInt();
        expect('-');
      } else if (c != '-') {
        throw new IOException("Unexpected syslog character: " + (char) c);
      }

      int m = readInt();
      expect('-');
      int d = readInt();

      c = read(true);

      if (c != 'T' && c != ' ') {
        throw new IOException("Unexpected syslog character: " + (char) c);
      }

      int hh = readInt();
      expect(':');
      int mm = readInt();
      expect(':');
      int ss = readInt();
      double subss = 0;

      c = read(true);

      if (c == '.') {
        // Fractions of seconds
        subss = readFractions();
        c = read(true);
      }

      int tz = 0;

      if (c == 'Z') {
        // UTC
      } else if (c == '-') {
        tz = readInt();

        if (peek() == ':') {
          read(true);
          tz = -(tz * 60 + readInt());
        }
      } else if (c == '+') {
        tz = readInt();

        if (peek() == ':') {
          read(true);
          tz = tz * 60 + readInt();
        }
      }

      cal = new GregorianCalendar(UTC, Locale.getDefault());

      cal.set(y, m - 1, d, hh, mm, ss);
      cal.set(Calendar.MILLISECOND, (int) (subss * 1000));
      cal.add(Calendar.MINUTE, tz);
    } else {
      // Assume BSD date and time
      int m = readMonthAbbreviation();

      expect(' ');
      skipSpaces();

      int d = readInt();

      expect(' ');
      skipSpaces();

      int hh = readInt();

      expect(':');

      int mm = readInt();

      expect(':');

      int ss = readInt();

      cal = new GregorianCalendar(Locale.ROOT);

      cal.set(Calendar.MONTH, m);
      cal.set(Calendar.DAY_OF_MONTH, d);
      cal.set(Calendar.HOUR_OF_DAY, hh);
      cal.set(Calendar.MINUTE, mm);
      cal.set(Calendar.SECOND, ss);
    }

    expect(' ');
    skipSpaces();

    String hostname = readWordString();

    expect(' ');

    byte[] appName = null;
    byte[] procId = null;
    byte[] msgId = null;
    Map<String, String> structuredData = null;

    if (version >= 1) {
      appName = readWordOrNil(48);
      expect(' ');
      procId = readWordOrNil(12);
      expect(' ');
      msgId = readWordOrNil(32);
      expect(' ');
      structuredData = readAndParseStructuredData();
    } else if (version == 0 && parseTag) {
      // Try to find a colon terminated tag.
      appName = readTag();
      if (peek() == '[') {
        procId = readPid();
      }
      expect(':');
    }

    c = skipSpaces();

    byte[] msg = null;
    if (c != -1) {
      msg = readLine();
    }
    createEvent(version, priority, cal, hostname, appName, procId, msgId, structuredData, msg, row);
    return row;
  }

  private List<Object> unmatchedEvent(int c) throws IOException {
    List<Object> row = new ArrayList<>(EXPECTED_COLUMNS);
    byte[] msg = readLine();
    for (int i = 0; i < SyslogParser.EXPECTED_COLUMNS; i++) {
      row.add(null);
    }
    row.set(10, ((char) c + new String(msg)).getBytes(charset));
    return row;
  }

  /**
   * Create a log event from the given parameters.
   * https://www.rfc-editor.org/rfc/rfc3164
   * https://www.rfc-editor.org/rfc/rfc5424
   *
   * @param version        the syslog version, 0 for RFC 3164
   * @param priority       the syslog priority, according to RFC 5424
   * @param cal            the timestamp of the message. Note that timezone matters
   * @param hostname       the hostname
   * @param appName        the RFC 5424 app-name
   * @param procId         the RFC 5424 proc-id
   * @param msgId          the RFC 5424 msg-id
   * @param structuredData the RFC 5424 structured-data
   * @param body           the message body
   */
  private void createEvent(int version, int priority, Calendar cal, String hostname,
    byte[] appName, byte[] procId, byte[] msgId, Map<String, String> structuredData, byte[] body, List<Object> row) {
    row.add(FACILITIES[priority / 8]);
    row.add(getEventPriorityBySyslog(priority));
    row.add(version == 0 ? "RFC3164" : "RFC5424");
    row.add(Timestamp.ofEpochMilli(cal.getTimeInMillis()));
    row.add(hostname != null ? hostname : "");
    row.add(appName != null ? new String(appName) : "");
    row.add(procId != null ? new String(procId) : "");
    row.add(msgId != null ? new String(msgId) : "");
    row.add(structuredData);
    row.add(body);
  }

  /**
   * Resolve the given syslog priority as a log priority.
   */
  private String getEventPriorityBySyslog(int priority) {
    switch (priority % 8) {
      case 0:
      case 1:
      case 2:
        return "FATAL";
      case 3:
        return "ERROR";
      case 4:
        return "WARN";
      case 5:
      case 6:
        return "INFO";
      case 7:
        return "DEBUG";
      default:
        // If this happens, we should tell the world.
        throw new RuntimeException("Failed to look up Syslog priority");
    }
  }

  /**
   * Read a month value as an English abbreviation.
   * see RFC 3164, Sec. 4.1.2.
   */
  private int readMonthAbbreviation() throws IOException {
    switch (read(true)) {
      case 'A':
        switch (read(true)) {
          case 'p':
            skipWord();
            return Calendar.APRIL;

          case 'u':
            skipWord();
            return Calendar.AUGUST;

          default:
            return -1;
        }

      case 'D':
        skipWord();
        return Calendar.DECEMBER;

      case 'F':
        skipWord();
        return Calendar.FEBRUARY;

      case 'J':
        read(true); // Second letter is ambiguous.
        read(true); // Third letter is also ambiguous.

        switch (read(true)) {
          case 'e':
            skipWord();
            return Calendar.JUNE;

          case 'u':
            skipWord();
            return Calendar.JANUARY;

          case 'y':
            skipWord();
            return Calendar.JULY;

          default:
            return -1;
        }

      case 'M':
        read(true); // Second letter is ambiguous.

        switch (read(true)) {
          case 'r':
            skipWord();
            return Calendar.MARCH;

          case 'y':
            skipWord();
            return Calendar.MAY;

          default:
            return -1;
        }

      case 'N':
        skipWord();
        return Calendar.NOVEMBER;

      case 'O':
        skipWord();
        return Calendar.OCTOBER;

      case 'S':
        skipWord();
        return Calendar.SEPTEMBER;

      default:
        return -1;
    }
  }

  /**
   * Read a byte and assert the value.
   *
   * @throw IOException if the character was unexpected
   */
  private void expect(int c) throws IOException {
    int d = read(true);

    if (d != c) {
      throw new IOException("Unexpected syslog character: " + (char) d);
    }
  }

  /**
   * Read until a non-whitespace ASCII byte is seen.
   */
  private int skipSpaces() throws IOException {
    int c;

    while ((c = read(false)) == ' ') {
      continue;
    }

    if (c != -1) {
      unread(c);
    }
    return c;
  }

  /**
   * Read the next byte, but then unread it again.
   */
  private int peek() throws IOException {
    int c = read(true);

    unread(c);

    return c;
  }

  /**
   * Read the next byte.
   *
   * @param checkEof true to throw EOFException on EOF, false to return -1.
   * @return the byte, or -1 on EOF.
   */
  private int read(boolean checkEof) throws IOException {
    if (pushBack != -1) {
      int c = pushBack;
      pushBack = -1;
      return c;
    }

    int c = in.read();

    if (checkEof && c == -1) {
      throw new EOFException("Unexpected end of syslog stream");
    }

    return c;
  }

  /**
   * Push back a character.
   * Only a single character can be pushed back simultaneously.
   */
  private void unread(int c) {
    assert c != -1 : "Trying to push back EOF";
    assert pushBack == -1 : "Trying to push back two bytes";
    pushBack = c;
  }

  /**
   * Read a positive integer and convert it from decimal text form.
   * EOF silently terminates the number.
   */
  private int readInt() throws IOException {
    int c;
    int ret = 0;

    boolean foundDigit = false;
    while (Character.isDigit(c = read(false))) {
      foundDigit = true;
      ret = ret * 10 + (c - '0');
    }

    if (!foundDigit) {
      unread(c);
      return -1;
    }
    if (c != -1) {
      unread(c);
    }

    return ret;
  }

  /**
   * Read fractions (digits after a decimal point.)
   *
   * @return a value in the range [0, 1).
   */
  private double readFractions() throws IOException {
    int c;
    int ret = 0;
    int order = 1;

    while (Character.isDigit(c = read(false))) {
      ret = ret * 10 + (c - '0');
      order *= 10;
    }

    if (c != -1) {
      unread(c);
    }

    return (double) ret / order;
  }

  /**
   * Read until EOF or a space.
   * The input is discarded.
   */
  private void skipWord() throws IOException {
    int c;

    do {
      c = read(false);
    } while (c != ' ' && c != -1);

    if (c != -1) {
      unread(c);
    }
  }

  /**
   * Read a word into the given output stream.
   * Usually the output stream will be a ByteArrayOutputStream.
   */
  private void readWord(OutputStream out) throws IOException {
    int c;

    while ((c = read(false)) != ' ' && c != -1) {
      out.write(c);
    }

    if (c != -1) {
      unread(c);
    }
  }

  /**
   * Read a word (until next ASCII space or EOF) as a string.
   * The encoding chosen while constructing the parser is used for decoding.
   *
   * @return a valid, but perhaps empty, word.
   */
  private String readWordString() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream(256);
    readWord(out);

    return out.toString(charset.name());
  }

  /**
   * Read a word (until next ASCII space or EOF) as a byte array.
   *
   * @param sizeHint an guess on how large string will be, in bytes.
   * @return a valid, but perhaps empty, word.
   */
  private byte[] readWord(int sizeHint) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream(sizeHint);
    readWord(out);

    return out.toByteArray();
  }

  /**
   * Read a word (until next space or EOF) as a byte array.
   * If the complete word is "-", return null.
   *
   * @param sizeHint an guess on how large string will be, in bytes.
   */
  private byte[] readWordOrNil(int sizeHint) throws IOException {
    byte[] ret = readWord(sizeHint);

    if (ret.length == 1 && ret[0] == '-') {
      return null;
    }

    return ret;
  }

  /**
   * Read a line (until next ASCII NL or EOF) as a byte array.
   */
  private byte[] readLine() throws IOException {
    ByteArrayOutputStream ret = new ByteArrayOutputStream(1024);
    int c;

    while ((c = read(false)) != '\n' && c != -1) {
      if (c != '\r') {
        ret.write(c);
      } else {
        // CR replaced with LF
        ret.write('\n');
      }
    }

    return ret.toByteArray();
  }

  /**
   * Read a RFC 3164 tag.
   * Tags end with left bracket, colon, ASCII CR, or ASCII NL.
   */
  private byte[] readTag() throws IOException {
    ByteArrayOutputStream ret = new ByteArrayOutputStream(16);
    int c;

    while ((c = read(true)) != ':' && c != '[' && c != '\r' && c != '\n') {
      ret.write(c);
    }

    unread(c);

    return ret.toByteArray();
  }

  /**
   * Read a RFC 3164 pid.
   * The format is "[1234]".
   */
  private byte[] readPid() throws IOException {
    ByteArrayOutputStream ret = new ByteArrayOutputStream(8);
    int c;

    expect('[');

    while ((c = read(true)) != ']' && c != '\r' && c != '\n') {
      ret.write(c);
    }

    return ret.toByteArray();
  }

  /**
   * Read RFC 5424 structured data and parse it into map.
   */
  private Map<String, String> readAndParseStructuredData() throws IOException {
    int c = read(true);

    if (c == '-') {
      return null;
    }

    ByteArrayOutputStream ret = new ByteArrayOutputStream(128);

    if (c != '[') {
      throw new IOException("Unexpected syslog character: " + (char) c);
    }

    Map<String, String> structuredData = new HashMap<>();
    while (c == '[') {

      // Read SD-ID
      while ((c = read(true)) != ' ' && c != ']') {
        ret.write(c);
      }

      String sdId = new String(ret.toByteArray());
      structuredData.put("sdId", sdId);
      ret.reset();

      while (c == ' ') {
        // Read PARAM-NAME
        while ((c = read(true)) != '=') {
          ret.write(c);
        }

        String sdKey = new String(ret.toByteArray());
        ret.reset();

        expect('"');

        // Read PARAM-DATA
        while ((c = read(true)) != '"') {
          ret.write(c);

          if (c == '\\') {
            c = read(true);
            ret.write(c);
          }
        }

        String sdValue = new String(ret.toByteArray());
        ret.reset();
        structuredData.put(sdKey, StringEscapeUtils.unescapeHtml4(sdValue));

        c = read(true);
      }

      if (c != ']') {
        throw new IOException("Unexpected syslog character: " + (char) c);
      }

      c = read(false);
    }

    if (c != -1) {
      unread(c);
    }

    return structuredData;
  }

  public void setInputStream(final InputStream in) {
    this.in = in;
  }
}
