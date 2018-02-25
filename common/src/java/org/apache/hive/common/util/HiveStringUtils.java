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

package org.apache.hive.common.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import com.google.common.base.Splitter;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.text.translate.CharSequenceTranslator;
import org.apache.commons.lang3.text.translate.EntityArrays;
import org.apache.commons.lang3.text.translate.LookupTranslator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.io.Text;

/**
 * HiveStringUtils
 * General string utils
 *
 * Originally copied from o.a.hadoop.util.StringUtils
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HiveStringUtils {

  /**
   * Priority of the StringUtils shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 0;

  private static final DecimalFormat decimalFormat;

  private static final CharSequenceTranslator ESCAPE_JAVA =
      new LookupTranslator(
        new String[][] {
          {"\"", "\\\""},
          {"\\", "\\\\"},
      }).with(
        new LookupTranslator(EntityArrays.JAVA_CTRL_CHARS_ESCAPE()));

  private static final CharSequenceTranslator ESCAPE_HIVE_COMMAND =
      new LookupTranslator(
        new String[][] {
          {"'", "\\'"},
          {";", "\\;"},
          {"\\", "\\\\"},
      }).with(
        new LookupTranslator(EntityArrays.JAVA_CTRL_CHARS_ESCAPE()));

  static {
    NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.ENGLISH);
    decimalFormat = (DecimalFormat) numberFormat;
    decimalFormat.applyPattern("#.##");
}

  /**
   * Return the internalized string, or null if the given string is null.
   * @param str The string to intern
   * @return The identical string cached in the string pool.
   */
  public static String intern(String str) {
    if(str == null) {
      return null;
    }
    return str.intern();
  }

  /**
   * Return an interned list with identical contents as the given list.
   * @param list The list whose strings will be interned
   * @return An identical list with its strings interned.
   */
  public static List<String> intern(List<String> list) {
    if(list == null) {
      return null;
    }
    List<String> newList = new ArrayList<String>(list.size());
    for(String str : list) {
      newList.add(intern(str));
    }
    return newList;
  }

  /**
   * Return an interned map with identical contents as the given map.
   * @param map The map whose strings will be interned
   * @return An identical map with its strings interned.
   */
  public static Map<String, String> intern(Map<String, String> map) {
    if(map == null) {
      return null;
    }

    if (map.isEmpty()) {
      // nothing to intern
      return map;
    }
    Map<String, String> newMap = new HashMap<String, String>(map.size());
    for(Map.Entry<String, String> entry : map.entrySet()) {
      newMap.put(intern(entry.getKey()), intern(entry.getValue()));
    }
    return newMap;
  }

  /**
   * Make a string representation of the exception.
   * @param e The exception to stringify
   * @return A string with exception name and call stack.
   */
  public static String stringifyException(Throwable e) {
    StringWriter stm = new StringWriter();
    PrintWriter wrt = new PrintWriter(stm);
    e.printStackTrace(wrt);
    wrt.close();
    return stm.toString();
  }

  /**
   * Given a full hostname, return the word upto the first dot.
   * @param fullHostname the full hostname
   * @return the hostname to the first dot
   */
  public static String simpleHostname(String fullHostname) {
    int offset = fullHostname.indexOf('.');
    if (offset != -1) {
      return fullHostname.substring(0, offset);
    }
    return fullHostname;
  }

  private static DecimalFormat oneDecimal = new DecimalFormat("0.0");

  /**
   * Given an integer, return a string that is in an approximate, but human
   * readable format.
   * It uses the bases 'k', 'm', and 'g' for 1024, 1024**2, and 1024**3.
   * @param number the number to format
   * @return a human readable form of the integer
   */
  public static String humanReadableInt(long number) {
    long absNumber = Math.abs(number);
    double result = number;
    String suffix = "";
    if (absNumber < 1024) {
      // since no division has occurred, don't format with a decimal point
      return String.valueOf(number);
    } else if (absNumber < 1024 * 1024) {
      result = number / 1024.0;
      suffix = "k";
    } else if (absNumber < 1024 * 1024 * 1024) {
      result = number / (1024.0 * 1024);
      suffix = "m";
    } else {
      result = number / (1024.0 * 1024 * 1024);
      suffix = "g";
    }
    return oneDecimal.format(result) + suffix;
  }

  /**
   * Format a percentage for presentation to the user.
   * @param done the percentage to format (0.0 to 1.0)
   * @param digits the number of digits past the decimal point
   * @return a string representation of the percentage
   */
  public static String formatPercent(double done, int digits) {
    DecimalFormat percentFormat = new DecimalFormat("0.00%");
    double scale = Math.pow(10.0, digits+2);
    double rounded = Math.floor(done * scale);
    percentFormat.setDecimalSeparatorAlwaysShown(false);
    percentFormat.setMinimumFractionDigits(digits);
    percentFormat.setMaximumFractionDigits(digits);
    return percentFormat.format(rounded / scale);
  }

  /**
   * Given an array of strings, return a comma-separated list of its elements.
   * @param strs Array of strings
   * @return Empty string if strs.length is 0, comma separated list of strings
   * otherwise
   */

  public static String arrayToString(String[] strs) {
    if (strs.length == 0) { return ""; }
    StringBuilder sbuf = new StringBuilder();
    sbuf.append(strs[0]);
    for (int idx = 1; idx < strs.length; idx++) {
      sbuf.append(",");
      sbuf.append(strs[idx]);
    }
    return sbuf.toString();
  }

  /**
   * Given an array of bytes it will convert the bytes to a hex string
   * representation of the bytes
   * @param bytes
   * @param start start index, inclusively
   * @param end end index, exclusively
   * @return hex string representation of the byte array
   */
  public static String byteToHexString(byte[] bytes, int start, int end) {
    if (bytes == null) {
      throw new IllegalArgumentException("bytes == null");
    }
    StringBuilder s = new StringBuilder();
    for(int i = start; i < end; i++) {
      s.append(String.format("%02x", bytes[i]));
    }
    return s.toString();
  }

  /** Same as byteToHexString(bytes, 0, bytes.length). */
  public static String byteToHexString(byte bytes[]) {
    return byteToHexString(bytes, 0, bytes.length);
  }

  /**
   * Given a hexstring this will return the byte array corresponding to the
   * string
   * @param hex the hex String array
   * @return a byte array that is a hex string representation of the given
   *         string. The size of the byte array is therefore hex.length/2
   */
  public static byte[] hexStringToByte(String hex) {
    byte[] bts = new byte[hex.length() / 2];
    for (int i = 0; i < bts.length; i++) {
      bts[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
    }
    return bts;
  }
  /**
   *
   * @param uris
   */
  public static String uriToString(URI[] uris){
    if (uris == null) {
      return null;
    }
    StringBuilder ret = new StringBuilder(uris[0].toString());
    for(int i = 1; i < uris.length;i++){
      ret.append(",");
      ret.append(uris[i].toString());
    }
    return ret.toString();
  }

  /**
   * @param str
   *          The string array to be parsed into an URI array.
   * @return <tt>null</tt> if str is <tt>null</tt>, else the URI array
   *         equivalent to str.
   * @throws IllegalArgumentException
   *           If any string in str violates RFC&nbsp;2396.
   */
  public static URI[] stringToURI(String[] str){
    if (str == null) {
      return null;
    }
    URI[] uris = new URI[str.length];
    for (int i = 0; i < str.length;i++){
      try{
        uris[i] = new URI(str[i]);
      }catch(URISyntaxException ur){
        throw new IllegalArgumentException(
            "Failed to create uri for " + str[i], ur);
      }
    }
    return uris;
  }

  /**
   *
   * @param str
   */
  public static Path[] stringToPath(String[] str){
    if (str == null) {
      return null;
    }
    Path[] p = new Path[str.length];
    for (int i = 0; i < str.length;i++){
      p[i] = new Path(str[i]);
    }
    return p;
  }
  /**
   *
   * Given a finish and start time in long milliseconds, returns a
   * String in the format Xhrs, Ymins, Z sec, for the time difference between two times.
   * If finish time comes before start time then negative valeus of X, Y and Z wil return.
   *
   * @param finishTime finish time
   * @param startTime start time
   */
  public static String formatTimeDiff(long finishTime, long startTime){
    long timeDiff = finishTime - startTime;
    return formatTime(timeDiff);
  }

  /**
   *
   * Given the time in long milliseconds, returns a
   * String in the format Xhrs, Ymins, Z sec.
   *
   * @param timeDiff The time difference to format
   */
  public static String formatTime(long timeDiff){
    StringBuilder buf = new StringBuilder();
    long hours = timeDiff / (60*60*1000);
    long rem = (timeDiff % (60*60*1000));
    long minutes =  rem / (60*1000);
    rem = rem % (60*1000);
    long seconds = rem / 1000;

    if (hours != 0){
      buf.append(hours);
      buf.append("hrs, ");
    }
    if (minutes != 0){
      buf.append(minutes);
      buf.append("mins, ");
    }
    // return "0sec if no difference
    buf.append(seconds);
    buf.append("sec");
    return buf.toString();
  }
  /**
   * Formats time in ms and appends difference (finishTime - startTime)
   * as returned by formatTimeDiff().
   * If finish time is 0, empty string is returned, if start time is 0
   * then difference is not appended to return value.
   * @param dateFormat date format to use
   * @param finishTime fnish time
   * @param startTime start time
   * @return formatted value.
   */
  public static String getFormattedTimeWithDiff(DateFormat dateFormat,
                                                long finishTime, long startTime){
    StringBuilder buf = new StringBuilder();
    if (0 != finishTime) {
      buf.append(dateFormat.format(new Date(finishTime)));
      if (0 != startTime){
        buf.append(" (" + formatTimeDiff(finishTime , startTime) + ")");
      }
    }
    return buf.toString();
  }

  /**
   * Returns an arraylist of strings.
   * @param str the comma seperated string values
   * @return the arraylist of the comma seperated string values
   */
  public static String[] getStrings(String str){
    Collection<String> values = getStringCollection(str);
    if(values.size() == 0) {
      return null;
    }
    return values.toArray(new String[values.size()]);
  }

  /**
   * Returns a collection of strings.
   * @param str comma seperated string values
   * @return an <code>ArrayList</code> of string values
   */
  public static Collection<String> getStringCollection(String str){
    List<String> values = new ArrayList<String>();
    if (str == null) {
      return values;
    }
    StringTokenizer tokenizer = new StringTokenizer (str,",");
    values = new ArrayList<String>();
    while (tokenizer.hasMoreTokens()) {
      values.add(tokenizer.nextToken());
    }
    return values;
  }

  /**
   * Splits a comma separated value <code>String</code>, trimming leading and trailing whitespace on each value.
   * @param str a comma separated <code>String</code> with values
   * @return a <code>Collection</code> of <code>String</code> values
   */
  public static Collection<String> getTrimmedStringCollection(String str){
    return new ArrayList<String>(
      Arrays.asList(getTrimmedStrings(str)));
  }

  /**
   * Splits a comma separated value <code>String</code>, trimming leading and trailing whitespace on each value.
   * @param str a comma separated <code>String</code> with values
   * @return an array of <code>String</code> values
   */
  public static String[] getTrimmedStrings(String str){
    if (null == str || "".equals(str.trim())) {
      return emptyStringArray;
    }

    return str.trim().split("\\s*,\\s*");
  }

  final public static String[] emptyStringArray = {};
  final public static char COMMA = ',';
  final public static char EQUALS = '=';
  final public static String COMMA_STR = ",";
  final public static char ESCAPE_CHAR = '\\';

  /**
   * Split a string using the default separator
   * @param str a string that may have escaped separator
   * @return an array of strings
   */
  public static String[] split(String str) {
    return split(str, ESCAPE_CHAR, COMMA);
  }

  /**
   * Split a string using the given separator
   * @param str a string that may have escaped separator
   * @param escapeChar a char that be used to escape the separator
   * @param separator a separator char
   * @return an array of strings
   */
  public static String[] split(
      String str, char escapeChar, char separator) {
    if (str==null) {
      return null;
    }
    ArrayList<String> strList = new ArrayList<String>();
    StringBuilder split = new StringBuilder();
    int index = 0;
    while ((index = findNext(str, separator, escapeChar, index, split)) >= 0) {
      ++index; // move over the separator for next search
      strList.add(split.toString());
      split.setLength(0); // reset the buffer
    }
    strList.add(split.toString());
    // remove trailing empty split(s)
    int last = strList.size(); // last split
    while (--last>=0 && "".equals(strList.get(last))) {
      strList.remove(last);
    }
    return strList.toArray(new String[strList.size()]);
  }

  /**
   * Split a string using the given separator, with no escaping performed.
   * @param str a string to be split. Note that this may not be null.
   * @param separator a separator char
   * @return an array of strings
   */
  public static String[] split(
      String str, char separator) {
    // String.split returns a single empty result for splitting the empty
    // string.
    if ("".equals(str)) {
      return new String[]{""};
    }
    ArrayList<String> strList = new ArrayList<String>();
    int startIndex = 0;
    int nextIndex = 0;
    while ((nextIndex = str.indexOf((int)separator, startIndex)) != -1) {
      strList.add(str.substring(startIndex, nextIndex));
      startIndex = nextIndex + 1;
    }
    strList.add(str.substring(startIndex));
    // remove trailing empty split(s)
    int last = strList.size(); // last split
    while (--last>=0 && "".equals(strList.get(last))) {
      strList.remove(last);
    }
    return strList.toArray(new String[strList.size()]);
  }

  /**
   * Split a string using the default separator/escape character,
   * then unescape the resulting array of strings
   * @param str
   * @return an array of unescaped strings
   */
  public static String[] splitAndUnEscape(String str) {
    return splitAndUnEscape(str, ESCAPE_CHAR, COMMA);
  }

  /**
   * Split a string using the specified separator/escape character,
   * then unescape the resulting array of strings using the same escape/separator.
   * @param str a string that may have escaped separator
   * @param escapeChar a char that be used to escape the separator
   * @param separator a separator char
   * @return an array of unescaped strings
   */
  public static String[] splitAndUnEscape(String str, char escapeChar, char separator) {
    String[] result = split(str, escapeChar, separator);
    if (result != null) {
      for (int idx = 0; idx < result.length; ++idx) {
        result[idx] = unEscapeString(result[idx], escapeChar, separator);
      }
    }
    return result;
  }

  /**
   * In a given string of comma-separated key=value pairs insert a new value of a given key
   *
   * @param key The key whose value needs to be replaced
   * @param newValue The new value of the key
   * @param strKvPairs Comma separated key=value pairs Eg: "k1=v1, k2=v2, k3=v3"
   * @return Comma separated string of key=value pairs with the new value for key keyName
   */
  public static String insertValue(String key, String newValue,
      String strKvPairs) {
    String[] keyValuePairs = HiveStringUtils.split(strKvPairs);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < keyValuePairs.length; i++) {
      String[] pair = HiveStringUtils.split(keyValuePairs[i], ESCAPE_CHAR, EQUALS);
      if (pair.length != 2) {
        throw new RuntimeException("Error parsing the keyvalue pair " + keyValuePairs[i]);
      }
      sb.append(pair[0]);
      sb.append(EQUALS);
      if (pair[0].equals(key)) {
        sb.append(newValue);
      } else {
        sb.append(pair[1]);
      }
      if (i < (keyValuePairs.length - 1)) {
        sb.append(COMMA);
      }
    }
    return sb.toString();
  }

  /**
   * Finds the first occurrence of the separator character ignoring the escaped
   * separators starting from the index. Note the substring between the index
   * and the position of the separator is passed.
   * @param str the source string
   * @param separator the character to find
   * @param escapeChar character used to escape
   * @param start from where to search
   * @param split used to pass back the extracted string
   */
  public static int findNext(String str, char separator, char escapeChar,
                             int start, StringBuilder split) {
    int numPreEscapes = 0;
    for (int i = start; i < str.length(); i++) {
      char curChar = str.charAt(i);
      if (numPreEscapes == 0 && curChar == separator) { // separator
        return i;
      } else {
        split.append(curChar);
        numPreEscapes = (curChar == escapeChar)
                        ? (++numPreEscapes) % 2
                        : 0;
      }
    }
    return -1;
  }

  /**
   * Escape commas in the string using the default escape char
   * @param str a string
   * @return an escaped string
   */
  public static String escapeString(String str) {
    return escapeString(str, ESCAPE_CHAR, COMMA);
  }

  /**
   * Escape <code>charToEscape</code> in the string
   * with the escape char <code>escapeChar</code>
   *
   * @param str string
   * @param escapeChar escape char
   * @param charToEscape the char to be escaped
   * @return an escaped string
   */
  public static String escapeString(
      String str, char escapeChar, char charToEscape) {
    return escapeString(str, escapeChar, new char[] {charToEscape});
  }

  // check if the character array has the character
  private static boolean hasChar(char[] chars, char character) {
    for (char target : chars) {
      if (character == target) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param charsToEscape array of characters to be escaped
   */
  public static String escapeString(String str, char escapeChar,
                                    char[] charsToEscape) {
    if (str == null) {
      return null;
    }
    StringBuilder result = new StringBuilder();
    for (int i=0; i<str.length(); i++) {
      char curChar = str.charAt(i);
      if (curChar == escapeChar || hasChar(charsToEscape, curChar)) {
        // special char
        result.append(escapeChar);
      }
      result.append(curChar);
    }
    return result.toString();
  }

  /**
   * Escape non-unicode characters. StringEscapeUtil.escapeJava() will escape
   * unicode characters as well but in some cases it's not desired.
   *
   * @param str Original string
   * @return Escaped string
   */
  public static String escapeJava(String str) {
    return ESCAPE_JAVA.translate(str);
  }

  /**
   * Escape non-unicode characters, and ', and ;
   * Like StringEscapeUtil.escapeJava() will escape
   * unicode characters as well but in some cases it's not desired.
   *
   * @param str Original string
   * @return Escaped string
   */
  public static String escapeHiveCommand(String str) {
    return ESCAPE_HIVE_COMMAND.translate(str);
  }

  /**
   * Unescape commas in the string using the default escape char
   * @param str a string
   * @return an unescaped string
   */
  public static String unEscapeString(String str) {
    return unEscapeString(str, ESCAPE_CHAR, COMMA);
  }

  /**
   * Unescape <code>charToEscape</code> in the string
   * with the escape char <code>escapeChar</code>
   *
   * @param str string
   * @param escapeChar escape char
   * @param charToEscape the escaped char
   * @return an unescaped string
   */
  public static String unEscapeString(
      String str, char escapeChar, char charToEscape) {
    return unEscapeString(str, escapeChar, new char[] {charToEscape});
  }

  /**
   * @param charsToEscape array of characters to unescape
   */
  public static String unEscapeString(String str, char escapeChar,
                                      char[] charsToEscape) {
    if (str == null) {
      return null;
    }
    StringBuilder result = new StringBuilder(str.length());
    boolean hasPreEscape = false;
    for (int i=0; i<str.length(); i++) {
      char curChar = str.charAt(i);
      if (hasPreEscape) {
        if (curChar != escapeChar && !hasChar(charsToEscape, curChar)) {
          // no special char
          throw new IllegalArgumentException("Illegal escaped string " + str +
              " unescaped " + escapeChar + " at " + (i-1));
        }
        // otherwise discard the escape char
        result.append(curChar);
        hasPreEscape = false;
      } else {
        if (hasChar(charsToEscape, curChar)) {
          throw new IllegalArgumentException("Illegal escaped string " + str +
              " unescaped " + curChar + " at " + i);
        } else if (curChar == escapeChar) {
          hasPreEscape = true;
        } else {
          result.append(curChar);
        }
      }
    }
    if (hasPreEscape ) {
      throw new IllegalArgumentException("Illegal escaped string " + str +
          ", not expecting " + escapeChar + " in the end." );
    }
    return result.toString();
  }

  /**
   * Return a message for logging.
   * @param prefix prefix keyword for the message
   * @param msg content of the message
   * @return a message for logging
   */
  private static String toStartupShutdownString(String prefix, String [] msg) {
    StringBuilder b = new StringBuilder(prefix);
    b.append("\n/************************************************************");
    for(String s : msg) {
      b.append("\n" + prefix + s);
    }
    b.append("\n************************************************************/");
    return b.toString();
  }

  /**
   * Print a log message for starting up and shutting down
   * @param clazz the class of the server
   * @param args arguments
   * @param LOG the target log object
   */
  public static void startupShutdownMessage(Class<?> clazz, String[] args,
                                     final org.slf4j.Logger LOG) {
    final String hostname = getHostname();
    final String classname = clazz.getSimpleName();
    LOG.info(
        toStartupShutdownString("STARTUP_MSG: ", new String[] {
            "Starting " + classname,
            "  host = " + hostname,
            "  args = " + Arrays.asList(args),
            "  version = " + HiveVersionInfo.getVersion(),
            "  classpath = " + System.getProperty("java.class.path"),
            "  build = " + HiveVersionInfo.getUrl() + " -r "
                         + HiveVersionInfo.getRevision()
                         + "; compiled by '" + HiveVersionInfo.getUser()
                         + "' on " + HiveVersionInfo.getDate()}
        )
      );

    ShutdownHookManager.addShutdownHook(
      new Runnable() {
        @Override
        public void run() {
          LOG.info(toStartupShutdownString("SHUTDOWN_MSG: ", new String[]{
            "Shutting down " + classname + " at " + hostname}));
        }
      }, SHUTDOWN_HOOK_PRIORITY);

  }

  /**
   * Return hostname without throwing exception.
   * @return hostname
   */
  public static String getHostname() {
    try {return "" + InetAddress.getLocalHost();}
    catch(UnknownHostException uhe) {return "" + uhe;}
  }

  
  /**
   * The traditional binary prefixes, kilo, mega, ..., exa,
   * which can be represented by a 64-bit integer.
   * TraditionalBinaryPrefix symbol are case insensitive.
   */
  public static enum TraditionalBinaryPrefix {
    KILO(1024),
    MEGA(KILO.value << 10),
    GIGA(MEGA.value << 10),
    TERA(GIGA.value << 10),
    PETA(TERA.value << 10),
    EXA(PETA.value << 10);

    public final long value;
    public final char symbol;

    TraditionalBinaryPrefix(long value) {
      this.value = value;
      this.symbol = toString().charAt(0);
    }

    /**
     * @return The TraditionalBinaryPrefix object corresponding to the symbol.
     */
    public static TraditionalBinaryPrefix valueOf(char symbol) {
      symbol = Character.toUpperCase(symbol);
      for(TraditionalBinaryPrefix prefix : TraditionalBinaryPrefix.values()) {
        if (symbol == prefix.symbol) {
          return prefix;
        }
      }
      throw new IllegalArgumentException("Unknown symbol '" + symbol + "'");
    }

    /**
     * Convert a string to long.
     * The input string is first be trimmed
     * and then it is parsed with traditional binary prefix.
     *
     * For example,
     * "-1230k" will be converted to -1230 * 1024 = -1259520;
     * "891g" will be converted to 891 * 1024^3 = 956703965184;
     *
     * @param s input string
     * @return a long value represented by the input string.
     */
    public static long string2long(String s) {
      s = s.trim();
      final int lastpos = s.length() - 1;
      final char lastchar = s.charAt(lastpos);
      if (Character.isDigit(lastchar)) {
        return Long.parseLong(s);
      } else {
        long prefix;
        try {
          prefix = TraditionalBinaryPrefix.valueOf(lastchar).value;
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Invalid size prefix '" + lastchar
              + "' in '" + s
              + "'. Allowed prefixes are k, m, g, t, p, e(case insensitive)");
        }
        long num = Long.parseLong(s.substring(0, lastpos));
        if (num > (Long.MAX_VALUE/prefix) || num < (Long.MIN_VALUE/prefix)) {
          throw new IllegalArgumentException(s + " does not fit in a Long");
        }
        return num * prefix;
      }
    }
  }

    /**
     * Escapes HTML Special characters present in the string.
     * @param string
     * @return HTML Escaped String representation
     */
    public static String escapeHTML(String string) {
      if(string == null) {
        return null;
      }
      StringBuilder sb = new StringBuilder();
      boolean lastCharacterWasSpace = false;
      char[] chars = string.toCharArray();
      for(char c : chars) {
        if(c == ' ') {
          if(lastCharacterWasSpace){
            lastCharacterWasSpace = false;
            sb.append("&nbsp;");
          }else {
            lastCharacterWasSpace=true;
            sb.append(" ");
          }
        }else {
          lastCharacterWasSpace = false;
          switch(c) {
          case '<': sb.append("&lt;"); break;
          case '>': sb.append("&gt;"); break;
          case '&': sb.append("&amp;"); break;
          case '"': sb.append("&quot;"); break;
          default : sb.append(c);break;
          }
        }
      }

      return sb.toString();
    }

  /**
   * Return an abbreviated English-language desc of the byte length
   */
  public static String byteDesc(long len) {
    double val = 0.0;
    String ending = "";
    if (len < 1024 * 1024) {
      val = (1.0 * len) / 1024;
      ending = " KB";
    } else if (len < 1024 * 1024 * 1024) {
      val = (1.0 * len) / (1024 * 1024);
      ending = " MB";
    } else if (len < 1024L * 1024 * 1024 * 1024) {
      val = (1.0 * len) / (1024 * 1024 * 1024);
      ending = " GB";
    } else if (len < 1024L * 1024 * 1024 * 1024 * 1024) {
      val = (1.0 * len) / (1024L * 1024 * 1024 * 1024);
      ending = " TB";
    } else {
      val = (1.0 * len) / (1024L * 1024 * 1024 * 1024 * 1024);
      ending = " PB";
    }
    return limitDecimalTo2(val) + ending;
  }

  public static synchronized String limitDecimalTo2(double d) {
    return decimalFormat.format(d);
  }

  /**
   * Concatenates strings, using a separator.
   *
   * @param separator Separator to join with.
   * @param strings Strings to join.
   */
  public static String join(CharSequence separator, Iterable<?> strings) {
    Iterator<?> i = strings.iterator();
    if (!i.hasNext()) {
      return "";
    }
    StringBuilder sb = new StringBuilder(i.next().toString());
    while (i.hasNext()) {
      sb.append(separator);
      sb.append(i.next().toString());
    }
    return sb.toString();
  }

  /**
   * Concatenates strings, using a separator. Empty/blank string or null will be
   * ignored.
   *
   * @param strings Strings to join.
   * @param separator Separator to join with.
   */
  public static String joinIgnoringEmpty(String[] strings, char separator) {
    ArrayList<String> list = new ArrayList<String>();
    for(String str : strings) {
      if (StringUtils.isNotBlank(str)) {
        list.add(str);
      }
    }

    return StringUtils.join(list, separator);
  }

  /**
   * Convert SOME_STUFF to SomeStuff
   *
   * @param s input string
   * @return camelized string
   */
  public static String camelize(String s) {
    StringBuilder sb = new StringBuilder();
    String[] words = split(s.toLowerCase(Locale.US), ESCAPE_CHAR, '_');

    for (String word : words) {
      sb.append(StringUtils.capitalize(word));
    }

    return sb.toString();
  }

  /**
   * Checks if b is the first byte of a UTF-8 character.
   *
   */
  public static boolean isUtfStartByte(byte b) {
    return (b & 0xC0) != 0x80;
  }

  public static int getTextUtfLength(Text t) {
    byte[] data = t.getBytes();
    int len = 0;
    for (int i = 0; i < t.getLength(); i++) {
      if (isUtfStartByte(data[i])) {
        len++;
      }
    }
    return len;
  }

  /**
   * Checks if b is an ascii character
   */
  public static boolean isAscii(byte b) {
    return (b & 0x80) == 0;
  }

  /**
   * Returns the number of leading whitespace characters in the utf-8 string
   */
  public static int findLeadingSpaces(byte[] bytes, int start, int length) {
    int numSpaces;
    for (numSpaces = 0; numSpaces < length; ++numSpaces) {
      int curPos = start + numSpaces;
      if (isAscii(bytes[curPos]) && Character.isWhitespace(bytes[curPos])) {
        continue;
      }
      break; // non-space character
    }
    return (numSpaces - start);
  }

  /**
   * Returns the number of trailing whitespace characters in the utf-8 string
   */
  public static int findTrailingSpaces(byte[] bytes, int start, int length) {
    int numSpaces;
    for (numSpaces = 0; numSpaces < length; ++numSpaces) {
      int curPos = start + (length - (numSpaces + 1));
      if (isAscii(bytes[curPos]) && Character.isWhitespace(bytes[curPos])) {
        continue;
      } else {
        break; // non-space character
      }
    }
    return numSpaces;
  }

  /**
   * Finds trimmed length of utf-8 string
   */
  public static int findTrimmedLength(byte[] bytes, int start, int length, int leadingSpaces) {
    int trailingSpaces = findTrailingSpaces(bytes, start, length);
    length = length - leadingSpaces;
    // If string is entirely whitespace, no need to apply trailingSpaces.
    if (length > 0) {
      length = length - trailingSpaces;
    }
    return length;
  }

  public static String normalizeIdentifier(String identifier) {
	  return identifier.trim().toLowerCase();
	}

  public static Map getPropertiesExplain(Properties properties) {
    if (properties != null) {
      String value = properties.getProperty("columns.comments");
      if (value != null) {
        // should copy properties first
        Map clone = new HashMap(properties);
        clone.put("columns.comments", quoteComments(value));
        return clone;
      }
    }
    return properties;
  }

  public static String quoteComments(String value) {
    char[] chars = value.toCharArray();
    if (!commentProvided(chars)) {
      return null;
    }
    StringBuilder builder = new StringBuilder();
    int prev = 0;
    for (int i = 0; i < chars.length; i++) {
      if (chars[i] == 0x00) {
        if (builder.length() > 0) {
          builder.append(',');
        }
        builder.append('\'').append(chars, prev, i - prev).append('\'');
        prev = i + 1;
      }
    }
    builder.append(",\'").append(chars, prev, chars.length - prev).append('\'');
    return builder.toString();
  }

  public static boolean commentProvided(char[] chars) {
    for (char achar : chars) {
      if (achar != 0x00) {
        return true;
      }
    }
    return false;
  }

  public static String getPartitionValWithInvalidCharacter(List<String> partVals,
      Pattern partitionValidationPattern) {
    if (partitionValidationPattern == null) {
      return null;
    }
  
    for (String partVal : partVals) {
      if (!partitionValidationPattern.matcher(partVal).matches()) {
        return partVal;
      }
    }
  
    return null;
  }

  /**
   * Strip comments from a sql statement, tracking when the statement contains a string literal.
   *
   * @param statement the input string
   * @return a stripped statement
   */
  public static String removeComments(String statement) {
    if (statement == null) {
      return null;
    }
    Iterator<String> iterator = Splitter.on("\n").omitEmptyStrings().split(statement).iterator();
    int[] startQuote = {-1};
    StringBuilder ret = new StringBuilder(statement.length());
    while (iterator.hasNext()) {
      String lineWithComments = iterator.next();
      String lineNoComments = removeComments(lineWithComments, startQuote);
      ret.append(lineNoComments);
      if (iterator.hasNext() && !lineNoComments.isEmpty()) {
        ret.append("\n");
      }
    }
    return ret.toString();
  }

  /**
   * Remove comments from the current line of a query.
   * Avoid removing comment-like strings inside quotes.
   * @param line a line of sql text
   * @param startQuote The value -1 indicates that line does not begin inside a string literal.
   *                   Other values indicate that line does begin inside a string literal
   *                   and the value passed is the delimiter character.
   *                   The array type is used to pass int type as input/output parameter.
   * @return the line with comments removed.
   */
  public static String removeComments(String line, int[] startQuote) {
    if (line == null || line.isEmpty()) {
      return line;
    }
    if (startQuote[0] == -1 && isComment(line)) {
      return "";  //assume # can only be used at the beginning of line.
    }
    StringBuilder builder = new StringBuilder();
    for (int index = 0; index < line.length();) {
      if (startQuote[0] == -1 && index < line.length() - 1 && line.charAt(index) == '-'
          && line.charAt(index + 1) == '-') {
        // Jump to the end of current line. When a multiple line query is executed with -e parameter,
        // it is passed in as one line string separated with '\n'
        for (; index < line.length() && line.charAt(index) != '\n'; ++index);
        continue;
      }

      char letter = line.charAt(index);
      if (startQuote[0] == letter && (index == 0 || line.charAt(index - 1) != '\\')) {
        startQuote[0] = -1; // Turn escape off.
      } else if (startQuote[0] == -1 && (letter == '\'' || letter == '"') && (index == 0
          || line.charAt(index - 1) != '\\')) {
        startQuote[0] = letter; // Turn escape on.
      }

      builder.append(letter);
      index++;
    }

    return builder.toString().trim();
  }

  /**
   * Test whether a line is a comment.
   *
   * @param line the line to be tested
   * @return true if a comment
   */
  private static boolean isComment(String line) {
    // SQL92 comment prefix is "--"
    // beeline also supports shell-style "#" prefix
    String lineTrimmed = line.trim();
    return lineTrimmed.startsWith("#") || lineTrimmed.startsWith("--");
  }

}
