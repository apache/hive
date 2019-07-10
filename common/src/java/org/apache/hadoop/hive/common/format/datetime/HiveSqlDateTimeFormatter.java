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

package org.apache.hadoop.hive.common.format.datetime;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Formatter using SQL:2016 datetime patterns.
 *
 * For all tokens:
 * - Patterns are case-insensitive, except AM/PM and T/Z. See these sections for more details.
 * - For string to datetime conversion, no duplicate format tokens are allowed, including tokens
 *   that have the same meaning but different lengths ("Y" and "YY" conflict) or different
 *   behaviors ("RR" and "YY" conflict).
 *
 * For all numeric tokens:
 * - The "expected length" of input/output is the number of tokens in the character (e.g. "YYY": 3,
 *   "Y": 1, and so on), with some exceptions (see map SPECIAL_LENGTHS).
 * - For string to datetime conversion, inputs of fewer digits than expected are accepted if
 *   followed by a delimiter, e.g. format="YYYY-MM-DD", input="19-1-1", output=2019-01-01 00:00:00.
 * - For datetime to string conversion, output is left padded with zeros, e.g. format="DD SSSSS",
 *   input=2019-01-01 00:00:03, output="01 00003".
 *
 *
 * Accepted format tokens:
 * Note: "|" means "or". "Delimiter" means a separator, tokens T or Z, or end of input.
 *
 * A. Temporal tokens
 * YYYY
 * 4-digit year
 * - For string to datetime conversion, prefix digits for 1, 2, and 3-digit inputs are obtained
 *   from current date
 *   E.g. input=‘9-01-01’, pattern =‘YYYY-MM-DD’, current year=2020, output=2029-01-01 00:00:00
 *
 *
 * YYY
 * Last 3 digits of a year
 * - Gets the prefix digit from current date.
 * - Can accept fewer digits than 3, similarly to YYYY.
 *
 * YY
 * Last 2 digits of a year
 * - Gets the 2 prefix digits from current date.
 * - Can accept fewer digits than 2, similarly to YYYY.
 *
 * Y
 * Last digit of a year
 * - Gets the 3 prefix digits from current date.
 *
 * RRRR
 * 4-digit rounded year
 * - String to datetime conversion:
 *   - If 2 digits are provided then acts like RR.
 *   - If 1,3 or 4 digits provided then acts like YYYY.
 * - For datetime to string conversion, acts like YYYY.
 *
 * RR
 * 2-digit rounded year
 * -String to datetime conversion:
 *   - Semantics:
 *     Input:     Last 2 digits of current year:   First 2 digits of output:
 *     0 to 49    00 to 49                         First 2 digits of current year
 *     0 to 49    50 to 99                         First 2 digits of current year + 1
 *     50 to 99   00 to 49                         First 2 digits of current year - 1
 *     50 to 99   50 to 99                         First 2 digits of current year
 *   - If 1-digit year is provided followed by a delimiter, falls back to YYYY with 1-digit year
 *     input.
 * - For datetime to string conversion, acts like YY.
 *
 * MM
 * Month (1-12)
 * - For string to datetime conversion, conflicts with DDD.
 *
 * DD
 * Day of month (1-31)
 * - For string to datetime conversion, conflicts with DDD.
 *
 * DDD
 * Day of year (1-366)
 * - For string to datetime conversion, conflicts with DD and MM.
 *
 * HH
 * Hour of day (1-12)
 * - If no AM/PM provided then defaults to AM.
 * - In string to datetime conversion, conflicts with SSSSS and HH24.
 *
 * HH12
 * Hour of day (1-12)
 * See HH.
 *
 * HH24
 * Hour of day (0-23)
 * - In string to datetime conversion, conflicts with SSSSS, HH12 and AM/PM.
 *
 * MI
 * Minute of hour (0-59)
 * - In string to datetime conversion, conflicts with SSSSS.
 *
 * SS
 * Second of minute (0-59)
 * - In string to datetime conversion, conflicts with SSSSS.
 *
 * SSSSS
 * Second of Day (0-86399)
 * - In string to datetime conversion, conflicts with SS, HH, HH12, HH24, MI, AM/PM.
 *
 * FF[1..9]
 * Fraction of second
 * - 1..9 indicates the number of decimal digits. "FF" (no number of digits specified) is also
 *   accepted.
 * - In datetime to string conversion, "FF" will omit trailing zeros, or output "0" if subsecond
 *   value is 0.
 * - In string to datetime conversion, fewer digits than expected are accepted if followed by a
 *   delimiter. "FF" acts like "FF9".
 *
 * AM|A.M.
 * Meridiem indicator or AM/PM
 * - Datetime to string conversion:
 *   - AM and PM mean the exact same thing in the pattern.
 *     e.g. input=2019-01-01 20:00, format=“AM”, output=“PM”.
 *   - Retains the exact format (capitalization and length) provided in the pattern string. If p.m.
 *     is in the pattern, we expect a.m. or p.m. in the output; if AM is in the pattern, we expect
 *     AM or PM in the output.
 * - String to datetime conversion:
 *   - Conflicts with HH24 and SSSSS.
 *   - It doesn’t matter which meridian indicator is in the pattern.
 *     E.g. input="2019-01-01 11:00 p.m.", pattern="YYYY-MM-DD HH12:MI AM",
 *          output=2019-01-01 23:00:00
 *
 * PM|P.M.
 * Meridiem indicator
 * See AM|A.M.
 *
 * B. Time zone tokens
 * TZH
 * Time zone offset hour (-15 to +15)
 * - 3-character-long input is expected: 1 character for the sign and 2 digits for the value.
 *   e.g. “+10”, “-05”
 * - 2-digit input is accepted without the sign, e.g. “04”.
 * - Both these 2 and 3-digit versions are accepted even if not followed by separators.
 * - Disabled for timestamp to string and date to string conversion, as timestamp and date are time
 *   zone agnostic.
 *
 * TZM
 * Time zone offset minute (0-59)
 * - For string to datetime conversion:
 *   - TZH token is required.
 *   - Unsigned; sign comes from TZH.
 *   - Therefore time zone offsets like “-30” minutes should be expressed thus: input=“-00:30”
 *     pattern=“TZH:TZM”.
 * - Disabled for timestamp to string and date to string conversion, as timestamp and date are time
 *   zone agnostic.
 *
 * C. Separators
 * -|.|/|,|'|;|:|<space>
 * Separator
 * - Uses loose matching. Existence of a sequence of separators in the format should match the
 *   existence of a sequence of separators in the input regardless of the types of the separator or
 *   the length of the sequence where length > 1. E.g. input=“2019-. ;10/10”, pattern=“YYYY-MM-DD”
 *   is valid; input=“20191010”, pattern=“YYYY-MM-DD” is not valid.
 * - If the last separator character in the separator substring is "-" and is immediately followed
 *   by a time zone hour (tzh) token, it's a negative sign and not counted as a separator, UNLESS
 *   this is the only possible separator character in the separator substring (in which case it is
 *   not counted as the tzh's negative sign).
 *
 * D. ISO 8601 delimiters
 * T
 * ISO 8601 delimiter
 * - Serves as a delimiter.
 * - Function is to support formats like “YYYY-MM-DDTHH24:MI:SS.FF9Z”, “YYYY-MM-DD-HH24:MI:SSZ”
 * - For datetime to string conversion, output is always capitalized ("T"), even if lowercase ("t")
 *   is provided in the pattern.
 *
 * Z
 * ISO 8601 delimiter
 * See T.
 */

public class HiveSqlDateTimeFormatter implements Serializable {

  private static final int LONGEST_TOKEN_LENGTH = 5;
  private static final int LONGEST_ACCEPTED_PATTERN = 100; // for sanity's sake
  private static final long MINUTES_PER_HOUR = 60;
  private static final int NANOS_MAX_LENGTH = 9;
  public static final int AM = 0;
  public static final int PM = 1;
  private String pattern;
  private List<Token> tokens = new ArrayList<>();

  private static final Map<String, TemporalField> TEMPORAL_TOKENS =
      ImmutableMap.<String, TemporalField>builder()
          .put("yyyy", ChronoField.YEAR).put("yyy", ChronoField.YEAR)
          .put("yy", ChronoField.YEAR).put("y", ChronoField.YEAR)
          .put("rrrr", ChronoField.YEAR).put("rr", ChronoField.YEAR)
          .put("mm", ChronoField.MONTH_OF_YEAR)
          .put("dd", ChronoField.DAY_OF_MONTH)
          .put("ddd", ChronoField.DAY_OF_YEAR)
          .put("hh", ChronoField.HOUR_OF_AMPM)
          .put("hh12", ChronoField.HOUR_OF_AMPM)
          .put("hh24", ChronoField.HOUR_OF_DAY)
          .put("mi", ChronoField.MINUTE_OF_HOUR)
          .put("ss", ChronoField.SECOND_OF_MINUTE)
          .put("sssss", ChronoField.SECOND_OF_DAY)
          .put("ff1", ChronoField.NANO_OF_SECOND).put("ff2", ChronoField.NANO_OF_SECOND)
          .put("ff3", ChronoField.NANO_OF_SECOND).put("ff4", ChronoField.NANO_OF_SECOND)
          .put("ff5", ChronoField.NANO_OF_SECOND).put("ff6", ChronoField.NANO_OF_SECOND)
          .put("ff7", ChronoField.NANO_OF_SECOND).put("ff8", ChronoField.NANO_OF_SECOND)
          .put("ff9", ChronoField.NANO_OF_SECOND).put("ff", ChronoField.NANO_OF_SECOND)
          .put("a.m.", ChronoField.AMPM_OF_DAY).put("am", ChronoField.AMPM_OF_DAY)
          .put("p.m.", ChronoField.AMPM_OF_DAY).put("pm", ChronoField.AMPM_OF_DAY)
          .build();

  private static final Map<String, TemporalUnit> TIME_ZONE_TOKENS =
      ImmutableMap.<String, TemporalUnit>builder()
          .put("tzh", ChronoUnit.HOURS).put("tzm", ChronoUnit.MINUTES).build();

  private static final List<String> VALID_ISO_8601_DELIMITERS =
      ImmutableList.of("t", "z");

  private static final List<String> VALID_SEPARATORS =
      ImmutableList.of("-", ":", " ", ".", "/", ";", "\'", ",");

  private static final Map<String, Integer> SPECIAL_LENGTHS = ImmutableMap.<String, Integer>builder()
      .put("hh12", 2).put("hh24", 2).put("tzm", 2).put("am", 4).put("pm", 4)
      .put("ff1", 1).put("ff2", 2).put("ff3", 3).put("ff4", 4).put("ff5", 5)
      .put("ff6", 6).put("ff7", 7).put("ff8", 8).put("ff9", 9).put("ff", 9)
      .build();

  /**
   * Represents broad categories of tokens.
   */
  public enum TokenType {
    TEMPORAL,
    SEPARATOR,
    TIMEZONE,
    ISO_8601_DELIMITER
  }

  /**
   * Token representation.
   */
  public static class Token implements Serializable {
    TokenType type;
    TemporalField temporalField; // for type TEMPORAL e.g. ChronoField.YEAR
    TemporalUnit temporalUnit; // for type TIMEZONE e.g. ChronoUnit.HOURS
    String string; // pattern string, e.g. "yyy"
    int length; // length (e.g. YYY: 3, FF8: 8)

    public Token(TemporalField temporalField, String string, int length) {
      this(TokenType.TEMPORAL, temporalField, null, string, length);
    }

    public Token(TemporalUnit temporalUnit, String string, int length) {
      this(TokenType.TIMEZONE, null, temporalUnit, string, length);
    }

    public Token(TokenType tokenType, String string) {
      this(tokenType, null, null, string, string.length());
    }

    public Token(TokenType tokenType, TemporalField temporalField, TemporalUnit temporalUnit,
        String string, int length) {
      this.type = tokenType;
      this.temporalField = temporalField;
      this.temporalUnit = temporalUnit;
      this.string = string;
      this.length = length;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(string);
      sb.append(" type: ");
      sb.append(type);
      if (temporalField != null) {
        sb.append(" temporalField: ");
        sb.append(temporalField);
      } else if (temporalUnit != null) {
        sb.append(" temporalUnit: ");
        sb.append(temporalUnit);
      }
      return sb.toString();
    }
  }

  public HiveSqlDateTimeFormatter(String pattern, boolean forParsing) {
    setPattern(pattern, forParsing);
  }

  /**
   * Parse and perhaps verify the pattern.
   */
  private void setPattern(String pattern, boolean forParsing) {
    assert pattern.length() < LONGEST_ACCEPTED_PATTERN : "The input format is too long";
    this.pattern = pattern;

    parsePatternToTokens(pattern);

    // throw IllegalArgumentException if pattern is invalid
    if (forParsing) {
      verifyForParse();
    } else {
      verifyForFormat();
    }
  }

  /**
   * Parse pattern to list of tokens.
   */
  private String parsePatternToTokens(String pattern) {
    tokens.clear();
    String originalPattern = pattern;
    pattern = pattern.toLowerCase();

    // indexes of the substring we will check (includes begin, does not include end)
    int begin=0, end=0;
    String candidate;
    Token lastAddedToken = null;

    while (begin < pattern.length()) {
      // if begin hasn't progressed, then pattern is not parsable
      if (begin != end) {
        tokens.clear();
        throw new IllegalArgumentException("Bad date/time conversion pattern: " + pattern);
      }

      // find next token
      for (int i = LONGEST_TOKEN_LENGTH; i > 0; i--) {
        end = begin + i;
        if (end > pattern.length()) { // don't go past the end of the pattern string
          continue;
        }
        candidate = pattern.substring(begin, end);
        if (isSeparator(candidate)) {
          lastAddedToken = parseSeparatorToken(candidate, lastAddedToken);
          begin = end;
          break;
        }
        if (isIso8601Delimiter(candidate)) {
          lastAddedToken = parseIso8601DelimiterToken(candidate);
          begin = end;
          break;
        }
        if (isTemporalToken(candidate)) {
          lastAddedToken = parseTemporalToken(originalPattern, begin, candidate);
          begin = end;
          break;
        }
        if (isTimeZoneToken(candidate)) {
          lastAddedToken = parseTimeZoneToken(candidate);
          begin = end;
          break;
        }
      }
    }
    return pattern;
  }

  private boolean isSeparator(String candidate) {
    return candidate.length() == 1 && VALID_SEPARATORS.contains(candidate);
  }

  private boolean isIso8601Delimiter(String candidate) {
    return candidate.length() == 1 && VALID_ISO_8601_DELIMITERS.contains(candidate);
  }

  private boolean isTemporalToken(String candidate) {
    return TEMPORAL_TOKENS.containsKey(candidate);
  }

  private boolean isTimeZoneToken(String pattern) {
    return TIME_ZONE_TOKENS.containsKey(pattern);
  }

  private Token parseSeparatorToken(String candidate, Token lastAddedToken) {
    // try to clump separator with immediately preceding separators (e.g. "---" counts as one
    // separator)
    if (lastAddedToken != null && lastAddedToken.type == TokenType.SEPARATOR) {
      lastAddedToken.string += candidate;
      lastAddedToken.length += 1;
    } else {
      lastAddedToken = new Token(TokenType.SEPARATOR, candidate);
      tokens.add(lastAddedToken);
    }
    return lastAddedToken;
  }

  private Token parseIso8601DelimiterToken(String candidate) {
    Token lastAddedToken;
    lastAddedToken = new Token(TokenType.ISO_8601_DELIMITER, candidate.toUpperCase());
    tokens.add(lastAddedToken);
    return lastAddedToken;
  }

  private Token parseTemporalToken(String originalPattern, int begin, String candidate) {
    Token lastAddedToken;

    // for AM/PM, keep original case
    if (TEMPORAL_TOKENS.get(candidate) == ChronoField.AMPM_OF_DAY) {
      int subStringEnd = begin + candidate.length();
      candidate = originalPattern.substring(begin, subStringEnd);
    }
    lastAddedToken = new Token(TEMPORAL_TOKENS.get(candidate.toLowerCase()), candidate,
        getTokenStringLength(candidate.toLowerCase()));
    tokens.add(lastAddedToken);
    return lastAddedToken;
  }

  private Token parseTimeZoneToken(String candidate) {
    Token lastAddedToken;
    lastAddedToken = new Token(TIME_ZONE_TOKENS.get(candidate), candidate,
        getTokenStringLength(candidate));
    tokens.add(lastAddedToken);
    return lastAddedToken;
  }

  private int getTokenStringLength(String candidate) {
    Integer length = SPECIAL_LENGTHS.get(candidate);
    if (length != null) {
      return length;
    }
    return candidate.length();
  }

  /**
   * Make sure the generated list of tokens is valid for parsing strings to datetime objects.
   */
  private void verifyForParse() {

    // create a list of tokens' temporal fields
    ArrayList<TemporalField> temporalFields = new ArrayList<>();
    ArrayList<TemporalUnit> timeZoneTemporalUnits = new ArrayList<>();
    int roundYearCount=0, yearCount=0;
    for (Token token : tokens) {
      if (token.temporalField != null) {
        temporalFields.add(token.temporalField);
        if (token.temporalField == ChronoField.YEAR) {
          if (token.string.startsWith("r")) {
            roundYearCount += 1;
          } else {
            yearCount += 1;
          }
        }
      } else if (token.temporalUnit != null) {
        timeZoneTemporalUnits.add(token.temporalUnit);
      }
    }
    if (!(temporalFields.contains(ChronoField.YEAR))) {
      throw new IllegalArgumentException("Missing year token.");
    }
    if (!(temporalFields.contains(ChronoField.MONTH_OF_YEAR) &&
            temporalFields.contains(ChronoField.DAY_OF_MONTH) ||
            temporalFields.contains(ChronoField.DAY_OF_YEAR))) {
      throw new IllegalArgumentException("Missing day of year or (month of year + day of month)"
          + " tokens.");
    }
    if (roundYearCount > 0 && yearCount > 0) {
      throw new IllegalArgumentException("Invalid duplication of format element: Both year and"
          + "round year are provided");
    }
    for (TemporalField tokenType : temporalFields) {
      if (Collections.frequency(temporalFields, tokenType) > 1) {
        throw new IllegalArgumentException(
            "Invalid duplication of format element: multiple " + tokenType.toString()
                + " tokens provided.");
      }
    }
    if (temporalFields.contains(ChronoField.AMPM_OF_DAY) &&
        !(temporalFields.contains(ChronoField.HOUR_OF_DAY) ||
            temporalFields.contains(ChronoField.HOUR_OF_AMPM))) {
      throw new IllegalArgumentException("AM/PM provided but missing hour token.");
    }
    if (temporalFields.contains(ChronoField.AMPM_OF_DAY) &&
        temporalFields.contains(ChronoField.HOUR_OF_DAY)) {
      throw new IllegalArgumentException("Conflict between median indicator and hour token.");
    }
    if (temporalFields.contains(ChronoField.HOUR_OF_AMPM) &&
        temporalFields.contains(ChronoField.HOUR_OF_DAY)) {
      throw new IllegalArgumentException("Conflict between hour of day and hour of am/pm token.");
    }
    if (temporalFields.contains(ChronoField.DAY_OF_YEAR) &&
        (temporalFields.contains(ChronoField.DAY_OF_MONTH) ||
            temporalFields.contains(ChronoField.MONTH_OF_YEAR))) {
      throw new IllegalArgumentException("Day of year provided with day or month token.");
    }
    if (temporalFields.contains(ChronoField.SECOND_OF_DAY) &&
        (temporalFields.contains(ChronoField.HOUR_OF_DAY) ||
            temporalFields.contains(ChronoField.HOUR_OF_AMPM) ||
            temporalFields.contains(ChronoField.MINUTE_OF_HOUR) ||
            temporalFields.contains(ChronoField.SECOND_OF_MINUTE))) {
      throw new IllegalArgumentException(
          "Second of day token conflicts with other token(s).");
    }
    if (timeZoneTemporalUnits.contains(ChronoUnit.MINUTES) &&
        !timeZoneTemporalUnits.contains(ChronoUnit.HOURS)) {
      throw new IllegalArgumentException("Time zone minute token provided without time zone hour token.");
    }
  }

  /**
   * Make sure the generated list of tokens is valid for formatting datetime objects to strings.
   */
  private void verifyForFormat() {
    for (Token token : tokens) {
      if (token.type == TokenType.TIMEZONE) {
        throw new IllegalArgumentException(token.string.toUpperCase() + " not a valid format for "
            + "timestamp or date.");
      }
    }
  }

  public String format(Timestamp ts) {
    StringBuilder fullOutputSb = new StringBuilder();
    String outputString = null;
    int value;
    LocalDateTime localDateTime =
        LocalDateTime.ofEpochSecond(ts.toEpochSecond(), ts.getNanos(), ZoneOffset.UTC);
    for (Token token : tokens) {
      switch (token.type) {
      case TEMPORAL:
        try {
          value = localDateTime.get(token.temporalField);
          outputString = formatTemporal(value, token);
        } catch (DateTimeException e) {
          throw new IllegalArgumentException(token.temporalField + " couldn't be obtained from "
              + "LocalDateTime " + localDateTime, e);
        }
        break;
      case TIMEZONE: //invalid for timestamp and date
        throw new IllegalArgumentException(token.string.toUpperCase() + " not a valid format for "
            + "timestamp or date.");
      case SEPARATOR:
        outputString = token.string;
        break;
      case ISO_8601_DELIMITER:
        outputString = token.string.toUpperCase();
        break;
      default:
        // won't happen
      }
      fullOutputSb.append(outputString);
    }
    return fullOutputSb.toString();
  }

  public String format(Date date) {
    return format(Timestamp.ofEpochSecond(date.toEpochSecond()));
  }

  private String formatTemporal(int value, Token token) {
    String output;
    if (token.temporalField == ChronoField.AMPM_OF_DAY) {
      output = value == 0 ? "a" : "p";
      output += token.string.length() == 2 ? "m" : ".m.";
      if (token.string.startsWith("A") || token.string.startsWith("P")) {
        output = output.toUpperCase();
      }
    } else { // it's a numeric value

      if (token.temporalField == ChronoField.HOUR_OF_AMPM && value == 0) {
        value = 12;
      }
      try {
        output = String.valueOf(value);
        output = padOrTruncateNumericTemporal(token, output);
      } catch (Exception e) {
        throw new IllegalArgumentException("Value: " + value + " couldn't be cast to string.", e);
      }
    }
    return output;
  }

  /**
   * To match token.length, pad left with zeroes or truncate.
   */
  private String padOrTruncateNumericTemporal(Token token, String output) {
    if (output.length() < token.length) {
      output = StringUtils.leftPad(output, token.length, '0'); // pad left
    } else if (output.length() > token.length) {
      if (token.temporalField == ChronoField.NANO_OF_SECOND) {
        output = output.substring(0, token.length); // truncate right
      } else {
        output = output.substring(output.length() - token.length); // truncate left
      }
    }
    if (token.temporalField == ChronoField.NANO_OF_SECOND
        && token.string.equalsIgnoreCase("ff")) {
      output = output.replaceAll("0*$", ""); //truncate trailing 0's
      if (output.isEmpty()) {
        output = "0";
      }
    }
    return output;
  }

  /**
   * Left here for timestamp with local time zone.
   */
  private String formatTimeZone(TimeZone timeZone, LocalDateTime localDateTime, Token token) {
    ZoneOffset offset = timeZone.toZoneId().getRules().getOffset(localDateTime);
    Duration seconds = Duration.of(offset.get(ChronoField.OFFSET_SECONDS), ChronoUnit.SECONDS);
    if (token.string.equals("tzh")) {
      long hours = seconds.toHours();
      String s = (hours >= 0) ? "+" : "-";
      s += (Math.abs(hours) < 10) ? "0" : "";
      s += String.valueOf(Math.abs(hours));
      return s;
    } else {
      long minutes = Math.abs(seconds.toMinutes() % MINUTES_PER_HOUR);
      String s = String.valueOf(minutes);
      if (s.length() == 1) {
        s = "0" + s;
      }
      return s;
    }
  }

  public Timestamp parseTimestamp(String fullInput){
    LocalDateTime ldt = LocalDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
    String substring;
    int index = 0;
    int value;
    int timeZoneSign = 0, timeZoneHours = 0, timeZoneMinutes = 0;

    for (Token token : tokens) {
      switch (token.type) {
      case TEMPORAL:
        substring = getNextSubstring(fullInput, index, token); // e.g. yy-m -> yy
        value = parseTemporal(substring, token); // e.g. 18->2018, July->07
        try {
          ldt = ldt.with(token.temporalField, value);
        } catch (DateTimeException e){
          throw new IllegalArgumentException(
              "Value " + value + " not valid for token " + token.toString());
        }
        index += substring.length();
        break;
      case TIMEZONE:
        if (token.temporalUnit == ChronoUnit.HOURS) {
          String nextCharacter = fullInput.substring(index, index + 1);
          timeZoneSign = "-".equals(nextCharacter) ? -1 : 1;
          if ("-".equals(nextCharacter) || "+".equals(nextCharacter)) {
            index++;
          }
          // parse next two digits
          substring = getNextSubstring(fullInput, index, index + 2, token);
          try {
            timeZoneHours = Integer.parseInt(substring);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Couldn't parse substring \"" + substring +
                "\" with token " + token + " to int. Pattern is " + pattern, e);
          }
          if (timeZoneHours < -15 || timeZoneHours > 15) {
            throw new IllegalArgumentException("Couldn't parse substring \"" + substring +
                "\" to TZH because TZH range is -15 to +15. Pattern is " + pattern);
          }
        } else { // time zone minutes
          substring = getNextSubstring(fullInput, index, token);
          try {
            timeZoneMinutes = Integer.parseInt(substring);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Couldn't parse substring \"" + substring +
            "\" with token " + token + " to int. Pattern is " + pattern, e);
          }
          if (timeZoneMinutes < 0 || timeZoneMinutes > 59) {
            throw new IllegalArgumentException("Couldn't parse substring \"" + substring +
                "\" to TZM because TZM range is 0 to 59. Pattern is " + pattern);
          }
        }
        index += substring.length();
        break;
      case SEPARATOR:
        index = parseSeparator(fullInput, index, token);
        break;
      case ISO_8601_DELIMITER:
        index = parseIso8601Delimiter(fullInput, index, token);
      default:
        //do nothing
      }
    }

    // anything left unparsed at end of string? throw error
    if (!fullInput.substring(index).isEmpty()) {
      throw new IllegalArgumentException("Leftover input after parsing: " +
          fullInput.substring(index) + " in string " + fullInput);
    }

    return Timestamp.ofEpochSecond(ldt.toEpochSecond(ZoneOffset.UTC), ldt.getNano());
  }

  public Date parseDate(String input){
    return Date.ofEpochMilli(parseTimestamp(input).toEpochMilli());
  }
  /**
   * Return the next substring to parse. Length is either specified or token.length, but a
   * separator or an ISO-8601 delimiter can cut the substring short. (e.g. if the token pattern is
   * "YYYY" we expect the next 4 characters to be 4 numbers. However, if it is "976/" then we
   * return "976" because a separator cuts it short.)
   */
  private String getNextSubstring(String s, int begin, Token token) {
    return getNextSubstring(s, begin, begin + token.length, token);
  }

  private String getNextSubstring(String s, int begin, int end, Token token) {
    if (end > s.length()) {
      end = s.length();
    }
    s = s.substring(begin, end);
    if (token.temporalField == ChronoField.AMPM_OF_DAY) {
      if (s.charAt(1) == 'm' || s.charAt(1) == 'M') { // length 2
        return s.substring(0, 2);
      } else {
        return s;
      }
    }
    for (String sep : VALID_SEPARATORS) {
      if (s.contains(sep)) {
        s = s.substring(0, s.indexOf(sep));
      }
    }
    // TODO this will cause problems with DAY (for example, Thursday starts with T)
    for (String delimiter : VALID_ISO_8601_DELIMITERS) {
      if (s.toLowerCase().contains(delimiter)) {
        s = s.substring(0, s.toLowerCase().indexOf(delimiter));
      }
    }

    return s;
  }

  /**
   * Get the integer value of a temporal substring.
   */
  private int parseTemporal(String substring, Token token){
    // exceptions to the rule
    if (token.temporalField == ChronoField.AMPM_OF_DAY) {
      return substring.toLowerCase().startsWith("a") ? AM : PM;

    } else if (token.temporalField == ChronoField.HOUR_OF_AMPM && "12".equals(substring)) {
      substring = "0";

    } else if (token.temporalField == ChronoField.YEAR) {
      String currentYearString = String.valueOf(LocalDateTime.now().getYear());
      //deal with round years
      if (token.string.startsWith("r") && substring.length() == 2) {
        int currFirst2Digits = Integer.parseInt(currentYearString.substring(0, 2));
        int currLast2Digits = Integer.parseInt(currentYearString.substring(2));
        int valLast2Digits = Integer.parseInt(substring);
        if (valLast2Digits < 50 && currLast2Digits >= 50) {
          currFirst2Digits += 1;
        } else if (valLast2Digits >= 50 && currLast2Digits < 50) {
          currFirst2Digits -= 1;
        }
        substring = String.valueOf(currFirst2Digits) + substring;
      } else { // fill in prefix digits with current date
        substring = currentYearString.substring(0, 4 - substring.length()) + substring;
      }

    } else if (token.temporalField == ChronoField.NANO_OF_SECOND) {
      int i = Integer.min(token.length, substring.length());
      substring += StringUtils.repeat("0", NANOS_MAX_LENGTH - i);
    }

    // the rule
    try {
      return Integer.parseInt(substring);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Couldn't parse substring \"" + substring +
          "\" with token " + token + " to integer. Pattern is " + pattern, e);
    }
  }

  /**
   * Parse the next separator(s). At least one separator character is expected. Separator
   * characters are interchangeable.
   *
   * Caveat: If the last separator character in the separator substring is "-" and is immediately
   *     followed by a time zone hour (tzh) token, it's a negative sign and not counted as a
   *     separator, UNLESS this is the only separator character in the separator substring (in
   *     which case it is not counted as the negative sign).
   *
   * @throws IllegalArgumentException if separator is missing
   */
  private int parseSeparator(String fullInput, int index, Token token){
    int separatorsFound = 0;
    int begin = index;

    while (index < fullInput.length() &&
        VALID_SEPARATORS.contains(fullInput.substring(index, index + 1))) {
      if (!isLastCharacterOfSeparator(index, fullInput)
          || !("-".equals(fullInput.substring(index, index + 1)) && (nextTokenIs("tzh", token)))
          || separatorsFound == 0) {
        separatorsFound++;
      }
      index++;
    }

    if (separatorsFound == 0) {
      throw new IllegalArgumentException("Missing separator at index " + index);
    }
    return begin + separatorsFound;
  }

  private int parseIso8601Delimiter(String fullInput, int index, Token token) {
    String substring;
    substring = fullInput.substring(index, index + 1);
    if (token.string.equalsIgnoreCase(substring)) {
      index++;
    } else {
      throw new IllegalArgumentException(
          "Missing ISO 8601 delimiter " + token.string.toUpperCase());
    }
    return index;
  }

  /**
   * Is the next character something other than a separator?
   */
  private boolean isLastCharacterOfSeparator(int index, String string) {
    if (index == string.length() - 1) { // if we're at the end of the string, yes
      return true;
    }
    return !VALID_SEPARATORS.contains(string.substring(index + 1, index + 2));
  }

  /**
   * Does the temporalUnit/temporalField of the next token match the pattern's?
   */
  private boolean nextTokenIs(String pattern, Token currentToken) {
    // make sure currentToken isn't the last one
    if (tokens.indexOf(currentToken) == tokens.size() - 1) {
      return false;
    }
    Token nextToken = tokens.get(tokens.indexOf(currentToken) + 1);
    pattern = pattern.toLowerCase();
    return (isTimeZoneToken(pattern) && TIME_ZONE_TOKENS.get(pattern) == nextToken.temporalUnit
        || isTemporalToken(pattern) && TEMPORAL_TOKENS.get(pattern) == nextToken.temporalField);
  }

  public String getPattern() {
    return pattern;
  }

  /**
   * @return a copy of token list
   */
  protected List<Token> getTokens() {
    return new ArrayList<>(tokens);
  }
}
