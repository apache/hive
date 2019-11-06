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
import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Formatter using SQL:2016 datetime patterns.
 *
 * For all tokens:
 * - Patterns are case-insensitive, except AM/PM, T/Z and nested strings. See these sections for more
 *   details.
 * - For string to datetime conversion, no duplicate format tokens are allowed, including tokens
 *   that have the same meaning but different lengths ("Y" and "YY" conflict) or different
 *   behaviors ("RR" and "YY" conflict).
 *
 * For all numeric tokens:
 * - The "expected length" of input/output is the number of tokens in the character (e.g. "YYY": 3,
 *   "Y": 1, and so on), with some exceptions (see map SPECIAL_LENGTHS).
 * - For string to datetime conversion, inputs of fewer digits than expected are accepted if
 *   followed by a delimiter, e.g. format="YYYY-MM-DD", input="19-1-1", output=2019-01-01 00:00:00.
 *   This is modified by format modifier FX (format exact). See FX for details.
 * - For datetime to string conversion, output is left padded with zeros, e.g. format="DD SSSSS",
 *   input=2019-01-01 00:00:03, output="01 00003".
 *   This is modified by format modifier FM (fill mode). See FM for details.
 *
 *
 * Accepted format tokens:
 * Note: - "|" means "or".
 *       - "Delimiter" for numeric tokens means any non-numeric character or end of input.
 *       - The words token and pattern are used interchangeably.
 *
 * A.1. Numeric temporal tokens
 * YYYY
 * 4-digit year
 * - For string to datetime conversion, prefix digits for 1, 2, and 3-digit inputs are obtained
 *   from current date
 *   E.g. input=‘9-01-01’, pattern =‘YYYY-MM-DD’, current year=2020, output=2029-01-01 00:00:00
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
 * - For string to datetime conversion, conflicts with DDD, MONTH, MON.
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
 * AM|A.M.|PM|P.M.
 * Meridiem indicator (or AM/PM)
 * - Datetime to string conversion:
 *   - AM and PM mean the exact same thing in the pattern.
 *     e.g. input=2019-01-01 20:00, format=“AM”, output=“PM”.
 *   - Retains the exact format (capitalization and length) provided in the pattern string. If p.m.
 *     is in the pattern, we expect a.m. or p.m. in the output; if AM is in the pattern, we expect
 *     AM or PM in the output. If the case is mixed (Am or aM) then the output case will match the
 *     case of the pattern's first character (Am => AM, aM => am).
 * - String to datetime conversion:
 *   - Conflicts with HH24 and SSSSS.
 *   - It doesn't matter which meridian indicator is in the pattern.
 *     E.g. input="2019-01-01 11:00 p.m.", pattern="YYYY-MM-DD HH12:MI AM",
 *          output=2019-01-01 23:00:00
 *   - If FX is enabled, input length has to match the pattern's length. e.g. pattern=AM input=A.M.
 *     is not accepted, but input=pm is.
 * - Not listed as a character temporal because of special status: does not get padded with spaces
 *   upon formatting, and case is handled differently at datetime to string conversion.
 *
 * D
 * Day of week (1-7)
 * - 1 means Sunday, 2 means Monday, and so on.
 * - Not allowed in string to datetime conversion.
 *
 * Q
 * Quarter of year (1-4)
 * - Not allowed in string to datetime conversion.
 *
 * WW
 * Aligned week of year (1-53)
 * - 1st week begins on January 1st and ends on January 7th, and so on.
 * - Not allowed in string to datetime conversion.
 *
 * W
 * Aligned week of month (1-5)
 * - 1st week starts on the 1st of the month and ends on the 7th, and so on.
 * - Not allowed in string to datetime conversion.
 *
 * IYYY
 * 4-digit ISO 8601 week-numbering year
 * - Returns the year relating to the ISO week number (IW), which is the full week (Monday to
 *   Sunday) which contains January 4 of the Gregorian year.
 * - Behaves similarly to YYYY in that for datetime to string conversion, prefix digits for 1, 2,
 *   and 3-digit inputs are obtained from current week-numbering year.
 * - For string to datetime conversion, requires IW and ID|DAY|DY. Conflicts with all other date
 *   patterns (see "List of Date-Based Patterns").
 *
 * IYY
 * Last 3 digits of ISO 8601 week-numbering year
 * - See IYYY.
 * - Behaves similarly to YYY in that for datetime to string conversion, prefix digit is obtained
 *   from current week-numbering year and can accept 1 or 2-digit input.
 *
 * IY
 * Last 2 digits of ISO 8601 week-numbering year
 * - See IYYY.
 * - Behaves similarly to YY in that for datetime to string conversion, prefix digits are obtained
 *   from current week-numbering year and can accept 1-digit input.
 *
 * I
 * Last digit of ISO 8601 week-numbering year
 * - See IYYY.
 * - Behaves similarly to Y in that for datetime to string conversion, prefix digits are obtained
 *   from current week-numbering year.
 *
 * IW
 * ISO 8601 week of year (1-53)
 * - Begins on the Monday closest to January 1 of the year.
 * - For string to datetime conversion, if the input week does not exist in the input year, an
 *   error will be thrown. e.g. the 2019 week-year has 52 weeks; with pattern="iyyy-iw-id"
 *   input="2019-53-2" is not accepted.
 * - For string to datetime conversion, requires IYYY|IYY|IY|I and ID|DAY|DY. Conflicts with all other
 *   date patterns (see "List of Date-Based Patterns").
 *
 * ID
 * ISO 8601 day of week (1-7)
 * - 1 is Monday, and so on.
 * - For string to datetime conversion, requires IYYY|IYY|IY|I and IW. Conflicts with all other
 *   date patterns (see "List of Date-Based Patterns").
 *
 * A.2. Character temporals
 * Temporal elements, but spelled out.
 * - For datetime to string conversion, the pattern's case must match one of the listed formats
 *   (e.g. mOnTh is not accepted) to avoid ambiguity. Output is right padded with trailing spaces
 *   unless the pattern is marked with the fill mode modifier (FM).
 * - For string to datetime conversion, the case of the pattern does not matter.
 *
 * MONTH|Month|month
 * Name of month of year
 * - For datetime to string conversion, will include trailing spaces up to length 9 (length of
 *   longest month of year name: "September"). Case is taken into account according to the
 *   following example (pattern => output):
 *   - MONTH => JANUARY
 *   - Month => January
 *   - month => january
 * - For string to datetime conversion, neither the case of the pattern nor the case of the input
 *   are taken into account.
 * - For string to datetime conversion, conflicts with MM and MON.
 *
 *
 * MON|Mon|mon
 * Abbreviated name of month of year
 * - For datetime to string conversion, case is taken into account according to the following
 *   example (pattern => output):
 *   - MON => JAN
 *   - Mon => Jan
 *   - mon => jan
 * - For string to datetime conversion, neither the case of the pattern nor the case of the input
 *   are taken into account.
 * - For string to datetime conversion, conflicts with MM and MONTH.
 *
 *
 * DAY|Day|day
 * Name of day of week
 * - For datetime to string conversion, will include trailing spaces until length is 9 (length of
 *   longest day of week name: "Wednesday"). Case is taken into account according to the following
 *   example (pattern => output):
 *   - DAY = SUNDAY
 *   - Day = Sunday
 *   - day = sunday
 * - For string to datetime conversion, neither the case of the pattern nor the case of the input
 *   are taken into account.
 * - Not allowed in string to datetime conversion except with IYYY|IYY|IY|I and IW.
 *
 * DY|Dy|dy
 * Abbreviated name of day of week
 * - For datetime to string conversion, case is taken into account according to the following
 *   example (pattern => output):
 *   - DY = SUN
 *   - Dy = Sun
 *   - dy = sun
 * - For string to datetime conversion, neither the case of the pattern nor the case of the input
 *   are taken into account.
 * - Not allowed in string to datetime conversion except with IYYY|IYY|IY|I and IW.
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
 * - If the whole pattern string is delimited by single quotes (''), then the apostrophe separator
 *   (') must be escaped with a single backslash: (\').
 *
 * D. ISO 8601 delimiters
 * T|Z
 * ISO 8601 delimiter
 * - Serves as a delimiter.
 * - Function is to support formats like “YYYY-MM-DDTHH24:MI:SS.FF9Z”, “YYYY-MM-DD-HH24:MI:SSZ”
 * - For datetime to string conversion, output is always capitalized ("T"), even if lowercase ("t")
 *   is provided in the pattern.
 * - For string to datetime conversion, case of input and pattern may differ.
 *
 * E. Nested strings (Text)
 * – Surround with double quotes (") in the pattern. Note, if the whole pattern string is delimited
 *   by double quotes, then the double quotes must be escaped with a single backslash: (\").
 * - In order to include a literal double quote character within the nested string, the double
 *   quote character must be escaped with a double backslash: (\\”). If the whole pattern string is
 *   delimited by double quotes, then escape with a triple backslash: (\\\")
 * - If the whole pattern string is delimited by single quotes, literal single
 *   quotes/apostrophes (') in the nested string must be escaped with a single backslash: (\')
 * - For datetime to string conversion, we simply include the string in the output, preserving the
 *   characters' case.
 * - For string to datetime conversion, the information is lost as the nested string won’t be part
 *   of the resulting datetime object. However, the nested string has to match the related part of
 *   the input string, except case may differ.
 *
 * F. Format modifier tokens
 * FM
 * Fill mode modifier
 * - Default for string to datetime conversion. Inputs of fewer digits than expected are accepted
 *   if followed by a delimiter:
 *   e.g. format="YYYY-MM-DD", input="19-1-1", output=2019-01-01 00:00:00
 * - For datetime to string conversion, padding (trailing spaces for text data and leading zeroes
 *   for numeric data) is omitted for the temporal element immediately following an "FM" in the
 *   pattern string. If the element following is not a temporal element (for example, if "FM"
 *   precedes a separator), an error will be thrown.
 *   e.g. pattern=FMHH12:MI:FMSS, input=2019-01-01 01:01:01, output=1:01:1
 * - Modifies FX so that lack of leading zeroes are accepted for the element immediately following
 *   an "FM" in the pattern string.
 *
 * FX
 * Format exact modifier
 * - Default for datetime to string conversion. Numeric output is left padded with zeros, and
 *   non-numeric output except for AM/PM is right padded with spaces up to expected length.
 * - Applies to the whole pattern.
 * - Rules applied at string to datetime conversion:
 *   - Separators must match exactly, down to the character.
 *   - Numeric input can't omit leading zeroes. This rule does not apply to elements (tokens)
 *     immediately preceded by an "FM."
 *   - AM/PM input length has to match the pattern's length. e.g. pattern=AM input=A.M. is not
 *     accepted, but input=pm is.
 *
 * Appendix:
 * List of Date-Based Patterns
 * These are patterns that help define a date as opposed to a time.
 * YYYY, YYY, YY, Y, RRRR, RR,
 * MM, MON, MONTH,
 * DD, DDD, D, DY, DAY,
 * Q, WW, W,
 * IYYY, IYY, IY, I, IW, ID
 */

public class HiveSqlDateTimeFormatter implements Serializable {

  private static final int LONGEST_TOKEN_LENGTH = 5;
  private static final int LONGEST_ACCEPTED_PATTERN = 100; // for sanity's sake
  private static final int NANOS_MAX_LENGTH = 9;
  public static final int AM = 0;
  public static final int PM = 1;
  private static final DateTimeFormatter MONTH_FORMATTER = DateTimeFormatter.ofPattern("MMM");
  public static final DateTimeFormatter DAY_OF_WEEK_FORMATTER = DateTimeFormatter.ofPattern("EEE");
  private String pattern;
  private List<Token> tokens = new ArrayList<>();
  private boolean formatExact = false;

  private static final Map<String, TemporalField> NUMERIC_TEMPORAL_TOKENS =
      ImmutableMap.<String, TemporalField>builder()
          .put("yyyy", ChronoField.YEAR).put("yyy", ChronoField.YEAR)
          .put("yy", ChronoField.YEAR).put("y", ChronoField.YEAR)
          .put("rrrr", ChronoField.YEAR).put("rr", ChronoField.YEAR)
          .put("mm", ChronoField.MONTH_OF_YEAR)
          .put("d", WeekFields.SUNDAY_START.dayOfWeek())
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
          .put("ww", ChronoField.ALIGNED_WEEK_OF_YEAR).put("w", ChronoField.ALIGNED_WEEK_OF_MONTH)
          .put("q", IsoFields.QUARTER_OF_YEAR)
          .put("iyyy", IsoFields.WEEK_BASED_YEAR).put("iyy", IsoFields.WEEK_BASED_YEAR)
          .put("iy", IsoFields.WEEK_BASED_YEAR).put("i", IsoFields.WEEK_BASED_YEAR)
          .put("iw", IsoFields.WEEK_OF_WEEK_BASED_YEAR).put("id", ChronoField.DAY_OF_WEEK)
          .build();

  private static final Map<String, TemporalField> CHARACTER_TEMPORAL_TOKENS =
      ImmutableMap.<String, TemporalField>builder()
          .put("mon", ChronoField.MONTH_OF_YEAR)
          .put("month", ChronoField.MONTH_OF_YEAR)
          .put("day", ChronoField.DAY_OF_WEEK)
          .put("dy", ChronoField.DAY_OF_WEEK)
      .build();

  private static final Map<String, TemporalUnit> TIME_ZONE_TOKENS =
      ImmutableMap.<String, TemporalUnit>builder()
          .put("tzh", ChronoUnit.HOURS).put("tzm", ChronoUnit.MINUTES).build();

  private static final List<String> VALID_ISO_8601_DELIMITERS =
      ImmutableList.of("t", "z");

  private static final List<String> VALID_SEPARATORS =
      ImmutableList.of("-", ":", " ", ".", "/", ";", "\'", ",");

  private static final List<String> VALID_FORMAT_MODIFIERS =
      ImmutableList.of("fm", "fx");

  private static final Map<String, Integer> SPECIAL_LENGTHS = ImmutableMap.<String, Integer>builder()
      .put("hh12", 2).put("hh24", 2).put("tzm", 2).put("am", 4).put("pm", 4)
      .put("ff1", 1).put("ff2", 2).put("ff3", 3).put("ff4", 4).put("ff5", 5)
      .put("ff6", 6).put("ff7", 7).put("ff8", 8).put("ff9", 9).put("ff", 9)
      .put("month", 9).put("day", 9).put("dy", 3)
      .build();

  private static final List<TemporalField> ISO_8601_TEMPORAL_FIELDS =
      ImmutableList.of(ChronoField.DAY_OF_WEEK,
          IsoFields.WEEK_OF_WEEK_BASED_YEAR,
          IsoFields.WEEK_BASED_YEAR);

  /**
   * Represents broad categories of tokens.
   */
  public enum TokenType {
    NUMERIC_TEMPORAL,
    CHARACTER_TEMPORAL,
    SEPARATOR,
    TIMEZONE,
    ISO_8601_DELIMITER,
    TEXT
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
    boolean fillMode; //FM, applies to type TEMPORAL only (later should apply to TIMEZONE as well)

    public Token(TokenType tokenType, TemporalField temporalField, String string, int length,
        boolean fillMode) {
      this(tokenType, temporalField, null, string, length, fillMode);
    }

    public Token(TemporalUnit temporalUnit, String string, int length, boolean fillMode) {
      this(TokenType.TIMEZONE, null, temporalUnit, string, length, fillMode);
    }

    public Token(TokenType tokenType, String string) {
      this(tokenType, null, null, string, string.length(), false);
    }

    public Token(TokenType tokenType, TemporalField temporalField, TemporalUnit temporalUnit,
        String string, int length, boolean fillMode) {
      this.type = tokenType;
      this.temporalField = temporalField;
      this.temporalUnit = temporalUnit;
      this.string = string;
      this.length = length;
      this.fillMode = fillMode;
    }

    @Override public String toString() {
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

    public void removeBackslashes() {
      string = string.replaceAll("\\\\", "");
      length = string.length();
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
  private void parsePatternToTokens(String pattern) {
    tokens.clear();
    String originalPattern = pattern;
    pattern = pattern.toLowerCase();

    // indexes of the substring we will check (includes begin, does not include end)
    int begin=0, end=0;
    String candidate;
    Token lastAddedToken = null;
    boolean fillMode = false;

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
          lastAddedToken = parseSeparatorToken(candidate, lastAddedToken, fillMode, begin);
          begin = end;
          break;
        }
        if (isIso8601Delimiter(candidate)) {
          lastAddedToken = parseIso8601DelimiterToken(candidate, fillMode, begin);
          begin = end;
          break;
        }
        if (isNumericTemporalToken(candidate)) {
          lastAddedToken = parseTemporalToken(originalPattern, candidate, fillMode, begin);
          fillMode = false;
          begin = end;
          break;
        }
        if (isCharacterTemporalToken(candidate)) {
          lastAddedToken = parseCharacterTemporalToken(originalPattern, candidate, fillMode, begin);
          fillMode = false;
          begin = end;
          break;
        }
        if (isTimeZoneToken(candidate)) {
          lastAddedToken = parseTimeZoneToken(candidate, fillMode, begin);
          begin = end;
          break;
        }
        if (isTextToken(candidate)) {
          lastAddedToken = parseTextToken(originalPattern, fillMode, begin);
          end = begin + lastAddedToken.length + 2; // skip 2 quotation marks
          lastAddedToken.removeBackslashes();
          begin = end;
          break;
        }
        if (isFormatModifierToken(candidate)) {
          checkFillModeOff(fillMode, begin);
          fillMode = isFm(candidate);
          if (!fillMode) {
            formatExact = true;
          }
          begin = end;
          break;
        }
      }
    }
  }

  private boolean isSeparator(String candidate) {
    return candidate.length() == 1 && VALID_SEPARATORS.contains(candidate);
  }

  private boolean isIso8601Delimiter(String candidate) {
    return candidate.length() == 1 && VALID_ISO_8601_DELIMITERS.contains(candidate);
  }

  private boolean isNumericTemporalToken(String candidate) {
    return NUMERIC_TEMPORAL_TOKENS.containsKey(candidate);
  }

  private boolean isCharacterTemporalToken(String candidate) {
    return CHARACTER_TEMPORAL_TOKENS.containsKey(candidate);
  }

  private boolean isTimeZoneToken(String pattern) {
    return TIME_ZONE_TOKENS.containsKey(pattern);
  }

  private boolean isTextToken(String candidate) {
    return candidate.startsWith("\"");
  }

  private boolean isFormatModifierToken(String candidate) {
    return candidate.length() == 2 && VALID_FORMAT_MODIFIERS.contains(candidate);
  }

  private boolean isFm(String candidate) {
    return "fm".equals(candidate);
  }

  private Token parseSeparatorToken(String candidate, Token lastAddedToken, boolean fillMode,
      int begin) {
    checkFillModeOff(fillMode, begin);
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

  private Token parseIso8601DelimiterToken(String candidate, boolean fillMode, int begin) {
    checkFillModeOff(fillMode, begin);
    Token lastAddedToken;
    lastAddedToken = new Token(TokenType.ISO_8601_DELIMITER, candidate.toUpperCase());
    tokens.add(lastAddedToken);
    return lastAddedToken;
  }

  private Token parseTemporalToken(String originalPattern, String candidate, boolean fillMode,
      int begin) {
    // for AM/PM, keep original case
    if (NUMERIC_TEMPORAL_TOKENS.get(candidate) == ChronoField.AMPM_OF_DAY) {
      int subStringEnd = begin + candidate.length();
      candidate = originalPattern.substring(begin, subStringEnd);
    }
    Token lastAddedToken = new Token(TokenType.NUMERIC_TEMPORAL,
        NUMERIC_TEMPORAL_TOKENS.get(candidate.toLowerCase()), candidate,
        getTokenStringLength(candidate), fillMode);
    tokens.add(lastAddedToken);
    return lastAddedToken;
  }

  private Token parseCharacterTemporalToken(String originalPattern, String candidate,
      boolean fillMode, int begin) {
    // keep original case
    candidate = originalPattern.substring(begin, begin + candidate.length());

    Token lastAddedToken = new Token(TokenType.CHARACTER_TEMPORAL,
        CHARACTER_TEMPORAL_TOKENS.get(candidate.toLowerCase()), candidate,
        getTokenStringLength(candidate), fillMode);
    tokens.add(lastAddedToken);
    return lastAddedToken;
  }

  private Token parseTimeZoneToken(String candidate, boolean fillMode, int begin) {
    checkFillModeOff(fillMode, begin);
    Token lastAddedToken = new Token(TIME_ZONE_TOKENS.get(candidate), candidate,
        getTokenStringLength(candidate), false);
    tokens.add(lastAddedToken);
    return lastAddedToken;
  }

  private Token parseTextToken(String fullPattern, boolean fillMode, int begin) {
    checkFillModeOff(fillMode, begin);
    int end = begin;
    do {
      end = fullPattern.indexOf('\"', end + 1);
      if (end == -1) {
        throw new IllegalArgumentException(
            "Missing closing double quote (\") opened at index " + begin);
      }
    // if double quote is escaped with a backslash, keep looking for the closing quotation mark
    } while ("\\".equals(fullPattern.substring(end - 1, end)));
    Token lastAddedToken = new Token(TokenType.TEXT, fullPattern.substring(begin + 1, end));
    tokens.add(lastAddedToken);
    return lastAddedToken;
  }

  private void checkFillModeOff(boolean fillMode, int index) {
    if (fillMode) {
      throw new IllegalArgumentException("Bad date/time conversion pattern: " + pattern +
          ". Error at index " + index + ": Fill mode modifier (FM) must "
          + "be followed by a temporal token.");
    }
  }

  private int getTokenStringLength(String candidate) {
    Integer length = SPECIAL_LENGTHS.get(candidate.toLowerCase());
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
    boolean containsIsoFields=false, containsGregorianFields=false;
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
        if (token.temporalField.isDateBased() && token.temporalField != ChronoField.DAY_OF_WEEK) {
          if (ISO_8601_TEMPORAL_FIELDS.contains(token.temporalField)) {
            containsIsoFields = true;
          } else {
            containsGregorianFields = true;
          }
        }
      } else if (token.temporalUnit != null) {
        timeZoneTemporalUnits.add(token.temporalUnit);
      }
    }

    //check for illegal temporal fields
    if (temporalFields.contains(IsoFields.QUARTER_OF_YEAR)) {
      throw new IllegalArgumentException("Illegal field: q (" + IsoFields.QUARTER_OF_YEAR + ")");
    }
    if (temporalFields.contains(WeekFields.SUNDAY_START.dayOfWeek())) {
      throw new IllegalArgumentException("Illegal field: d (" + WeekFields.SUNDAY_START.dayOfWeek() + ")");
    }
    if (temporalFields.contains(ChronoField.DAY_OF_WEEK) && containsGregorianFields) {
      throw new IllegalArgumentException("Illegal field: dy/day (" + ChronoField.DAY_OF_WEEK + ")");
    }
    if (temporalFields.contains(ChronoField.ALIGNED_WEEK_OF_MONTH)) {
      throw new IllegalArgumentException("Illegal field: w (" + ChronoField.ALIGNED_WEEK_OF_MONTH + ")");
    }
    if (temporalFields.contains(ChronoField.ALIGNED_WEEK_OF_YEAR)) {
      throw new IllegalArgumentException("Illegal field: ww (" + ChronoField.ALIGNED_WEEK_OF_YEAR + ")");
    }

    if (containsGregorianFields && containsIsoFields) {
      throw new IllegalArgumentException("Pattern cannot contain both ISO and Gregorian tokens");
    }
    if (!(temporalFields.contains(ChronoField.YEAR)
        || temporalFields.contains(IsoFields.WEEK_BASED_YEAR))) {
      throw new IllegalArgumentException("Missing year token.");
    }
    if (containsGregorianFields &&
        !(temporalFields.contains(ChronoField.MONTH_OF_YEAR) &&
            temporalFields.contains(ChronoField.DAY_OF_MONTH) ||
            temporalFields.contains(ChronoField.DAY_OF_YEAR))) {
      throw new IllegalArgumentException("Missing day of year or (month of year + day of month)"
          + " tokens.");
    }
    if (containsIsoFields &&
        !(temporalFields.contains(IsoFields.WEEK_OF_WEEK_BASED_YEAR) &&
            temporalFields.contains(ChronoField.DAY_OF_WEEK))) {
      throw new IllegalArgumentException("Missing week of year (iw) or day of week (id) tokens.");
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
      if (token.type == TokenType.CHARACTER_TEMPORAL) {
        String s = token.string;
        if (!(s.equals(s.toUpperCase()) || s.equals(capitalize(s)) || s.equals(s.toLowerCase()))) {
          throw new IllegalArgumentException(
              "Ambiguous capitalization of token " + s + ". Accepted " + "forms are " + s
                  .toUpperCase() + ", " + capitalize(s) + ", or " + s.toLowerCase() + ".");
        }
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
      case NUMERIC_TEMPORAL:
      case CHARACTER_TEMPORAL:
        try {
          value = localDateTime.get(token.temporalField);
          if (token.type == TokenType.NUMERIC_TEMPORAL) {
            outputString = formatNumericTemporal(value, token);
          } else {
            outputString = formatCharacterTemporal(value, token);
          }
        } catch (DateTimeException e) {
          throw new IllegalArgumentException(token.temporalField + " couldn't be obtained from "
              + "LocalDateTime " + localDateTime, e);
        }
        break;
      case TIMEZONE: //invalid for timestamp and date
        throw new IllegalArgumentException(token.string.toUpperCase() + " not a valid format for "
            + "timestamp or date.");
      case SEPARATOR:
      case TEXT:
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

  private String formatNumericTemporal(int value, Token token) {
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

  private String formatCharacterTemporal(int value, Token token) {
    String output = null;
    if (token.temporalField == ChronoField.MONTH_OF_YEAR) {
      output = Month.of(value).getDisplayName(TextStyle.FULL, Locale.US);
    } else if (token.temporalField == ChronoField.DAY_OF_WEEK) {
      output = DayOfWeek.of(value).getDisplayName(TextStyle.FULL, Locale.US);
    }
    if (output == null) {
      throw new IllegalStateException("TemporalField: " + token.temporalField + " not valid for "
          + "character formatting.");
    }

    // set length
    if (output.length() > token.length) {
      output = output.substring(0, token.length); // truncate to length
    } else if (!token.fillMode && output.length() < token.length) {
      output = StringUtils.rightPad(output, token.length); //pad to size
    }

    // set case
    if (Character.isUpperCase(token.string.charAt(1))) {
      output = output.toUpperCase();
    } else if (Character.isLowerCase(token.string.charAt(0))) {
      output = output.toLowerCase();
    }
    return output;
  }

  /**
   * To match token.length, pad left with zeroes or truncate.
   * Omit padding if fill mode (FM) modifier on.
   */
  private String padOrTruncateNumericTemporal(Token token, String output) {
    //exception
    if (token.temporalField == ChronoField.NANO_OF_SECOND) {
      output = StringUtils.leftPad(output, 9, '0'); // pad left to length 9
      if (output.length() > token.length) {
        output = output.substring(0, token.length); // truncate right to size
      }
      if (token.string.equalsIgnoreCase("ff")) {
        output = output.replaceAll("0*$", ""); //truncate trailing 0's
      }

      // the rule
    } else {
      if (output.length() < token.length && !token.fillMode) {
        output = StringUtils.leftPad(output, token.length, '0'); // pad left
      } else if (output.length() > token.length) {
        output = output.substring(output.length() - token.length); // truncate left
      }
      if (token.fillMode) {
        output = output.replaceAll("^0*", ""); //truncate leading 0's
      }
    }
    if (output.isEmpty()) {
      output = "0";
    }
    return output;
  }

  public Timestamp parseTimestamp(String fullInput){
    LocalDateTime ldt = LocalDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
    String substring;
    int index = 0;
    int value;
    int timeZoneSign = 0, timeZoneHours = 0, timeZoneMinutes = 0;
    int iyyy = 0, iw = 0;

    for (Token token : tokens) {
      switch (token.type) {
      case NUMERIC_TEMPORAL:
      case CHARACTER_TEMPORAL:
        if (token.type == TokenType.NUMERIC_TEMPORAL) {
          substring = getNextNumericSubstring(fullInput, index, token); // e.g. yy-m -> yy
          value = parseNumericTemporal(substring, token); // e.g. 18->2018
        } else {
          substring = getNextCharacterSubstring(fullInput, index, token); //e.g. Marcharch -> March
          value = parseCharacterTemporal(substring, token); // e.g. July->07
        }
        try {
          ldt = ldt.with(token.temporalField, value);
        } catch (DateTimeException e){
          throw new IllegalArgumentException(
              "Value " + value + " not valid for token " + token.toString());
        }

        //update IYYY and IW if necessary
        if (token.temporalField == IsoFields.WEEK_BASED_YEAR) {
          iyyy = value;
        }
        if (token.temporalField == IsoFields.WEEK_OF_WEEK_BASED_YEAR) {
          iw = value;
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
          substring = getNextNumericSubstring(fullInput, index, index + 2, token);
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
          substring = getNextNumericSubstring(fullInput, index, token);
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
      case TEXT:
        index = parseText(fullInput, index, token);
      default:
        //do nothing
      }
    }

    // anything left unparsed at end of string? throw error
    if (!fullInput.substring(index).isEmpty()) {
      throw new IllegalArgumentException("Leftover input after parsing: " +
          fullInput.substring(index) + " in string " + fullInput);
    }
    checkForInvalidIsoWeek(iyyy, iw);

    return Timestamp.ofEpochSecond(ldt.toEpochSecond(ZoneOffset.UTC), ldt.getNano());
  }

  /**
   * Check for WEEK_OF_WEEK_BASED_YEAR (iw) value 53 when WEEK_BASED_YEAR (iyyy) does not have 53
   * weeks.
   */
  private void checkForInvalidIsoWeek(int iyyy, int iw) {
    if (iyyy == 0) {
      return;
    }

    LocalDateTime ldt = LocalDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
    ldt = ldt.with(IsoFields.WEEK_BASED_YEAR, iyyy);
    ldt = ldt.with(IsoFields.WEEK_OF_WEEK_BASED_YEAR, iw);
    if (ldt.getYear() != iyyy) {
      throw new IllegalArgumentException("ISO year " + iyyy + " does not have " + iw + " weeks.");
    }
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
  private String getNextNumericSubstring(String s, int begin, Token token) {
    return getNextNumericSubstring(s, begin, begin + token.length, token);
  }

  private String getNextNumericSubstring(String s, int begin, int end, Token token) {
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
    // if it's a character temporal, the first non-letter character is a delimiter
    if (token.type == TokenType.CHARACTER_TEMPORAL && s.matches(".*[^A-Za-z].*")) {
      s = s.split("[^A-Za-z]", 2)[0];

    // if it's a numeric element, next non-numeric character is a delimiter. Don't worry about
    // AM/PM since we've already handled that case.
    } else if ((token.type == TokenType.NUMERIC_TEMPORAL || token.type == TokenType.TIMEZONE)
        && s.matches(".*\\D.*")) {
      s = s.split("\\D", 2)[0];
    }

    return s;
  }

  /**
   * Get the integer value of a temporal substring.
   */
  private int parseNumericTemporal(String substring, Token token){
    checkFormatExact(substring, token);

    // exceptions to the rule
    if (token.temporalField == ChronoField.AMPM_OF_DAY) {
      return substring.toLowerCase().startsWith("a") ? AM : PM;

    } else if (token.temporalField == ChronoField.HOUR_OF_AMPM && "12".equals(substring)) {
      substring = "0";

    } else if (token.temporalField == ChronoField.YEAR
        || token.temporalField == IsoFields.WEEK_BASED_YEAR) {

      String currentYearString;
      if (token.temporalField == ChronoField.YEAR) {
        currentYearString = String.valueOf(LocalDateTime.now().getYear());
      } else {
        currentYearString =  String.valueOf(LocalDateTime.now().get(IsoFields.WEEK_BASED_YEAR));
      }

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

  private static final String MONTH_REGEX;
  private static final String DAY_OF_WEEK_REGEX;
  static {
    StringBuilder sb = new StringBuilder();
    String or = "";
    for (Month month : Month.values()) {
      sb.append(or).append(month);
      or = "|";
    }
    MONTH_REGEX = sb.toString();
    sb = new StringBuilder();
    or = "";
    for (DayOfWeek dayOfWeek : DayOfWeek.values()) {
      sb.append(or).append(dayOfWeek);
      or = "|";
    }
    DAY_OF_WEEK_REGEX = sb.toString();
  }

  private String getNextCharacterSubstring(String fullInput, int index, Token token) {
    int end = index + token.length;
    if (end > fullInput.length()) {
      end = fullInput.length();
    }
    String substring = fullInput.substring(index, end);
    if (token.length == 3) { //dy, mon
      return substring;
    }

    // patterns day, month
    String regex;
    if (token.temporalField == ChronoField.MONTH_OF_YEAR) {
      regex = MONTH_REGEX;
    } else if (token.temporalField == ChronoField.DAY_OF_WEEK) {
      regex = DAY_OF_WEEK_REGEX;
    } else {
      throw new IllegalArgumentException("Error at index " + index + ": " + token + " not a "
          + "character temporal with length not 3");
    }
    Matcher matcher = Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(substring);
    if (matcher.find()) {
      return substring.substring(0, matcher.end());
    }
    throw new IllegalArgumentException(
        "Couldn't find " + token.string + " in substring " + substring + " at index " + index);
  }

  private int parseCharacterTemporal(String substring, Token token) {
    try {
      if (token.temporalField == ChronoField.MONTH_OF_YEAR) {
        if (token.length == 3) {
          return Month.from(MONTH_FORMATTER.parse(capitalize(substring))).getValue();
        } else {
          return Month.valueOf(substring.toUpperCase()).getValue();
        }
      } else if (token.temporalField == ChronoField.DAY_OF_WEEK) {
        if (token.length == 3) {
          return DayOfWeek.from(DAY_OF_WEEK_FORMATTER.parse(capitalize(substring))).getValue();
        } else {
          return DayOfWeek.valueOf(substring.toUpperCase()).getValue();
        }
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Couldn't parse substring \"" + substring + "\" with token " + token + " to integer."
              + "Pattern is " + pattern, e);
    }
    throw new IllegalArgumentException(
        "token: (" + token + ") isn't a valid character temporal. Pattern is " + pattern);
  }

  /**
   * @throws IllegalArgumentException if input length doesn't match expected (token) length
   */
  private void checkFormatExact(String substring, Token token) {
    // AM/PM defaults to length 4 but make it 2 for FX check if the pattern actually has length 2
    if (formatExact && token.temporalField == ChronoField.AMPM_OF_DAY) {
      token.length = token.string.length();
    }
    if (formatExact
        && !(token.fillMode || token.temporalField == ChronoField.NANO_OF_SECOND)
        && token.length != substring.length()) {
      throw new IllegalArgumentException(
          "FX on and expected token length " + token.length + " for token " + token.toString()
              + " does not match substring (" + substring + ") length " + substring.length());
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
   * @throws IllegalArgumentException if separator is missing or if FX is on and separator doesn't
   * match the expected separator pattern exactly
   */
  private int parseSeparator(String fullInput, int index, Token token) {
    int begin = index;
    String s;
    StringBuilder separatorsFound = new StringBuilder();

    while (index < fullInput.length() &&
        VALID_SEPARATORS.contains(fullInput.substring(index, index + 1))) {
      s = fullInput.substring(index, index + 1);
      if (!isLastCharacterOfSeparator(index, fullInput)
          || !("-".equals(s) && (nextTokenIs("tzh", token)))
          || separatorsFound.length() == 0) {
        separatorsFound.append(s);
      }
      index++;
    }

    if (separatorsFound.length() == 0) {
      throw new IllegalArgumentException("Missing separator at index " + index);
    }
    if (formatExact && !token.string.equals(separatorsFound.toString())) {
      throw new IllegalArgumentException("FX on and separator found: " + separatorsFound.toString()
          + " doesn't match expected separator: " + token.string);
    }

    return begin + separatorsFound.length();
  }

  private int parseText(String fullInput, int index, Token token) {
    String substring;
    substring = fullInput.substring(index, index + token.length);
    if (!token.string.equalsIgnoreCase(substring)) {
      throw new IllegalArgumentException(
          "Wrong input at index " + index + ": Expected: \"" + token.string + "\" but got: \""
              + substring + "\" for token: " + token);
    }
    return index + token.length;
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
        || isNumericTemporalToken(pattern) && NUMERIC_TEMPORAL_TOKENS.get(pattern) == nextToken.temporalField
        || isCharacterTemporalToken(pattern) && CHARACTER_TEMPORAL_TOKENS.get(pattern) == nextToken.temporalField);
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

  private static String capitalize(String substring) {
    return WordUtils.capitalize(substring.toLowerCase());
  }
}
