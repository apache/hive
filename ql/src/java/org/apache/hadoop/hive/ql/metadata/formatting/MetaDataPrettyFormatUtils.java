/**
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

package org.apache.hadoop.hive.ql.metadata.formatting;

import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

/**
 * This class provides methods to format the output of DESCRIBE PRETTY
 * in a human-readable way.
 */
public final class MetaDataPrettyFormatUtils {

  public static final int PRETTY_MAX_INTERCOL_SPACING = 4;
  private static final int PRETTY_ALIGNMENT = 10;
  /**
   * Minimum length of the comment column. This is relevant only when the terminal width
   * or hive.cli.pretty.output.num.cols is too small, or when there are very large column
   * names.
   * 10 was arbitrarily chosen.
   */
  private static final int MIN_COMMENT_COLUMN_LEN = 10;

  private MetaDataPrettyFormatUtils() {
  }

  /**
   * @param prettyOutputNumCols The pretty output is formatted to fit within
   * these many columns.
   */
  public static String getAllColumnsInformation(List<FieldSchema> cols,
      List<FieldSchema> partCols, int prettyOutputNumCols) {
    StringBuilder columnInformation = new StringBuilder(
        MetaDataFormatUtils.DEFAULT_STRINGBUILDER_SIZE);
    int maxColNameLen = findMaxColumnNameLen(cols);
    formatColumnsHeaderPretty(columnInformation, maxColNameLen, prettyOutputNumCols);
    formatAllFieldsPretty(columnInformation, cols, maxColNameLen, prettyOutputNumCols);

    if ((partCols != null) && (!partCols.isEmpty())) {
      columnInformation.append(MetaDataFormatUtils.LINE_DELIM)
                        .append("# Partition Information")
                        .append(MetaDataFormatUtils.LINE_DELIM);
      formatColumnsHeaderPretty(columnInformation, maxColNameLen, prettyOutputNumCols);
      formatAllFieldsPretty(columnInformation, partCols, maxColNameLen, prettyOutputNumCols);
    }

    return columnInformation.toString();
  }

  /**
   * Find the length of the largest column name.
   */
  private static int findMaxColumnNameLen(List<FieldSchema> cols) {
    int maxLen = -1;
    for (FieldSchema col : cols) {
      int colNameLen = col.getName().length();
      if (colNameLen > maxLen) {
        maxLen = colNameLen;
      }
    }
    return maxLen;
  }

  /**
   * @param maxColNameLen The length of the largest column name
   */
  private static void formatColumnsHeaderPretty(StringBuilder columnInformation,
      int maxColNameLen, int prettyOutputNumCols) {
    String columnHeaders[] = MetaDataFormatUtils.getColumnsHeader();
    formatOutputPretty(columnHeaders[0], columnHeaders[1], columnHeaders[2],
                        columnInformation, maxColNameLen, prettyOutputNumCols);
    columnInformation.append(MetaDataFormatUtils.LINE_DELIM);
  }

  private static void formatAllFieldsPretty(StringBuilder tableInfo,
      List<FieldSchema> cols, int maxColNameLen, int prettyOutputNumCols) {
    for (FieldSchema col : cols) {
      formatOutputPretty(col.getName(), col.getType(),
          MetaDataFormatUtils.getComment(col), tableInfo, maxColNameLen,
          prettyOutputNumCols);
    }
  }

  /**
   * If the specified comment is too long, add line breaks at appropriate
   * locations.  Note that the comment may already include line-breaks
   * specified by the user at table creation time.
   * @param columnsAlreadyConsumed The number of columns on the current line
   * that have already been consumed by the column name, column type and
   * and the surrounding delimiters.
   * @return The comment with line breaks added at appropriate locations.
   */
  private static String breakCommentIntoMultipleLines(String comment,
      int columnsAlreadyConsumed, int prettyOutputNumCols) {

    if (prettyOutputNumCols == -1) {
      // XXX fixed to 80 to remove jline dep
      prettyOutputNumCols = 80 - 1;
    }

    int commentNumCols = prettyOutputNumCols - columnsAlreadyConsumed;
    if (commentNumCols < MIN_COMMENT_COLUMN_LEN) {
      commentNumCols = MIN_COMMENT_COLUMN_LEN;
    }

    // Track the number of columns allocated for the comment that have
    // already been consumed on the current line.
    int commentNumColsConsumed = 0;

    StringTokenizer st = new StringTokenizer(comment, " \t\n\r\f", true);
    // We use a StringTokenizer instead of a BreakIterator, because
    // table comments often contain text that looks like code. For eg:
    // 'Type0' => 0, // This is Type 0
    // 'Type1' => 1, // This is Type 1
    // BreakIterator is meant for regular text, and was found to give
    // bad line breaks when we tried it out.

    StringBuilder commentBuilder = new StringBuilder(comment.length());
    while (st.hasMoreTokens()) {
      String currWord = st.nextToken();
      if (currWord.equals("\n") || currWord.equals("\r") || currWord.equals("\f")) {
        commentBuilder.append(currWord);
        commentNumColsConsumed = 0;
        continue;
      }
      if (commentNumColsConsumed + currWord.length() > commentNumCols) {
        // currWord won't fit on the current line
        if (currWord.length() > commentNumCols) {
          // currWord is too long to split on a line even all by itself.
          // Hence we have no option but to split it.  The first chunk
          // will go to the end of the current line.  Subsequent chunks
          // will be of length commentNumCols.  The last chunk
          // may be smaller.
          while (currWord.length() > commentNumCols) {
            int remainingLineLen = commentNumCols - commentNumColsConsumed;
            String wordChunk = currWord.substring(0, remainingLineLen);
            commentBuilder.append(wordChunk);
            commentBuilder.append(MetaDataFormatUtils.LINE_DELIM);
            commentNumColsConsumed = 0;
            currWord = currWord.substring(remainingLineLen);
          }
          // Handle the last chunk
          if (currWord.length() > 0) {
            commentBuilder.append(currWord);
            commentNumColsConsumed = currWord.length();
          }
        } else {
          // Start on a new line
          commentBuilder.append(MetaDataFormatUtils.LINE_DELIM);
          if (!currWord.equals(" ")) {
            // When starting a new line, do not start with a space.
            commentBuilder.append(currWord);
            commentNumColsConsumed = currWord.length();
          } else {
            commentNumColsConsumed = 0;
          }
        }
      } else {
        commentBuilder.append(currWord);
        commentNumColsConsumed += currWord.length();
      }
    }
    return commentBuilder.toString();
  }

  /**
   * Appends the specified text with alignment to sb.
   * Also appends an appopriately sized delimiter.
   * @return The number of columns consumed by the aligned string and the
   * delimiter.
   */
  private static int appendFormattedColumn(StringBuilder sb, String text,
      int alignment) {
    String paddedText = String.format("%-" + alignment + "s", text);
    int delimCount = 0;
    if (paddedText.length() < alignment + PRETTY_MAX_INTERCOL_SPACING) {
      delimCount = (alignment + PRETTY_MAX_INTERCOL_SPACING)
                      - paddedText.length();
    } else {
      delimCount = PRETTY_MAX_INTERCOL_SPACING;
    }
    String delim = StringUtils.repeat(" ", delimCount);
    sb.append(paddedText);
    sb.append(delim);

    return paddedText.length() + delim.length();
  }

  private static void formatOutputPretty(String colName, String colType,
      String colComment, StringBuilder tableInfo, int maxColNameLength,
      int prettyOutputNumCols) {
    int colsConsumed = 0;
    colsConsumed += appendFormattedColumn(tableInfo, colName, maxColNameLength + 1);
    colsConsumed += appendFormattedColumn(tableInfo, colType, PRETTY_ALIGNMENT);

    colComment = breakCommentIntoMultipleLines(colComment, colsConsumed, prettyOutputNumCols);

    /* Comment indent processing for multi-line comments.
     * Comments should be indented the same amount on each line
     * if the first line comment starts indented by k,
     * the following line comments should also be indented by k.
     */
    String[] commentSegments = colComment.split("\n|\r|\r\n");
    tableInfo.append(trimTrailingWS(commentSegments[0]));
    tableInfo.append(MetaDataFormatUtils.LINE_DELIM);
    for (int i = 1; i < commentSegments.length; i++) {
      tableInfo.append(StringUtils.repeat(" ", colsConsumed));
      tableInfo.append(trimTrailingWS(commentSegments[i]));
      tableInfo.append(MetaDataFormatUtils.LINE_DELIM);
    }
  }

  private static String trimTrailingWS(String str) {
    return str.replaceAll("\\s+$", "");
  }
}
