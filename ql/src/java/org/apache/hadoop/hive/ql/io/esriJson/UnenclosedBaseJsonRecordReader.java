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
package org.apache.hadoop.hive.ql.io.esriJson;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 *
 * Enumerates records from an Unenclosed JSON file - use either Esri JSON or GeoJSON subclass
 *
 */
public abstract class UnenclosedBaseJsonRecordReader extends RecordReader<LongWritable, Text>
    implements org.apache.hadoop.mapred.RecordReader<LongWritable, Text> {
  static final Logger LOG = LoggerFactory.getLogger(UnenclosedBaseJsonRecordReader.class.getName());

  protected BufferedReader inputReader;
  protected LongWritable mkey = null;
  protected Text mval = null;
  protected long readerPosition;
  protected long start, end;
  protected boolean firstBraceConsumed = false;

  protected UnenclosedBaseJsonRecordReader() throws IOException {
    mkey = createKey();
    mval = createValue();
  }

  protected UnenclosedBaseJsonRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration conf)
      throws IOException {
    org.apache.hadoop.mapred.FileSplit fileSplit = (org.apache.hadoop.mapred.FileSplit) split;
    start = fileSplit.getStart();
    end = fileSplit.getLength() + start;
    Path filePath = fileSplit.getPath();
    commonInit(filePath, conf);
  }

  @Override
  public void close() throws IOException {
    if (inputReader != null)
      inputReader.close();
  }

  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }

  @Override
  public Text createValue() {
    return new Text();
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return mkey;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return mval;
  }

  @Override
  public long getPos() throws IOException {
    return readerPosition;
  }

  @Override
  public float getProgress() throws IOException {
    return (float) (readerPosition - start) / (end - start);
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext taskContext) throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit) split;
    start = fileSplit.getStart();
    end = fileSplit.getLength() + start;
    Path filePath = fileSplit.getPath();
    commonInit(filePath, taskContext.getConfiguration());
  }

  @Override
  public boolean next(LongWritable key, Text value) throws IOException {
    /*
     * NOTE : we are not using a JSONParser, so this will not validate JSON structure aside from correct counts of '{' and '}'
     * The fact that it may handle some invalid JSON, does not imply that we support invalid JSON;
     * rather, updates to the code may require valid JSON in order to locate record boundaries.
     *
     * We will count '{' and '}' to find the beginning and end of each record, while ignoring braces in string literals.
     */

    int chr = 0;
    int brace_depth = 0;
    char lit_char = 0;
    boolean first_brace_found = false;

    // The case of split point exactly at whitespace between records,
    // is handled by forcing the record following to the split following,
    // in the interest of better balancing the splits, by consuming the
    // whitespace before checking the end of the split.
    if (!firstBraceConsumed) {  // That should only ever be true on the very first read in the split
      chr = getNonWhite();
      firstBraceConsumed = (chr == '{');
    }

    if (readerPosition + (firstBraceConsumed ? 0 : 1) > end) {
      return false;
    }

    StringBuilder sb = new StringBuilder(2000);

    if (firstBraceConsumed) {
      // first open brace was consumed already;
      // update initial state accordingly
      brace_depth = 1;
      sb.append("{");
      first_brace_found = true;
      firstBraceConsumed = false;
      key.set(readerPosition - 1);
    }

    boolean inEscape = false;
    while (brace_depth > 0 || !first_brace_found) {
      chr = getChar();

      if (chr < 0) {
        if (first_brace_found) {
          // last record was invalid
          LOG.error("Parsing error : EOF occurred before record ended");
        }
        return false;
      }

      switch (chr) {
      case '\\':
        inEscape = (lit_char != 0 && !inEscape);
        break;
      case '"':
      case '\'':
        if (lit_char == 0) {
          lit_char = (char) chr;  // mark start literal (double/single quote)
        } else if (inEscape) {
          inEscape = false;
        } else if (lit_char == chr) {
          lit_char = 0;   // mark end literal (double/single-quote)
        }
        // ignored because we found a ' inside a " " block quote (or vice versa)
        break;
      case '{':
        if (inEscape) {
          inEscape = false;
        } else if (lit_char == 0) {  // not in string literal,
          brace_depth++;         // so increase brace depth
          if (!first_brace_found) {
            first_brace_found = true;
            key.set(readerPosition - 1); // set record key to the char offset of the first '{'
          }
        }
        break;
      case '}':
        if (inEscape) {
          inEscape = false;
        } else if (lit_char == 0) { // not in string literal,
          brace_depth--;  //  so decrease brace depth
        }
        break;
      default:
        inEscape = false;
        break;
      }

      if (brace_depth < 0) {
        // found more '}'s than we did '{'s
        LOG.error("Parsing error : no '{' - unmatched '}' in record");
        return false;
      }

      if (first_brace_found) {
        sb.append((char) chr);
      }
    }

    // no '{' found before EOF.  Not an error as this could mean that there is extra white-space at the end
    if (!first_brace_found) {
      return false;
    }

    value.set(sb.toString());
    return true;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return next(mkey, mval);
  }

  private void commonInit(Path filePath, Configuration conf) throws IOException {

    readerPosition = start;

    FileSystem fs = filePath.getFileSystem(conf);
    inputReader = new BufferedReader(new InputStreamReader(fs.open(filePath)));

    if (start != 0) {
      // split starts inside the json
      inputReader.skip(start);
      moveToRecordStart();
    }

  }

  protected int getChar() throws IOException {
    int ch = inputReader.read();
    readerPosition++;
    return ch;
  }

  protected int getNonWhite() throws IOException {
    int ch;
    do {
      ch = getChar();
    } while (Character.isWhitespace((char) ch));
    return ch;
  }

  /**
   * Given an arbitrary byte offset into an unenclosed JSON document,
   * find the start of the next record in the document.  Discard trailing
   * bytes from the previous record if we happened to seek to the middle
   * of it.
   *
   * @throws java.io.IOException
   */
  protected abstract boolean moveToRecordStart() throws IOException;
}
