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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 * Enumerates records from an Unenclosed GeoJSON file
 *
 */
/*
 * The JSON will look like this (white-space ignored)
 *
 * { // start record 1
 *  "type" : ""
 * 	"properties" : {}
 *  "geometry" : {}
 * } // end record 1
 * { // start record 2
 *  "type" : ""
 * 	"properties" : {}
 *  "geometry" : {}
 * } // end record 2
 */
public class UnenclosedGeoJsonRecordReader extends UnenclosedBaseJsonRecordReader {
  static final Logger LOG = LoggerFactory.getLogger(UnenclosedGeoJsonRecordReader.class.getName());

  public UnenclosedGeoJsonRecordReader() throws IOException {  // explicit just to declare exception
    super();
  }

  public UnenclosedGeoJsonRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration conf)
      throws IOException {
    //attrLabel = "properties";
    super(split, conf);
  }

  // Record boundary defined as : \{\s*"(properties|geometry)"\s*:\s*\{
  protected boolean moveToRecordStart() throws IOException {
    // The case of split point exactly at whitespace between records, is
    // handled by forcing it to the split following, in the interest of
    // better balancing the splits, by consuming the whitespace in next().
    // The alternative of forcing it to the split preceding, could be
    // done like what is commented here.
    //   while (next != '{' || skipDup > 0) {  // skipDup>0 => record already consumed
    // 	  next = getChar();
    // 	  if (next < 0)  return false;   // end of stream, no good
    // 	  if (next == '}')  skipDup = -1;  // Definitely not
    // 	  else if (skipDup == 0) skipDup = 1;  // no info - Maybe so until refuted by '}'
    //   }

    final int START = 0, BRACE = 1, TYPE = 2, FOUND = 3, FAIL = 4;
    int next = 0, state = START;
    long resetPosition = readerPosition;
    boolean inEscape = false;
    String fieldName = "";

    while (true) {
      switch (state) {
      case START:
        // scan until we reach a {
        while (next != '{') {
          next = getChar();
          // end of stream, no good
          if (next < 0) {
            return false;
          }
        }

        resetPosition = readerPosition;
        inputReader.mark(100);

        // ok last char was '{', skip till we get to a '"'
        next = getNonWhite();
        if (next < 0) {   // end of stream, no good
          return false;
        }
        if (next == '"') {
          state = BRACE;
        }  // else redo START
        break;

      case BRACE:
        fieldName = "";
        // Next should be a field name of "geometry" or "properties" or "type".

        // If we see another opening brace, the previous one must have been inside
        // a quoted string literal (after which the double quote we found, was a
        // closing quote mark rather than the opening quote mark) - start over.

        while (next != '{') {
          next = getChar();
          if (next < 0) {  // end of stream, no good
            return false;
          }

          inEscape = (!inEscape && next == '\\');
          if (!inEscape && next == '"') {
            break;
          }

          fieldName += (char) next;
        }

        if (fieldName.equals("properties") || fieldName.equals("geometry")) {
          // ok last char was '"', skip till we get to a ':'
          if ((next = getNonWhite()) < 0) {   // end of stream, no good
            return false;
          }
          if (next != ':') {
            state = START;
          } else {
            // and finally, if the next char is a {, we know for sure that this is a valid record
            if ((next = getNonWhite()) < 0) {   // end of stream, no good
              return false;
            }
            state = (next == '{') ? FOUND : START;
          }
        } else if (fieldName.equals("type")) {
          state = TYPE;
        } else {
          // not a field name we were expecting, start over
          state = START;
        }
        break;

      case TYPE:  // expect  :"Feature","
        if ((next = getNonWhite()) < 0) {   // end of stream, no good
          return false;
        }
        if (next != ':') {
          state = START;
        } else {
          if ((next = getNonWhite()) < 0) {   // end of stream, no good
            return false;
          }
          if (next != '"') {
            state = START;
          } else {
            fieldName = "";
            while (true) {
              if ((next = getChar()) < 0)
                return false;  // end of stream, no good
              inEscape = (!inEscape && next == '\\');
              if (!inEscape && next == '"')
                break;
              fieldName += (char) next;
            }
            if (!"feature".equals(fieldName.toLowerCase())) {
              state = START;
            } else {
              if ((next = getNonWhite()) < 0) {   // end of stream, no good
                return false;
              }
              if (next != ',') {
                state = START;
              } else {
                if ((next = getNonWhite()) < 0) {   // end of stream, no good
                  return false;
                }
                state = (next == '"') ? BRACE : START;
              }
            }
          }
        }
        break;

      case FOUND:
        inputReader.reset();
        readerPosition = resetPosition;
        firstBraceConsumed = true;
        return true;

      case FAIL:
        return false;

      default:
        throw new RuntimeException("Internal error");
      }
    }

  }

}
