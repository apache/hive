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
 * Enumerates records from an Esri Unenclosed JSON file
 *
 */
/*
 * The JSON will look like this (white-space ignored)
 *
 * { // start record 1
 * 	"attributes" : {}
 *  "geometry" : {}
 * } // end record 1
 * { // start record 2
 * 	"attributes" : {}
 *  "geometry" : {}
 * } // end record 2
 */
public class UnenclosedEsriJsonRecordReader extends UnenclosedBaseJsonRecordReader {
  static final Logger LOG = LoggerFactory.getLogger(UnenclosedEsriJsonRecordReader.class.getName());

  public UnenclosedEsriJsonRecordReader() throws IOException {  // explicit just to declare exception
    super();
  }

  public UnenclosedEsriJsonRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration conf)
      throws IOException {
    //attrLabel = "attributes";
    super(split, conf);
  }

  /**
   * Given an arbitrary byte offset into a unenclosed JSON document,
   * find the start of the next record in the document.  Discard trailing
   * bytes from the previous record if we happened to seek to the middle
   * of it
   *
   * Record boundary defined as : \{\s*"(attributes|geometry)"\s*:\s*\{
   *
   * @throws java.io.IOException
   */
  protected boolean moveToRecordStart() throws IOException {
    int next = 0;
    long resetPosition = readerPosition;

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

    while (true) {

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
      if (next != '"') {
        continue;
      }

      boolean inEscape = false;
      String fieldName = "";
      // Next should be a field name of  attributes  or  geometry .

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

      if (!(fieldName.equals("attributes") || fieldName.equals("geometry"))) {
        // not the field name we were expecting, start over
        continue;
      }

      // ok last char was '"', skip till we get to a ':'
      next = getNonWhite();
      if (next < 0) {   // end of stream, no good
        return false;
      }
      if (next != ':') {
        continue;
      }

      // and finally, if the next char is a {, we know for sure that this is a valid record
      next = getNonWhite();
      if (next < 0) {   // end of stream, no good
        return false;
      }

      if (next == '{') {
        // at this point we can be sure that we have found the record boundary
        break;
      }
    }

    inputReader.reset();
    readerPosition = resetPosition;

    firstBraceConsumed = true;

    return true;
  }

}
