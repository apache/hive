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

package org.apache.hadoop.hive.ql;

/**
 * Standard output and return code of a process executed during the qtests.
 */
public class QTestProcessExecResult {

  private static final String TRUNCATED_OUTPUT = "Output was too long and had to be truncated...";
  private static final short MAX_OUTPUT_CHAR_LENGTH = 2000;

  private final int returnCode;
  private final String standardOut;

  QTestProcessExecResult(int code, String output) {
    this.returnCode = code;
    this.standardOut = truncatefNeeded(output);
  }

  /**
   * @return executed process return code
   */
  public int getReturnCode() {
    return this.returnCode;
  }

  /**
   * @return output captured from stdout while process was executing
   */
  public String getCapturedOutput() {
    return this.standardOut;
  }

  public static QTestProcessExecResult create(int code, String output) {
    return new QTestProcessExecResult(code, output);
  }

  public static  QTestProcessExecResult createWithoutOutput(int code) {
    return new QTestProcessExecResult(code, "");
  }

  private String truncatefNeeded(String orig) {
    if (orig.length() > MAX_OUTPUT_CHAR_LENGTH) {
      return orig.substring(0, MAX_OUTPUT_CHAR_LENGTH) + "\r\n" + TRUNCATED_OUTPUT;
    } else {
      return orig;
    }
  }
}
