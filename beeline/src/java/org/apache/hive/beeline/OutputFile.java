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

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * See SQLLine notice in LICENSE
 */
package org.apache.hive.beeline;

import com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

public class OutputFile {
  private final PrintStream out;
  private final String filename;

  public OutputFile(String filename) throws IOException {
    File file = new File(filename);
    this.filename = file.getAbsolutePath();
    this.out = new PrintStream(file, "UTF-8");
  }

  @VisibleForTesting
  protected PrintStream getOut() {
    return out;
  }

  @VisibleForTesting
  protected String getFilename() {
    return filename;
  }

  /**
   * Constructor used by the decorating classes in tests.
   * @param out The output stream
   * @param filename The filename, to use in the toString() method
   */
  @VisibleForTesting
  protected OutputFile(PrintStream out, String filename) {
    this.out = out;
    this.filename = filename;
  }

  /**
   * Returns true if a FetchConverter is defined for writing the results. Should be used only for
   * testing, otherwise returns false.
   * @return True if a FetchConverter is active
   */
  boolean isActiveConverter() {
    return false;
  }

  /**
   * Indicates that result fetching is started, and the converter should be activated. The
   * Converter starts to collect the data when the fetch is started, and prints out the
   * converted data when the fetch is finished. Converter will collect data only if
   * fetchStarted, and foundQuery is true.
   */
  void fetchStarted() {
    // no-op for default output file
  }

  /**
   * Indicates that the following data will be a query result, and the converter should be
   * activated. Converter will collect the data only if fetchStarted, and foundQuery is true.
   * @param foundQuery The following data will be a query result (true) or not (false)
   */
  void foundQuery(boolean foundQuery) {
    // no-op for default output file
  }

  /**
   * Indicates that the previously collected data should be converted and written. Converter
   * starts to collect the data when the fetch is started, and prints out the converted data when
   * the fetch is finished.
   */
  void fetchFinished() {
    // no-op for default output file
  }

  @Override
  public String toString() {
    return filename;
  }

  public void addLine(String command) {
    out.println(command);
  }

  public void println(String command) {
    out.println(command);
  }

  public void print(String command) {
    out.print(command);
  }

  public void close() throws IOException {
    out.close();
  }
}
