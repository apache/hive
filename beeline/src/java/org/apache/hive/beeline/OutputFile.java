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

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * See SQLLine notice in LICENSE
 */
package org.apache.hive.beeline;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class OutputFile {
  final File file;
  final PrintWriter out;

  public OutputFile(String filename) throws IOException {
    file = new File(filename);
    out = new PrintWriter(new FileWriter(file));
  }

  @Override
  public String toString() {
    return file.getAbsolutePath();
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
