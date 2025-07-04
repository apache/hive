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

package org.apache.hive.beeline;

import org.apache.hive.beeline.BeeLineDummyTerminal;

import java.io.IOException;
import java.io.InputStream;

import org.jline.reader.LineReader;

/**
 * A Beeline implementation with a dummy terminal that forces the use of the file reader.
 * This is typically needed in testing scenarios where the entire script is provided via the -f option.
 * More specific use cases are described in the getLineReader method.
 */
public class BeeLineDummyTerminalFromFile extends BeeLineDummyTerminal {
  private LineReader fileReader;

  @Override
  public LineReader getFileLineReader(InputStream inputStream) throws IOException {
    this.fileReader = super.getFileLineReader(inputStream);
    return this.fileReader;
  }

  /**
   * There are scenarios involving connect/reconnect,
   * where the password prompt is triggered. In these cases, the prompt is bypassed using a simple line break.
   * Since the test class exclusively uses file-based script input, getLineReader should return the
   * file reader created in getFileLineReader to correctly handle password prompts via '\n' control characters.
   *
   * @return the file reader created in getFileLineReader
   */
  @Override
  public LineReader getLineReader() {
    return fileReader;
  }
}
