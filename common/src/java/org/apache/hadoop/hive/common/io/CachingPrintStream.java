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

package org.apache.hadoop.hive.common.io;

import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

// A printStream that stores messages logged to it in a list.
public class CachingPrintStream extends PrintStream {

  List<String> output = new ArrayList<String>();

  public CachingPrintStream(OutputStream out, boolean autoFlush, String encoding)
      throws FileNotFoundException, UnsupportedEncodingException {

    super(out, autoFlush, encoding);
  }

  public CachingPrintStream(OutputStream out) {

    super(out);
  }

  @Override
  public void println(String out) {
    output.add(out);
    super.println(out);
  }

  @Override
  public void flush() {
    output = new ArrayList<String>();
    super.flush();
  }

  public List<String> getOutput() {
    return output;
  }
}
