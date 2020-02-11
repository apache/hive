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

import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

/**
 * The Session uses this stream instead of PrintStream to prevent closing of System out and System err.
 * Ref: HIVE-21033
 */
public class SessionStream extends PrintStream {

  public SessionStream(OutputStream out) {
    super(out);
  }

  public SessionStream(OutputStream out, boolean autoFlush, String encoding) throws UnsupportedEncodingException {
    super(out, autoFlush, encoding);
  }

  @Override
  public void close() {
    if (out != System.out && out != System.err) {
      //Don't close if system out or system err
      super.close();
    }
  }
}
