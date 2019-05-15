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
import java.io.UnsupportedEncodingException;

public abstract class FetchConverter extends SessionStream {

  protected volatile boolean queryfound;
  protected volatile boolean fetchStarted;

  public FetchConverter(OutputStream out, boolean autoFlush, String encoding)
      throws UnsupportedEncodingException {
    super(out, autoFlush, encoding);
  }

  public void foundQuery(boolean queryfound) {
    this.queryfound = queryfound;
  }

  public void fetchStarted() {
    fetchStarted = true;
  }

  public void println(String out) {
    if (byPass()) {
      printDirect(out);
    } else {
      process(out);
    }
  }

  protected final void printDirect(String out) {
    super.println(out);
  }

  protected final boolean byPass() {
    return !queryfound || !fetchStarted;
  }

  protected abstract void process(String out);

  protected abstract void processFinal();

  @Override
  public void flush() {
    if (byPass()) {
      super.flush();
    }
  }

  public void fetchFinished() {
    if (!byPass()) {
      processFinal();
    }
    super.flush();
    fetchStarted = false;
  }
}
