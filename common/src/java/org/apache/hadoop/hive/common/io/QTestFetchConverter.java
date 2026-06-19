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
import java.util.function.UnaryOperator;

/**
 * Applies a function to the processed lines, before passing it to the wrapped output stream.
 */
public class QTestFetchConverter extends SessionStream implements FetchCallback {

  private final UnaryOperator<String> transformation;

  private final PrintStream inner;
  private final boolean hasFetchCallback;

  public QTestFetchConverter(OutputStream out, boolean autoFlush, String encoding, UnaryOperator<String> transformation)
      throws UnsupportedEncodingException {
    super(out, autoFlush, encoding);
    inner = out instanceof PrintStream ps ? ps : new PrintStream(out);
    hasFetchCallback = out instanceof FetchCallback;
    this.transformation = transformation;
  }

  @Override
  public void println(String str) {
    inner.println(transformation.apply(str));
  }

  @Override
  public void foundQuery(boolean queryfound) {
    if (hasFetchCallback) {
      ((FetchCallback)inner).foundQuery(queryfound);
    }
  }

  @Override
  public void fetchStarted() {
    if (hasFetchCallback) {
      ((FetchCallback)inner).fetchStarted();
    }
  }

  @Override
  public void fetchFinished() {
    if (hasFetchCallback) {
      ((FetchCallback)inner).fetchFinished();
    }
    flush();
  }
}

