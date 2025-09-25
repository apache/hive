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
import java.util.function.Function;

public class LambdaFetchConverter extends FetchConverter {

  private final Function<String, String> lambda;

  private final PrintStream inner;
  private final boolean innerIsFetchConverter;

  public LambdaFetchConverter(OutputStream out, boolean autoFlush, String encoding, Function<String, String> lambda)
      throws UnsupportedEncodingException {
    super(out, autoFlush, encoding);
    inner = out instanceof PrintStream ? (PrintStream) out : new PrintStream(out);
    innerIsFetchConverter = out instanceof FetchConverter;
    this.lambda = lambda;
  }

  @Override
  public void foundQuery(boolean queryfound) {
    super.foundQuery(queryfound);
    if(innerIsFetchConverter) {
      ((FetchConverter)inner).foundQuery(queryfound);
    }
  }

  @Override
  public void fetchStarted() {
    super.fetchStarted();
    if(innerIsFetchConverter) {
      ((FetchConverter)inner).fetchStarted();
    }
  }

  @Override
  protected void process(String out) {
    inner.println(lambda.apply(out));
  }

  @Override
  protected void processFinal() {
    if(innerIsFetchConverter) {
      ((FetchConverter)inner).processFinal();
    }
  }

  @Override
  public void flush() {
    super.flush();
    if(innerIsFetchConverter) {
      ((FetchConverter)inner).flush();
    }
  }

  @Override
  public void fetchFinished() {
    super.fetchFinished();
    if(innerIsFetchConverter) {
      ((FetchConverter)inner).fetchFinished();
    }
  }
}

