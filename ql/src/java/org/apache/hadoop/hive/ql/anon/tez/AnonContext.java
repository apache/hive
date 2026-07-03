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

package org.apache.hadoop.hive.ql.anon.tez;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class AnonContext {

  private Path inputPath;
  private Path tmpPath;

  public AnonContext(final Object value) {
    if (!(value instanceof Text)) {
      throw new RuntimeException("Text expected");
    }
    final Text pathText = (Text) value;
    final String inputPathString = pathText.toString();
    init(inputPathString);
  }

  public AnonContext(final Path inputPath) {
    init(inputPath.toString());
  }

  private void init(final String inputPathString) {
    inputPath = new Path(inputPathString);
    tmpPath = new Path(inputPath.getParent(), "." + inputPath.getName() + ".anon.tmp");
  }

  public Path getInputPath() {
    return inputPath;
  }

  public Path getTmpPath() {
    return tmpPath;
  }

  public void setTmpPath(final Path tmpPath) {
    this.tmpPath = tmpPath;
  }
}
