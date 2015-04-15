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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * CopyWork.
 *
 */
@Explain(displayName = "Copy", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CopyWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private Path fromPath;
  private Path toPath;
  private boolean errorOnSrcEmpty;

  public CopyWork() {
  }

  public CopyWork(final Path fromPath, final Path toPath) {
    this(fromPath, toPath, true);
  }

  public CopyWork(final Path fromPath, final Path toPath, boolean errorOnSrcEmpty) {
    this.fromPath = fromPath;
    this.toPath = toPath;
    this.setErrorOnSrcEmpty(errorOnSrcEmpty);
  }
  
  @Explain(displayName = "source", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Path getFromPath() {
    return fromPath;
  }

  @Explain(displayName = "destination", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Path getToPath() {
    return toPath;
  }

  public void setErrorOnSrcEmpty(boolean errorOnSrcEmpty) {
    this.errorOnSrcEmpty = errorOnSrcEmpty;
  }

  public boolean isErrorOnSrcEmpty() {
    return errorOnSrcEmpty;
  }

}
