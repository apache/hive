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
  private Path[] fromPath;
  private Path[] toPath;
  private boolean errorOnSrcEmpty;
  private boolean isSkipMmDirs = false;

  public CopyWork() {
  }

  public CopyWork(final Path fromPath, final Path toPath) {
    this(fromPath, toPath, true);
  }

  public CopyWork(final Path fromPath, final Path toPath, boolean errorOnSrcEmpty) {
    this(new Path[] { fromPath }, new Path[] { toPath });
    this.setErrorOnSrcEmpty(errorOnSrcEmpty);
  }

  public CopyWork(final Path[] fromPath, final Path[] toPath) {
    if (fromPath.length != toPath.length) {
      throw new RuntimeException(
          "Cannot copy " + fromPath.length + " paths into " + toPath.length + " paths");
    }
    this.fromPath = fromPath;
    this.toPath = toPath;
  }

  // Keep backward compat in explain for single-file copy tasks.
  @Explain(displayName = "source", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Path getFromPathExplain() {
    return (fromPath == null || fromPath.length > 1) ? null : fromPath[0];
  }

  @Explain(displayName = "destination", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Path getToPathExplain() {
    return (toPath == null || toPath.length > 1) ? null : toPath[0];
  }

  @Explain(displayName = "sources", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Path[] getFromPathsExplain() {
    return (fromPath != null && fromPath.length > 1) ? fromPath : null;
  }

  @Explain(displayName = "destinations", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Path[] getToPathsExplain() {
    return (toPath != null && toPath.length > 1) ? toPath : null;
  }

  public Path[] getFromPaths() {
    return fromPath;
  }

  public Path[] getToPaths() {
    return toPath;
  }

  public void setErrorOnSrcEmpty(boolean errorOnSrcEmpty) {
    this.errorOnSrcEmpty = errorOnSrcEmpty;
  }

  public boolean isErrorOnSrcEmpty() {
    return errorOnSrcEmpty;
  }

  /** Whether the copy should ignore MM directories in the source, and copy their content to
   * destination directly, rather than copying the directories themselves. */
  public void setSkipSourceMmDirs(boolean isMm) {
    this.isSkipMmDirs = isMm;
  }

  public boolean doSkipSourceMmDirs() {
    return isSkipMmDirs ;
  }

}
