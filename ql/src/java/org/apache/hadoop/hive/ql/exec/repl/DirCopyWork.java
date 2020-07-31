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
package org.apache.hadoop.hive.ql.exec.repl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.repl.util.StringConvertibleObject;
import org.apache.hadoop.hive.ql.plan.Explain;
import java.io.Serializable;

/**
 * DirCopyWork, mainly to be used to copy External table data.
 */
@Explain(displayName = "HDFS Copy Operator", explainLevels = { Explain.Level.USER,
        Explain.Level.DEFAULT,
        Explain.Level.EXTENDED })
public class DirCopyWork implements Serializable, StringConvertibleObject {
  private static final String URI_SEPARATOR = "#";
  private static final long serialVersionUID = 1L;
  private Path fullyQualifiedSourcePath;
  private Path fullyQualifiedTargetPath;

  public DirCopyWork() {
  }

  public DirCopyWork(Path fullyQualifiedSourcePath, Path fullyQualifiedTargetPath) {
    this.fullyQualifiedSourcePath = fullyQualifiedSourcePath;
    this.fullyQualifiedTargetPath = fullyQualifiedTargetPath;
  }
  @Override
  public String toString() {
    return "DirCopyWork{"
            + "fullyQualifiedSourcePath=" + getFullyQualifiedSourcePath()
            + ", fullyQualifiedTargetPath=" + getFullyQualifiedTargetPath()
            + '}';
  }

  public Path getFullyQualifiedSourcePath() {
    return fullyQualifiedSourcePath;
  }

  public Path getFullyQualifiedTargetPath() {
    return fullyQualifiedTargetPath;
  }

  @Override
  public String convertToString() {
    StringBuilder objInStr = new StringBuilder();
    objInStr.append(fullyQualifiedSourcePath)
            .append(URI_SEPARATOR)
            .append(fullyQualifiedTargetPath);
    return objInStr.toString();
  }

  @Override
  public void loadFromString(String objectInStr) {
    String paths[] = objectInStr.split(URI_SEPARATOR);
    this.fullyQualifiedSourcePath = new Path(paths[0]);
    this.fullyQualifiedTargetPath = new Path(paths[1]);
  }
}
