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
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * LoadDesc.
 *
 */
public class LoadDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private Path sourcePath;
  /**
   * Need to remember whether this is an acid compliant operation, and if so whether it is an
   * insert, update, or delete.
   */
  private final AcidUtils.Operation writeType;


  LoadDesc(final Path sourcePath, AcidUtils.Operation writeType) {
    this.sourcePath = sourcePath;
    this.writeType = writeType;
  }

  @Explain(displayName = "source", explainLevels = { Level.EXTENDED })
  public Path getSourcePath() {
    return sourcePath;
  }

  public void setSourcePath(Path sourcePath) {
    this.sourcePath = sourcePath;
  }

  public AcidUtils.Operation getWriteType() {
    return writeType;
  }

  @Explain(displayName = "Write Type")
  public String getWriteTypeString() {
    //if acid write, add to plan output, else don't bother
    return getWriteType() == AcidUtils.Operation.NOT_ACID ? null : getWriteType().toString();
  }
}
