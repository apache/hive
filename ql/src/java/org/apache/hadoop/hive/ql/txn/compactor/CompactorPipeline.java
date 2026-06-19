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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import java.io.IOException;

/**
 * Runs different compactions based on the order in which compactors are added.<br>
 * Mainly used for fall back mechanism for Merge compaction.
 */
public class CompactorPipeline {

  private Compactor compactor;
  private final boolean isMR;

  public CompactorPipeline(Compactor compactor) {
    this.compactor = compactor;
    this.isMR = compactor instanceof MRCompactor;
  }

  CompactorPipeline addCompactor(Compactor newCompactor) {
    compactor = new FallbackCompactor(compactor, newCompactor);
    return this;
  }

  public boolean isMRCompaction() {
    return isMR;
  }

  public Boolean execute(CompactorContext input) throws IOException, HiveException, InterruptedException {
    return compactor.run(input);
  }

  /**
   * This class defines a way of handling fallback given any number of fallback compactors.<br>
   * It can encapsulate other fallback compactors within itself.
   */
  static final class FallbackCompactor implements Compactor {
    private final Compactor primaryCompactor;
    private final Compactor secondaryCompactor;

    FallbackCompactor(Compactor primaryCompactor, Compactor secondaryCompactor) {
      this.primaryCompactor = primaryCompactor;
      this.secondaryCompactor = secondaryCompactor;
    }

    @Override
    public boolean run(CompactorContext context) throws IOException, HiveException, InterruptedException {
      boolean result = primaryCompactor.run(context);
      return !result ? secondaryCompactor.run(context) : result;
    }
  }
}