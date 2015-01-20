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

package org.apache.hadoop.hive.llap.io.decode;

import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.Consumer;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.api.orc.OrcBatchKey;
import org.apache.hadoop.hive.llap.io.encoded.EncodedDataProducer;
import org.apache.hadoop.hive.llap.io.encoded.OrcEncodedDataProducer;

public class OrcColumnVectorProducer extends ColumnVectorProducer<OrcBatchKey> {
  private final OrcEncodedDataProducer edp;

  public OrcColumnVectorProducer(
      ExecutorService executor, OrcEncodedDataProducer edp, Configuration conf) {
    super(executor);
    this.edp = edp;
  }

  @Override
  protected EncodedDataProducer<OrcBatchKey> getEncodedDataProducer() {
    return edp;
  }

  @Override
  protected void decodeBatch(OrcBatchKey batchKey,
      EncodedColumnBatch batch, Consumer<ColumnVectorBatch> downstreamConsumer) {
    throw new UnsupportedOperationException("not implemented");
    // TODO:  HERE decode EncodedColumn-s into ColumnVector-s
    //        sarg columns first, apply sarg, then decode others if needed; can cols skip values?
    //        fill lockedBuffers from batch as we go and lock multiple times /after first/;
    //        the way unlocking would work is that consumer returns CVB and we unlock lockedBuffer,
    //        so we must either lock for each CVB or have extra refcount here and a map.
  }
}
