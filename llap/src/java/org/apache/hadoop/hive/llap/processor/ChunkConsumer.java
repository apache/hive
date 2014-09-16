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
package org.apache.hadoop.hive.llap.processor;

import org.apache.hadoop.hive.llap.api.Vector;

/**
 * Interface implemented by reader; allows it to receive blocks asynchronously.
 */
public interface ChunkConsumer {
  public void init(ChunkProducerFeedback feedback);
  public void setDone();
  // For now this returns Vector, which has to have full rows.
  // Vectorization cannot run on non-full rows anyway so that's ok. Maybe later we can
  // have LazyVRB which only loads columns when needed... one can dream right?
  public void consumeVector(Vector vector);
  public void setError(Throwable t);
}
