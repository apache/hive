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
package org.apache.hadoop.hive.llap.cache;


import static org.junit.Assert.*;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.llap.IncrementalObjectSizeEstimator;
import org.apache.hadoop.hive.llap.IncrementalObjectSizeEstimator.ObjectEstimator;
import org.apache.hadoop.hive.llap.io.metadata.OrcFileMetadata;
import org.apache.hadoop.hive.llap.io.metadata.OrcStripeMetadata;
import org.junit.Test;

public class TestIncrementalObjectSizeEstimator {
  private static final Log LOG = LogFactory.getLog(TestIncrementalObjectSizeEstimator.class);

  @Test
  public void testBasicEstimate() {
    OrcStripeMetadata ofm = OrcStripeMetadata.createDummy();
    HashMap<Class<?>, ObjectEstimator> map =
        IncrementalObjectSizeEstimator.createEstimator(ofm);
    LOG.info("Estimated " + map.get(OrcFileMetadata.class).estimate(ofm, map));
  }
}
