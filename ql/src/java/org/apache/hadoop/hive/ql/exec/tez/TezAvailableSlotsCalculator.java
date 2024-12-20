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

package org.apache.hadoop.hive.ql.exec.tez;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of AvailableSlotsCalculator which relies on available capacity of the cluster
 */
public class TezAvailableSlotsCalculator implements AvailableSlotsCalculator {
  private static final Logger LOG = LoggerFactory.getLogger(TezAvailableSlotsCalculator.class);

    private InputInitializerContext inputInitializerContext;
    @Override
    public void initialize(Configuration conf, HiveSplitGenerator splitGenerator) {
        inputInitializerContext = splitGenerator.getContext();
    }

    @Override
    public int getAvailableSlots() {
        if (inputInitializerContext == null) {
            // for now, totalResource = taskResource for llap
            return 1;
        }
        int totalResource = inputInitializerContext.getTotalAvailableResource().getMemory();
        int taskResource = inputInitializerContext.getVertexTaskResource().getMemory();
        int availableSlots = totalResource / taskResource;;
        LOG.debug("totalResource: {}mb / taskResource: {}mb =  availableSlots: {}", totalResource, taskResource,
            availableSlots);
        return availableSlots;
    }
}
