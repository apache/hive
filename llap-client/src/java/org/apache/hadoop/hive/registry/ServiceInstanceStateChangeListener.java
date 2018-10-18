/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.registry;

/**
 * Callback listener for instance state change events
 */
public interface ServiceInstanceStateChangeListener<InstanceType extends ServiceInstance> {
  /**
   * Called when new {@link ServiceInstance} is created.
   *
   * @param serviceInstance - created service instance
   */
  void onCreate(InstanceType serviceInstance, int ephSeqVersion);

  /**
   * Called when an existing {@link ServiceInstance} is updated.
   *
   * @param serviceInstance - updated service instance
   */
  void onUpdate(InstanceType serviceInstance, int ephSeqVersion);

  /**
   * Called when an existing {@link ServiceInstance} is removed.
   *
   * @param serviceInstance - removed service instance
   */
  void onRemove(InstanceType serviceInstance, int ephSeqVersion);
}
