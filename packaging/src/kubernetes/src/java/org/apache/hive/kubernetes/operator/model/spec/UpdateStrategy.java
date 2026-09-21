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
package org.apache.hive.kubernetes.operator.model.spec;

/**
 * How a pod template change reaches the daemons that are already running. The constants are
 * named as they are written in the custom resource, which is also what the generated CRD
 * schema accepts, so a misspelling is rejected by the API server rather than by the operator.
 */
public enum UpdateStrategy {

  /**
   * One LLAP daemon at a time. A StatefulSet rolls strictly by ordinal, so the wall time is one
   * pod startup per daemon, and part of the cluster keeps serving throughout.
   */
  RollingUpdate,

  /**
   * All of them together: one pod startup for the whole cluster, during which none of it serves.
   * A StatefulSet has no such strategy, so this becomes OnDelete plus a bulk delete driven by
   * the reconciler; a Deployment implements it natively.
   */
  Recreate
}
