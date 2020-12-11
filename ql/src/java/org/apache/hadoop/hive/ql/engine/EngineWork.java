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

package org.apache.hadoop.hive.ql.engine;

import java.io.Serializable;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * EngineWork.  This class serves as a dummy base class for engine specific "Work"
 * objects. The core logic needs a specific class definition.
 * XXX: CDPD-20696: This logic will only work with one external class.  This will need
 * to be redesigned.
 */
@InterfaceStability.Unstable
public abstract class EngineWork implements Serializable {
}
