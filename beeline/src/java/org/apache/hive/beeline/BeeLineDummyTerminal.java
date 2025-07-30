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

package org.apache.hive.beeline;

import java.io.IOException;
import java.io.InputStream;

import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;

/**
 * A Beeline implementation that always creates a DumbTerminal.
 * This class resides in the production source code (not in tests) because Beeline can serve as a
 * dummy terminal tool without real user interaction (e.g., HiveSchemaTool), not just in testing scenarios,
 * although that is its primary use case.
 */
public class BeeLineDummyTerminal extends BeeLine {

  public BeeLineDummyTerminal() {
    this(true);
  }

  public BeeLineDummyTerminal(boolean isBeeLine) {
    super(isBeeLine);
  }

  @Override
  protected Terminal buildTerminal(InputStream inputStream) throws IOException {
    return new DumbTerminal(inputStream, getErrorStream());
  }
}
