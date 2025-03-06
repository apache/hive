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
package org.apache.hive.hplsql;

import java.util.regex.Pattern;

/**
 * {@link Console} implementation used in tests to capture the output of the {@link Exec} class.
 */
class TestConsole implements Console {

    StringBuilder out = new StringBuilder();
    StringBuilder err = new StringBuilder();
    private final Pattern ignorePattern;

    TestConsole(String ignorePattern) {
        this.ignorePattern = Pattern.compile(ignorePattern);
    }

    private boolean isNotIgnored(String msg) {
        return msg == null || !ignorePattern.matcher(msg).matches();
    }

    @Override
    public void print(String msg) {
        if (isNotIgnored(msg)) {
            out.append(msg);
        }
    }

    @Override
    public void printLine(String msg) {
        if (isNotIgnored(msg)) {
            out.append(msg).append("\n");
        }
    }

    @Override
    public void printError(String msg) {
        if (isNotIgnored(msg)) {
            err.append(msg).append("\n");
        }
    }
}
