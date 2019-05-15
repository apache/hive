/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.log;

import java.lang.management.ManagementFactory;

import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.pattern.AbstractPatternConverter;
import org.apache.logging.log4j.core.pattern.ArrayPatternConverter;
import org.apache.logging.log4j.core.pattern.ConverterKeys;

/**
 * FilePattern converter that converts %pid pattern to &lt;process-id&gt;@&lt;hostname&gt; information
 * obtained at runtime.
 *
 * Example usage:
 * &lt;RollingFile name="Rolling-default" fileName="test.log" filePattern="test.log.%pid.gz"&gt;
 *
 * Will generate output file with name containing &lt;process-id&gt;@&lt;hostname&gt; like below
 * test.log.95232@localhost.gz
 */
@Plugin(name = "PidFilePatternConverter", category = "FileConverter")
@ConverterKeys({ "pid" })
public class PidFilePatternConverter extends AbstractPatternConverter implements
    ArrayPatternConverter {

  /**
   * Private constructor.
   */
  private PidFilePatternConverter() {
    super("pid", "pid");
  }

  @PluginFactory
  public static PidFilePatternConverter newInstance() {
    return new PidFilePatternConverter();
  }

  public void format(StringBuilder toAppendTo, Object... objects) {
    toAppendTo.append(ManagementFactory.getRuntimeMXBean().getName());
  }

  public void format(Object obj, StringBuilder toAppendTo) {
    toAppendTo.append(ManagementFactory.getRuntimeMXBean().getName());
  }
}
