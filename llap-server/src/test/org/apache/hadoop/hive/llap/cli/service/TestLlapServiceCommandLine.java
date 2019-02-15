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

package org.apache.hadoop.hive.llap.cli.service;

import static junit.framework.TestCase.assertEquals;

import java.util.Properties;

import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableMap;

/** Tests for LlapServiceCommandLine. */
public class TestLlapServiceCommandLine {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testArgumentParsingEmpty() throws Exception {
    thrown.expect(ParseException.class);
    thrown.expectMessage("instance must be set");

    new LlapServiceCommandLine(new String[] {});
  }

  @Test
  public void testArgumentParsingDefault() throws Exception {
    LlapServiceCommandLine cl = new LlapServiceCommandLine(new String[] {"--instances", "1"});
    assertEquals(null, cl.getAuxJars());
    assertEquals(-1, cl.getCache());
    assertEquals(new Properties(), cl.getConfig());
    assertEquals(null, cl.getDirectory());
    assertEquals(-1, cl.getExecutors());
    assertEquals(-1, cl.getIoThreads());
    assertEquals(true, cl.getIsHBase());
    assertEquals(true, cl.getIsHiveAux());
    assertEquals(null, cl.getJavaPath());
    assertEquals(null, cl.getLlapQueueName());
    assertEquals(null, cl.getLogger());
    assertEquals(null, cl.getName());
    assertEquals(null, cl.getOutput());
    assertEquals(-1, cl.getSize());
    assertEquals(-1, cl.getXmx());
    assertEquals(false, cl.isStarting());
  }

  @Test
  public void testParsingArguments() throws Exception {
    LlapServiceCommandLine cl = new LlapServiceCommandLine(new String[] {"--instances", "2", "--auxjars", "auxjarsVal",
        "--cache", "10k", "--hiveconf", "a=b", "--directory", "directoryVal", "--executors", "4", "--iothreads", "5",
        "--auxhbase", "false", "--auxhive", "false", "--javaHome", "javaHomeVal", "--queue", "queueVal",
        "--logger", "console", "--name", "nameVal", "--output", "outputVal", "--size", "10m", "--xmx", "10g",
        "--startImmediately"});
    assertEquals("auxjarsVal", cl.getAuxJars());
    assertEquals(10L * 1024, cl.getCache());
    assertEquals(ConfigurationConverter.getProperties(new MapConfiguration(ImmutableMap.of("a", "b"))), cl.getConfig());
    assertEquals("directoryVal", cl.getDirectory());
    assertEquals(4, cl.getExecutors());
    assertEquals(5, cl.getIoThreads());
    assertEquals(false, cl.getIsHBase());
    assertEquals(false, cl.getIsHiveAux());
    assertEquals("javaHomeVal", cl.getJavaPath());
    assertEquals("queueVal", cl.getLlapQueueName());
    assertEquals("console", cl.getLogger());
    assertEquals("nameVal", cl.getName());
    assertEquals("outputVal", cl.getOutput());
    assertEquals(10L * 1024 * 1024, cl.getSize());
    assertEquals(10L * 1024 * 1024 * 1024, cl.getXmx());
    assertEquals(true, cl.isStarting());
  }

  @Test
  public void testIllegalLogger() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    new LlapServiceCommandLine(new String[] {"--instances", "1", "--logger", "someValue"});
  }

  @Test
  public void testIllegalInstances() throws Exception {
    thrown.expect(NumberFormatException.class);
    new LlapServiceCommandLine(new String[] {"--instances", "a"});
  }

  @Test
  public void testIllegalCache() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    new LlapServiceCommandLine(new String[] {"--instances", "1", "--cache", "a"});
  }

  @Test
  public void testIllegalExecutors() throws Exception {
    thrown.expect(NumberFormatException.class);
    new LlapServiceCommandLine(new String[] {"--instances", "1", "--executors", "a"});
  }

  @Test
  public void testIllegalIoThreads() throws Exception {
    thrown.expect(NumberFormatException.class);
    new LlapServiceCommandLine(new String[] {"--instances", "1", "--iothreads", "a"});
  }

  @Test
  public void testIllegalSize() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    new LlapServiceCommandLine(new String[] {"--instances", "1", "--size", "a"});
  }

  @Test
  public void testIllegalXmx() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    new LlapServiceCommandLine(new String[] {"--instances", "1", "--xmx", "a"});
  }
}
