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
package org.apache.hadoop.hive.cli;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// Cannot call class TestCliDriver since that's the name of the generated
// code for the script-based testing
public class TestCliDriverMethods extends TestCase {

  // If the command has an associated schema, make sure it gets printed to use
  public void testThatCliDriverPrintsHeaderForCommandsWithSchema() throws CommandNeedRetryException {
    Schema mockSchema = mock(Schema.class);
    List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
    String fieldName = "FlightOfTheConchords";
    fieldSchemas.add(new FieldSchema(fieldName, "type", "comment"));

    when(mockSchema.getFieldSchemas()).thenReturn(fieldSchemas);

    PrintStream mockOut = headerPrintingTestDriver(mockSchema);
    // Should have printed out the header for the field schema
    verify(mockOut, times(1)).print(fieldName);
  }

  // If the command has no schema, make sure nothing is printed
  public void testThatCliDriverPrintsNoHeaderForCommandsWithNoSchema() throws CommandNeedRetryException {
    Schema mockSchema = mock(Schema.class);
    when(mockSchema.getFieldSchemas()).thenReturn(null);

    PrintStream mockOut = headerPrintingTestDriver(mockSchema);
    // Should not have tried to print any thing.
    verify(mockOut, never()).print(anyString());
  }

  /**
   * Do the actual testing against a mocked CliDriver based on what type of schema
   * @param mockSchema Schema to throw against test
   * @return Output that would have been sent to the user
   * @throws CommandNeedRetryException won't actually be thrown
   */
  private PrintStream headerPrintingTestDriver(Schema mockSchema) throws CommandNeedRetryException {
    CliDriver cliDriver = new CliDriver();

    // We want the driver to try to print the header...
    Configuration conf = mock(Configuration.class);
    when(conf.getBoolean(eq(ConfVars.HIVE_CLI_PRINT_HEADER.varname), anyBoolean())).thenReturn(true);
    cliDriver.setConf(conf);

    Driver proc = mock(Driver.class);

    CommandProcessorResponse cpr = mock(CommandProcessorResponse.class);
    when(cpr.getResponseCode()).thenReturn(0);
    when(proc.run(anyString())).thenReturn(cpr);

    // and then see what happens based on the provided schema
    when(proc.getSchema()).thenReturn(mockSchema);

    CliSessionState mockSS = mock(CliSessionState.class);
    PrintStream mockOut = mock(PrintStream.class);

    mockSS.out = mockOut;

    cliDriver.processLocalCmd("use default;", proc, mockSS);
    return mockOut;
  }

}
