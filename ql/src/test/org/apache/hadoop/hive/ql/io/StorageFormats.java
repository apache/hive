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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;

import static org.junit.Assert.assertTrue;

/**
 * Utility class for enumerating Hive native storage formats for testing. Native Storage formats
 * are registered via {@link org.apache.hadoop.hive.ql.io.StorageFormatDescriptor}.
 */
public class StorageFormats {
  /**
   * Table of additional storage formats. These are SerDes or combinations of SerDe with
   * InputFormat and OutputFormat that are not registered as a native Hive storage format.
   *
   * Each row in this table has the following fields:
   *  - formatName - A string name for the storage format. This is used to give the table created
   *    for the test a unique name.
   *  - serdeClass - The name of the SerDe class used by the storage format.
   *  - inputFormatClass - The name of the InputFormat class.
   *  - outputFormatClass - The name of the OutputFormat class.
   */
  public static final Object[][] ADDITIONAL_STORAGE_FORMATS = new Object[][] {
    {
      "rcfile_columnar",
      ColumnarSerDe.class.getName(),
      RCFileInputFormat.class.getName(),
      RCFileOutputFormat.class.getName(),
    }
  };

  /**
   * Create an array of Objects used to populate the test paramters.
   *
   * @param name Name of the storage format.
   * @param serdeClass Name of the SerDe class.
   * @param inputFormatClass Name of the InputFormat class.
   * @param outputFormatClass Name of the OutputFormat class.
   * @return Object array containing the arguments.
   */
  protected static Object[] createTestArguments(String name, String serdeClass,
      String inputFormatClass, String outputFormatClass) {
    Object[] args = {
      name,
      serdeClass,
      inputFormatClass,
      outputFormatClass
    };
    return args;
  }

  /**
   * Generates a collection of parameters that can be used as paramters for a JUnit test fixture.
   * Each parameter represents one storage format that the fixture will run against. The list
   * includes both native Hive storage formats as well as those enumerated in the
   * ADDITIONAL_STORAGE_FORMATS table.
   *
   * @return List of storage format as a Collection of Object arrays, each containing (in order):
   *         Storage format name, SerDe class name, InputFormat class name, OutputFormat class name.
   *         This list is used as the parameters to JUnit parameterized tests.
   */
  public static Collection<Object[]> asParameters() {
    List<Object[]> parameters = new ArrayList<Object[]>();

    // Add test parameters from official storage formats registered with Hive via
    // StorageFormatDescriptor.
    final Configuration conf = new Configuration();
    for (StorageFormatDescriptor descriptor : ServiceLoader.load(StorageFormatDescriptor.class)) {
      String serdeClass = descriptor.getSerde();
      if (serdeClass == null) {
        if (descriptor instanceof RCFileStorageFormatDescriptor) {
          serdeClass = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_DEFAULT_RCFILE_SERDE);
        } else {
          serdeClass = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_DEFAULT_SERDE);
        }
      }

      String[] names = new String[descriptor.getNames().size()];
      names = descriptor.getNames().toArray(names);
      Object[] arguments = createTestArguments(names[0], serdeClass, descriptor.getInputFormat(),
          descriptor.getOutputFormat());
      parameters.add(arguments);
    }

    // Add test parameters from storage formats specified in ADDITIONAL_STORAGE_FORMATS table.
    for (int i = 0; i < ADDITIONAL_STORAGE_FORMATS.length; i++) {
      String serdeClass = (String) ADDITIONAL_STORAGE_FORMATS[i][1];
      String name = (String) ADDITIONAL_STORAGE_FORMATS[i][0];
      String inputFormatClass = (String) ADDITIONAL_STORAGE_FORMATS[i][2];
      String outputFormatClass = (String) ADDITIONAL_STORAGE_FORMATS[i][3];
      assertTrue("InputFormat for storage format not set", inputFormatClass != null);
      assertTrue("OutputFormat for storage format not set", outputFormatClass != null);
      Object[] arguments = createTestArguments(name, serdeClass, inputFormatClass,
          outputFormatClass);
      parameters.add(arguments);
    }

    return parameters;
  }

  /**
   * Returns a list of the names of storage formats.
   *
   * @return List of names of storage formats.
   */
  public static Collection<Object[]> names() {
    List<Object[]> names = new ArrayList<Object[]>();
    for (StorageFormatDescriptor descriptor : ServiceLoader.load(StorageFormatDescriptor.class)) {
      String[] formatNames = new String[descriptor.getNames().size()];
      formatNames = descriptor.getNames().toArray(formatNames);
      String[] params = { formatNames[0] };
      names.add(params);
    }
    return names;
  }
}

