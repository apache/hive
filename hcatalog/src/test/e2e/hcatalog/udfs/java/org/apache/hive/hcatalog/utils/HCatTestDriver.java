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

package org.apache.hive.hcatalog.utils;

import org.apache.hadoop.util.ProgramDriver;

/**
 * A description of an example program based on its class and a 
 * human-readable description.
 */
public class HCatTestDriver {

  public static void main(String argv[]) {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("typedatacheck", TypeDataCheck.class,
        "A map/reduce program that checks the type of each field and" +
          " outputs the entire table (to test hcat).");
      pgd.addClass("sumnumbers", SumNumbers.class,
        "A map/reduce program that performs a group by on the first column and a " +
          "SUM operation on the other columns of the \"numbers\" table.");
      pgd.addClass("storenumbers", StoreNumbers.class, "A map/reduce program that " +
        "reads from the \"numbers\" table and adds 10 to each fields and writes " +
        "to the \"numbers_partitioned\" table into the datestamp=20100101 " +
        "partition OR the \"numbers_empty_initially\" table based on a " +
        "cmdline arg");
      pgd.addClass("storecomplex", StoreComplex.class, "A map/reduce program that " +
        "reads from the \"complex\" table and stores as-is into the " +
        "\"complex_empty_initially\" table.");
      pgd.addClass("storedemo", StoreDemo.class, "demo prog.");
      pgd.driver(argv);

      // Success
      exitCode = 0;
    } catch (Throwable e) {
      e.printStackTrace();
    }

    System.exit(exitCode);
  }
}
