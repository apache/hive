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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hcatalog.data.HCatRecord;

import java.io.IOException;

/**
 * Interface used to define conversion of HCatRecord to and from Native HBase write (Put) and read (Result) objects.
 * How the actual mapping is defined between an HBase Table's schema and an HCatalog Table's schema
 * is up to the underlying implementation
 */
interface ResultConverter {

  /**
   * convert HCatRecord instance to an HBase Put, used when writing out data.
   * @param record instance to convert
   * @return converted Put instance
   * @throws IOException
   */
  Put convert(HCatRecord record) throws IOException;

  /**
   * convert HBase Result to HCatRecord instance, used when reading data.
   * @param result instance to convert
   * @return converted Result instance
   * @throws IOException
   */
  HCatRecord convert(Result result) throws IOException;

  /**
   * Returns the hbase columns that are required for the scan.
   * @return String containing hbase columns delimited by space.
   * @throws IOException
   */
  String getHBaseScanColumns() throws IOException;

}
