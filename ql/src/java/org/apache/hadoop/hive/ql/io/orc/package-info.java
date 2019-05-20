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

/**
 * The Optimized Row Columnar (ORC) File Format.
 *
 * This format:
 * <ul>
 *   <li>Decomposes complex column types into primitives</li>
 *   <li>Uses type-specific encoders for each column
 *     <ul>
 *       <li>Dictionary encodings for low cardinality columns</li>
 *       <li>Run length encoding of data</li>
 *       <li>variable length encoding of integers</li>
 *     </ul>
 *   </li>
 *   <li>Divides file into large stripes</li>
 *   <li>Each stripe includes light-weight indexes that enable the reader to
 *     skip large sets of rows that don't satisfy the filter condition</li>
 *   <li>A file footer that contains meta-information about file
 *     <ul>
 *       <li>Precise byte range for each stripe</li>
 *       <li>Type information for the file</li>
 *       <li>Any user meta-information</li>
 *     </ul>
 *   </li>
 *   <li>Seek to row number is implemented to support secondary indexes</li>
 *   <li>Support for additional generic compression: LZO, SNAPPY, ZLIB.</li>
 * </ul>
 *
 * <br>
 * <b>Format:</b>
 * <pre>
 * {@code
 * HEADER (3 bytes) "ORC"
 * STRIPE (0 or more stripes)
 * FILE-FOOTER
 * POST SCRIPT
 * PS LENGTH (1 byte)
 * }
 * </pre>
 *
 * <br>
 * <b>Stripe:</b>
 * <pre>
 * {@code
 * INDEX-STREAM (0 or more)
 * DATA-STREAM (0 or more)
 * STRIPE-FOOTER
 * }
 * </pre>
 */
package org.apache.hadoop.hive.ql.io.orc;
