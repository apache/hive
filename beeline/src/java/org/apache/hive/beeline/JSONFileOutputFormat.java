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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;

/**
 * OutputFormat for hive JSON file format.
 * Removes "{ "resultset": [...] }" wrapping and prints one object per line.
 * This output format matches the same format as a Hive table created with JSONFILE file format:
 * CREATE TABLE ... STORED AS JSONFILE;
 * e.g.
 * {"name":"Ritchie Tiger","age":40,"is_employed":true,"college":"RIT"}
 * {"name":"Bobby Tables","age":8,"is_employed":false,"college":null}
 * ...
 * 
 * Note the lack of "," at the end of lines.
 * 
 */ 
public class JSONFileOutputFormat extends JSONOutputFormat {
  

  JSONFileOutputFormat(BeeLine beeLine) {
    super(beeLine);
    this.generator.setPrettyPrinter(new MinimalPrettyPrinter("\n"));    
  }

  @Override
  void printHeader(Rows.Row header) {}

  @Override
  void printFooter(Rows.Row header) {
    ByteArrayOutputStream buf = (ByteArrayOutputStream) generator.getOutputTarget();
    try {
      generator.flush();
      String out = buf.toString(StandardCharsets.UTF_8.name());
      beeLine.output(out);
    } catch(IOException e) {
      beeLine.handleException(e);
    }
    buf.reset();
  }
}
