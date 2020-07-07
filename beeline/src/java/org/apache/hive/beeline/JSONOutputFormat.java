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

/*
 * This source file is based on code taken from SQLLine 1.9
 * See SQLLine notice in LICENSE
 */
package org.apache.hive.beeline;

import java.sql.SQLException;
import java.sql.Types;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

/**
 * OutputFormat for standard JSON format.
 *
 */ 
public class JSONOutputFormat extends AbstractOutputFormat {
  protected final BeeLine beeLine;
  protected JsonGenerator generator;
  

  /**
   * @param beeLine
   */
  JSONOutputFormat(BeeLine beeLine){ 
    this.beeLine = beeLine;
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try{
      this.generator = new JsonFactory().createGenerator(buf, JsonEncoding.UTF8);
    }catch(IOException e){
      beeLine.handleException(e);
    }
  }

  @Override 
  void printHeader(Rows.Row header) {
    try {
      generator.writeStartObject();
      generator.writeArrayFieldStart("resultset");
    } catch (IOException e) {
      beeLine.handleException(e);
    }
  }

  @Override 
  void printFooter(Rows.Row header) {
    try {
      generator.writeEndArray();
      generator.writeEndObject();
      beeLine.output(generator.getOutputTarget().toString());
      generator.flush();
    } catch (IOException e) {
      beeLine.handleException(e);
    }
  }

  @Override 
  void printRow(Rows rows, Rows.Row header, Rows.Row row) {
    String[] head = header.values;
    String[] vals = row.values;
    try{
      for (int i = 0; (i < head.length) && (i < vals.length); i++) {
        generator.writeFieldName(head[i]);
        switch(rows.rsMeta.getColumnType(i)) {
          case Types.TINYINT:
          case Types.SMALLINT:
          case Types.INTEGER:
          case Types.BIGINT:
          case Types.REAL:
          case Types.FLOAT:
          case Types.DOUBLE:
          case Types.DECIMAL:
          case Types.NUMERIC:
            generator.writeNumber(vals[i]);
            break;
          case Types.NULL:
            generator.writeNull();
            break;
          case Types.BOOLEAN:
            generator.writeBoolean(vals[i].equalsIgnoreCase("true"));
            break;
          default:
            generator.writeString(vals[i]);
        }
      }
    } catch (IOException e){
      beeLine.handleException(e);
    } catch (SQLException e){
      beeLine.handleSQLException(e);
    }
  }
}
