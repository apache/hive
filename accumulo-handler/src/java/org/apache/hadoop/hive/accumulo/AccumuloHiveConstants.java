/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo;

import java.nio.charset.Charset;

/**
 *
 */
public class AccumuloHiveConstants {
  public static final String ROWID = ":rowID";
  public static final char COLON = ':', COMMA = ',', ESCAPE = '\\', POUND = '#', ASTERISK = '*';

  public static final String ESCAPED_COLON = Character.toString(ESCAPE) + Character.toString(COLON);

  // Escape the escape
  public static final String ESCAPED_COLON_REGEX = Character.toString(ESCAPE)
      + Character.toString(ESCAPE) + Character.toString(COLON);

  public static final String ESCAPED_ASTERISK = Character.toString(ESCAPE)
      + Character.toString(ASTERISK);

  // Escape the escape, and escape the asterisk
  public static final String ESCAPED_ASTERISK_REGEX = Character.toString(ESCAPE)
      + Character.toString(ESCAPE) + Character.toString(ESCAPE) + Character.toString(ASTERISK);

  public static final Charset UTF_8 = Charset.forName("UTF-8");
}
