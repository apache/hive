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
package org.apache.hadoop.hive.serde2.text;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;

/**
 * A Hive Serializer-Deserializer (SerDe) that supports delimited-separated
 * values (DSV) files. The delimited used is supplied by the user with the table
 * definition.
 *
 * A DSV file is a simple, widely supported, text format for storing data in a
 * tabular structure. Each record in the table is one line of a text file. Each
 * field value of a record is separated from the next by an arbitrary string of
 * characters.
 *
 * This SerDe can be configured to handle different error scenarios.
 * <ol>
 * <li>Blank line in the text file</li>
 * <li>Too many fields in the record</li>
 * <li>Too few fields in the record</li>
 * </ol>
 *
 * The default behavior of this class is to be tolerant of such issues to avoid
 * query failures. Issues are handled in a reasonable way. All data is presented
 * to the user even if it is invalid: <br/>
 * <br/>
 * <ol>
 * <li>If a blank line is encountered, all fields are set to a null value</li>
 * <li>If there are too many fields, the extra fields are appended to the final
 * value</li>
 * <li>If there are too few fields, all remaining fields are set to null
 * value</li>
 * </ol>
 *
 * <b>Examples</b>
 *
 * <pre>
 * "1,2,3"   = ["1","2","3"]
 * "1,2,"    = ["1","2",null]
 * ""        = [null,null,null]
 * "1,2,3,4" = ["1","2","3,4"]
 * </pre>
 *
 * Several configurations exist to cause an exception to be raised if one of
 * these situations is encountered.
 *
 * @see CsvDelimitedTextSerDe
 * @see AbstractDelimitedTextSerDe
 */
@SerDeSpec(schemaProps = { serdeConstants.LIST_COLUMNS,
    serdeConstants.SERIALIZATION_ENCODING, serdeConstants.FIELD_DELIM,
    AbstractTextSerDe.IGNORE_EMPTY_LINES,
    AbstractDelimitedTextSerDe.STRICT_FIELDS_COUNT })
public class DelimitedTextSerDe extends AbstractDelimitedTextSerDe {

  @Override
  public void initialize(Configuration configuration,
      Properties tableProperties) throws SerDeException {
    final String delim =
        tableProperties.getProperty(serdeConstants.FIELD_DELIM);

    if (StringUtils.isBlank(delim)) {
      throw new SerDeException("Value cannot be blank for configuration "
          + serdeConstants.FIELD_DELIM);
    }

    super.setDelim(delim);
    super.initialize(configuration, tableProperties);
  }

}
