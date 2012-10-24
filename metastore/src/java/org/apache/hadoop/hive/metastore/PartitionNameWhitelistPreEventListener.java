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

package org.apache.hadoop.hive.metastore;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.events.PreAddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;

public class PartitionNameWhitelistPreEventListener extends MetaStorePreEventListener {

  // When enabled, this hook causes an exception to be thrown
  // if partition fields contain characters which are not
  // matched by the whitelist

  private static String regex;
  private static Pattern pattern;

  public PartitionNameWhitelistPreEventListener(Configuration config) {
    super(config);

    regex = config.get(HiveConf.ConfVars.METASTORE_PARTITION_NAME_WHITELIST_PATTERN.varname);
    pattern = Pattern.compile(regex);
  }

  @Override
  public void onEvent(PreEventContext event) throws MetaException, NoSuchObjectException,
      InvalidOperationException {

    switch (event.getEventType()) {
    case ADD_PARTITION:
      checkWhitelist(((PreAddPartitionEvent) event).getPartition().getValues());
      break;
    }

  }

  private static void checkWhitelist(List<String> partVals) throws MetaException {
    for (String partVal : partVals) {
      if (!pattern.matcher(partVal).matches()) {
        throw new MetaException("Partition value '" + partVal + "' contains a character "
            + "not matched by whitelist pattern '" + regex + "'.  " + "(configure with "
            + HiveConf.ConfVars.METASTORE_PARTITION_NAME_WHITELIST_PATTERN.varname + ")");
      }
    }
  }

}
