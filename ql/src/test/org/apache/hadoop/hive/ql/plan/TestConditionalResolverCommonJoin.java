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

package org.apache.hadoop.hive.ql.plan;

import org.junit.Assert;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ddl.DDLTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestConditionalResolverCommonJoin {

  @Test
  public void testResolvingDriverAlias() throws Exception {
    ConditionalResolverCommonJoin resolver = new ConditionalResolverCommonJoin();

    Map<Path, List<String>> pathToAliases = new HashMap<>();
    pathToAliases.put(new Path("path1"), new ArrayList<String>(Arrays.asList("alias1", "alias2")));
    pathToAliases.put(new Path("path2"), new ArrayList<String>(Arrays.asList("alias3")));

    HashMap<String, Long> aliasToKnownSize = new HashMap<String, Long>();
    aliasToKnownSize.put("alias1", 1024l);
    aliasToKnownSize.put("alias2", 2048l);
    aliasToKnownSize.put("alias3", 4096l);

    DDLTask task1 = new DDLTask();
    task1.setId("alias2");
    DDLTask task2 = new DDLTask();
    task2.setId("alias3");

    // joins alias1, alias2, alias3 (alias1 was not eligible for big pos)
    // Must be deterministic order map for consistent q-test output across Java versions
    HashMap<Task<?>, Set<String>> taskToAliases =
        new LinkedHashMap<Task<?>, Set<String>>();
    taskToAliases.put(task1, new HashSet<String>(Arrays.asList("alias2")));
    taskToAliases.put(task2, new HashSet<String>(Arrays.asList("alias3")));

    ConditionalResolverCommonJoin.ConditionalResolverCommonJoinCtx ctx =
        new ConditionalResolverCommonJoin.ConditionalResolverCommonJoinCtx();
    ctx.setPathToAliases(pathToAliases);
    ctx.setTaskToAliases(taskToAliases);
    ctx.setAliasToKnownSize(aliasToKnownSize);

    HiveConf conf = new HiveConf();
    conf.setLongVar(HiveConf.ConfVars.HIVE_SMALL_TABLES_FILESIZE, 4096);

    // alias3 only can be selected
    Task resolved = resolver.resolveMapJoinTask(ctx, conf);
    Assert.assertEquals("alias3", resolved.getId());

    conf.setLongVar(HiveConf.ConfVars.HIVE_SMALL_TABLES_FILESIZE, 65536);

    // alias1, alias2, alias3 all can be selected but overriden by biggest one (alias3)
    resolved = resolver.resolveMapJoinTask(ctx, conf);
    Assert.assertEquals("alias3", resolved.getId());

    conf.setLongVar(HiveConf.ConfVars.HIVE_SMALL_TABLES_FILESIZE, 2048);

    // not selected
    resolved = resolver.resolveMapJoinTask(ctx, conf);
    Assert.assertNull(resolved);
  }
}
