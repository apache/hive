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

package org.apache.hadoop.hive.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestHadoop23Shims {

  @Test
  public void testConstructDistCpParams() {
    Path copySrc = new Path("copySrc");
    Path copyDst = new Path("copyDst");
    Configuration conf = new Configuration();

    Hadoop23Shims shims = new Hadoop23Shims();
    List<String> paramsDefault = shims.constructDistCpParams(Collections.singletonList(copySrc), copyDst, conf);

    assertEquals(5, paramsDefault.size());
    assertTrue("Distcp -pbx set by default", paramsDefault.contains("-pbx"));
    assertTrue("Distcp -update set by default", paramsDefault.contains("-update"));
    assertTrue("Distcp -delete set by default", paramsDefault.contains("-delete"));
    assertEquals(copySrc.toString(), paramsDefault.get(3));
    assertEquals(copyDst.toString(), paramsDefault.get(4));

    conf.set("distcp.options.foo", "bar"); // should set "-foo bar"
    conf.set("distcp.options.blah", ""); // should set "-blah"
    conf.set("distcp.options.pug", ""); // should set "-pug"
    conf.set("dummy", "option"); // should be ignored.
    List<String> paramsWithCustomParamInjection =
        shims.constructDistCpParams(Collections.singletonList(copySrc), copyDst, conf);

    assertEquals(8, paramsWithCustomParamInjection.size());

    // check that the mandatory ones remain along with user passed ones.
    assertTrue("Distcp -update set even if not requested",
        paramsWithCustomParamInjection.contains("-update"));
    assertTrue("Distcp -delete set even if not requested",
            paramsWithCustomParamInjection.contains("-delete"));
    assertTrue("Distcp -foo is set as passes",
            paramsWithCustomParamInjection.contains("-foo"));
    assertTrue("Distcp -blah is set as passes",
            paramsWithCustomParamInjection.contains("-blah"));
    assertTrue("Distcp -pug is set as passes",
            paramsWithCustomParamInjection.contains("-pug"));
    assertTrue("Distcp -pbx not set as overridden",
            !paramsWithCustomParamInjection.contains("-pbx"));
    assertTrue("Distcp -skipcrccheck not set if not requested",
        !paramsWithCustomParamInjection.contains("-skipcrccheck"));

    // the "-foo bar" order is guaranteed
    int idx = paramsWithCustomParamInjection.indexOf("-foo");
    assertEquals("bar", paramsWithCustomParamInjection.get(idx+1));

    // the dummy option should not have made it either - only options
    // beginning with distcp.options. should be honoured
    assertTrue(!paramsWithCustomParamInjection.contains("dummy"));
    assertTrue(!paramsWithCustomParamInjection.contains("-dummy"));
    assertTrue(!paramsWithCustomParamInjection.contains("option"));
    assertTrue(!paramsWithCustomParamInjection.contains("-option"));

    assertEquals(copySrc.toString(), paramsWithCustomParamInjection.get(6));
    assertEquals(copyDst.toString(), paramsWithCustomParamInjection.get(7));

  }

}
