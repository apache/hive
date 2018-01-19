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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.DistCpOptions;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestHadoop23Shims {

  @Test
  public void testConstructDistCpParams() {
    Path copySrc = new Path("copySrc");
    Path copyDst = new Path("copyDst");
    Configuration conf = new Configuration();

    Hadoop23Shims shims = new Hadoop23Shims();
    List<String> paramsDefault = shims.constructDistCpParams(Collections.singletonList(copySrc), copyDst, conf);

    assertEquals(5, paramsDefault.size());
    assertTrue("Distcp -update set by default", paramsDefault.contains("-update"));
    assertTrue("Distcp -skipcrccheck set by default", paramsDefault.contains("-skipcrccheck"));
    assertTrue("Distcp -pb set by default", paramsDefault.contains("-pb"));
    assertEquals(copySrc.toString(), paramsDefault.get(3));
    assertEquals(copyDst.toString(), paramsDefault.get(4));

    conf.set("distcp.options.foo", "bar"); // should set "-foo bar"
    conf.set("distcp.options.blah", ""); // should set "-blah"
    conf.set("dummy", "option"); // should be ignored.
    List<String> paramsWithCustomParamInjection =
        shims.constructDistCpParams(Collections.singletonList(copySrc), copyDst, conf);

    assertEquals(5, paramsWithCustomParamInjection.size());

    // check that the defaults did not remain.
    assertTrue("Distcp -update not set if not requested",
        !paramsWithCustomParamInjection.contains("-update"));
    assertTrue("Distcp -skipcrccheck not set if not requested",
        !paramsWithCustomParamInjection.contains("-skipcrccheck"));
    assertTrue("Distcp -pb not set if not requested",
        !paramsWithCustomParamInjection.contains("-pb"));

    // the "-foo bar" and "-blah" params order is not guaranteed
    String firstParam = paramsWithCustomParamInjection.get(0);
    if (firstParam.equals("-foo")){
      // "-foo bar -blah"  form
      assertEquals("bar", paramsWithCustomParamInjection.get(1));
      assertEquals("-blah", paramsWithCustomParamInjection.get(2));
    } else {
      // "-blah -foo bar" form
      assertEquals("-blah", paramsWithCustomParamInjection.get(0));
      assertEquals("-foo", paramsWithCustomParamInjection.get(1));
      assertEquals("bar", paramsWithCustomParamInjection.get(2));
    }

    // the dummy option should not have made it either - only options
    // beginning with distcp.options. should be honoured
    assertTrue(!paramsWithCustomParamInjection.contains("dummy"));
    assertTrue(!paramsWithCustomParamInjection.contains("-dummy"));
    assertTrue(!paramsWithCustomParamInjection.contains("option"));
    assertTrue(!paramsWithCustomParamInjection.contains("-option"));

    assertEquals(copySrc.toString(), paramsWithCustomParamInjection.get(3));
    assertEquals(copyDst.toString(), paramsWithCustomParamInjection.get(4));

  }

}
