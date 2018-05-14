/*
 * Copyright 2016 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.registry.client;

import com.google.common.base.Joiner;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

/**
 *
 */
public class LoadBalancedFailoverUrlSelectorTest {

    @Test
    public void testFailoverUrls() throws Exception {
        String[] urls = new String[5];
        for (int i = 0; i < urls.length; i++) {
            urls[i] = ("localhost:808" + i);
        }
        String clusterUrl = Joiner.on(",").join(urls);
        LoadBalancedFailoverUrlSelector failoverUrlSelector = new LoadBalancedFailoverUrlSelector(clusterUrl);
        failoverUrlSelector.init(Collections.emptyMap());

        for (int i = 0; i < urls.length; i++) {
            String current = failoverUrlSelector.select();
            Assert.assertEquals(urls[i], current);
        }

        failoverUrlSelector.urlWithError(urls[0], new IOException());
        failoverUrlSelector.urlWithError(urls[1], new IOException());
        failoverUrlSelector.urlWithError(urls[2], new IOException());

        String current = failoverUrlSelector.select();
        Assert.assertEquals(urls[3], current);

        current = failoverUrlSelector.select();
        Assert.assertEquals(urls[4], current);

    }
}
