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
package org.apache.hadoop.hive.metastore.registry.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class defines a strategy to roundrobin among active URLs. When all URLs are considered failed which are maintained in
 * the cache then it invalidates the cache and all urls are considered active.
 */
public class LoadBalancedFailoverUrlSelector extends AbstractUrlSelector {

    /**
     * Property to configure  time interval at which failed URL may be considered active.
     */
    public static final String FAILED_URL_EXPIRY_INTERVAL_MS = "failed.url.expiry.interval.ms";

    private Cache<String, Boolean> failedUrls;

    private AtomicInteger index = new AtomicInteger();

    public LoadBalancedFailoverUrlSelector(String clusterUrl) {
        super(clusterUrl);
    }

    @Override
    public void init(Map<String, Object> conf) {
        super.init(conf);
        failedUrls = CacheBuilder.newBuilder().expireAfterWrite((Long) conf.getOrDefault(FAILED_URL_EXPIRY_INTERVAL_MS, 5 * 60 * 1000L),
                                                                TimeUnit.MILLISECONDS).build();
    }

    @Override
    public String select() {
        String url = null;
        while (true) {
            int i = index.get();
            if (index.compareAndSet(i, (i + 1) % urls.length)) {
                url = urls[i];

                if (failedUrls.getIfPresent(url) == null) {
                    break;
                }
            }
        }

        return url;
    }

    @Override
    public void urlWithError(String url, Exception e) {
        if (failedError(e)) {
            //mark this url as failed.
            failedUrls.put(url, true);
            if (failedUrls.size() == urls.length) {
                // simple assumption to consider all of them as active to try out the existing failed URLs.
                failedUrls.invalidateAll();
            }
        }
    }

    /**
     * Returns true if the given Exception indicates the respective URL can be treated as failed.
     *
     * @param ex
     */
    protected boolean failedError(Exception ex) {
        return true;
    }

}
