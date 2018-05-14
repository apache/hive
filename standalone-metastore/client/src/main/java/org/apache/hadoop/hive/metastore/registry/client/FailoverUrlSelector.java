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

import java.util.PriorityQueue;

/**
 * This class defines strategy to failover to a url when the current chosen url is considered to be failed.
 */
public class FailoverUrlSelector extends AbstractUrlSelector {

    private final PriorityQueue<UrlTimeEntry> failedUrls;
    private String current;

    public FailoverUrlSelector(String clusterUrl) {
        super(clusterUrl);
        failedUrls = new PriorityQueue<>();
        current = urls[0];
    }

    @Override
    public String select() {
        return current;
    }

    @Override
    public void urlWithError(String url, Exception e) {
        if (failedError(e)) {
            synchronized (failedUrls) {
                failedUrls.add(new UrlTimeEntry(url, System.currentTimeMillis()));
                if (failedUrls.size() == urls.length) {
                    current = failedUrls.remove().url;
                } else if (current.equals(url)) {
                    for (String s : urls) {
                        if (!failedUrls.contains(new UrlTimeEntry(s, System.currentTimeMillis()))) {
                            current = s;
                            break;
                        }
                    }
                }
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

    private static class UrlTimeEntry implements Comparable<UrlTimeEntry> {
        final String url;
        private final long time;

        public UrlTimeEntry(String url, long time) {
            this.url = url;
            this.time = time;
        }

        @Override
        public int compareTo(UrlTimeEntry other) {
            int x = 0;
            if (this.time == other.time) {
                x = 0;
            } else if (time < other.time) {
                x = -1;
            } else {
                x = 1;
            }

            return x;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            UrlTimeEntry urlTimeEntry = (UrlTimeEntry) o;

            return url != null ? url.equals(urlTimeEntry.url) : urlTimeEntry.url == null;
        }

        @Override
        public int hashCode() {
            return url != null ? url.hashCode() : 0;
        }
    }
}
