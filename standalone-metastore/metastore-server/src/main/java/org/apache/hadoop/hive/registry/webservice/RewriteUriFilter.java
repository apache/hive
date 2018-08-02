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
package org.apache.hadoop.hive.registry.webservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;


public class RewriteUriFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(RewriteUriFilter.class);

    private FilterConfig filterConfig;
    private Map<String, String> forwardPaths;
    private Map<String, String> redirectPaths;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        this.filterConfig = filterConfig;
        forwardPaths = buildRewritePathsMap(filterConfig.getInitParameter("forwardPaths"));
        redirectPaths = buildRewritePathsMap(filterConfig.getInitParameter("redirectPaths"));
    }

    private Map<String, String> buildRewritePathsMap(String pathsStr) {
        if(pathsStr == null || pathsStr.trim().isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> pathsMapper = new LinkedHashMap<>();
        StringTokenizer tokenizer = new StringTokenizer(pathsStr, "|");
        while (tokenizer.hasMoreTokens()) {
            StringTokenizer st = new StringTokenizer(tokenizer.nextToken(), ",");
            List<String> configPaths = new ArrayList<>();
            while (st.hasMoreTokens()) {
                configPaths.add(st.nextToken());
            }
            String targetPath = configPaths.remove(0);
            configPaths.forEach(x -> pathsMapper.put(x, targetPath));
        }
        return pathsMapper;
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        if (!handleRewriteUris(servletRequest, servletResponse)) {
            LOG.debug("No rewrite or forward path found, ");
            filterChain.doFilter(servletRequest, servletResponse);
        }
    }

    private boolean handleRewriteUris(ServletRequest servletRequest, ServletResponse servletResponse)
            throws ServletException, IOException {
        String reqPath = ((HttpServletRequest) servletRequest).getRequestURI();

        String redirectPath = findRewritePath(reqPath, redirectPaths);
        LOG.debug("request path [{}], redirectPath [{}]", reqPath, redirectPath);
        if(redirectPath != null) {
            LOG.info("Redirecting request [{}] to [{}]", servletRequest, redirectPath);
            ((HttpServletResponse) servletResponse).sendRedirect(redirectPath);
            return true;
        } else {
            String forwardPath = findRewritePath(reqPath, forwardPaths);
            LOG.debug("request path [{}], forwardPath [{}]", reqPath, forwardPath);
            if (forwardPath != null) {
                LOG.info("Forwarding request [{}] to [{}]", servletRequest, forwardPath);
                servletRequest.getRequestDispatcher(forwardPath + reqPath)
                              .forward(servletRequest, servletResponse);
                return true;
            }
        }

        return false;
    }

    private String findRewritePath(String reqPath, Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String path = entry.getKey();

            if (matchesPath(reqPath, path)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private boolean matchesPath(String reqPath, String path) {
        char lastChar = path.charAt(path.length() - 1);
        if (lastChar == '*') {
            int lastIdx = path.length() - 1;
            if (path.length() > 2 && path.charAt(path.length() - 2) == '/') {
                lastIdx = path.length() - 2;
            }
            return reqPath.startsWith(path.substring(0, lastIdx));
        } else {
            return reqPath.equals(path);
        }
    }

    @Override
    public void destroy() {
    }

}
