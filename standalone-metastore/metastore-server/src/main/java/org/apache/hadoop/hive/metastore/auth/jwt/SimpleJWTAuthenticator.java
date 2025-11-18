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

package org.apache.hadoop.hive.metastore.auth.jwt;

import com.google.common.collect.Sets;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.proc.BadJOSEException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleJWTAuthenticator {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleJWTAuthenticator.class.getName());
  private static final Set<JOSEObjectType> ACCEPTABLE_TYPES = Sets.newHashSet(null, JOSEObjectType.JWT);

  private final JWTValidator validator;

  public static SimpleJWTAuthenticator create(Configuration conf) throws IOException {
    final var plainJwksURLs = MetastoreConf.getStringCollection(conf,
        ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL);
    if (plainJwksURLs.isEmpty()) {
      throw new IOException("Invalid value of property: " +
          ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL.getHiveName());
    }
    final List<URL> jwksURLs = new ArrayList<>(plainJwksURLs.size());
    for (String url : plainJwksURLs) {
      jwksURLs.add(URI.create(url).toURL());
      LOG.info("Loaded JWKS from {}", url);
    }
    final var validator = new JWTValidator(ACCEPTABLE_TYPES, jwksURLs, null, null, Collections.singleton("sub"));
    return new SimpleJWTAuthenticator(validator);
  }

  public SimpleJWTAuthenticator(JWTValidator validator) {
    this.validator = validator;
  }

  public String resolveUserName(String bearerToken) throws ParseException, BadJOSEException, JOSEException {
    return validator.validateJWT(bearerToken).getSubject();
  }
}
