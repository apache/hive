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
package org.apache.hive.service.auth.ldap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchResult;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.stubbing.OngoingStubbing;

public final class LdapTestUtils {

  private LdapTestUtils() {
  }

  public static NamingEnumeration<SearchResult> mockEmptyNamingEnumeration() throws NamingException {
    return mockNamingEnumeration(new SearchResult[0]);
  }

  public static NamingEnumeration<SearchResult> mockNamingEnumeration(String... dns) throws NamingException {
    return mockNamingEnumeration(mockSearchResults(dns).toArray(new SearchResult[0]));
  }

  public static NamingEnumeration<SearchResult> mockNamingEnumeration(SearchResult... searchResults) throws NamingException {
    NamingEnumeration<SearchResult> ne =
        (NamingEnumeration<SearchResult>) mock(NamingEnumeration.class);
    mockHasMoreMethod(ne, searchResults.length);
    if (searchResults.length > 0) {
      List<SearchResult> mockedResults = Arrays.asList(searchResults);
      mockNextMethod(ne, mockedResults);
    }
    return ne;
  }

  public static void mockHasMoreMethod(NamingEnumeration<SearchResult> ne, int length) throws NamingException {
    OngoingStubbing<Boolean> hasMoreStub = when(ne.hasMore());
    for (int i = 0; i < length; i++) {
      hasMoreStub = hasMoreStub.thenReturn(true);
    }
    hasMoreStub.thenReturn(false);
  }

  public static void mockNextMethod(NamingEnumeration<SearchResult> ne, List<SearchResult> searchResults) throws NamingException {
    OngoingStubbing<SearchResult> nextStub = when(ne.next());
    for (SearchResult searchResult : searchResults) {
      nextStub = nextStub.thenReturn(searchResult);
    }
  }

  public static List<SearchResult> mockSearchResults(String[] dns) {
    List<SearchResult> list = new ArrayList<>();
    for (String dn : dns) {
      list.add(mockSearchResult(dn, null));
    }
    return list;
  }

  public static SearchResult mockSearchResult(String dn, Attributes attributes) {
    SearchResult searchResult = mock(SearchResult.class);
    when(searchResult.getNameInNamespace()).thenReturn(dn);
    when(searchResult.getAttributes()).thenReturn(attributes);
    return searchResult;
  }

  public static Attributes mockEmptyAttributes() throws NamingException {
    return mockAttributes();
  }

  public static Attributes mockAttributes(String name, String value) throws NamingException {
    return mockAttributes(new NameValues(name, value));
  }

  public static Attributes mockAttributes(String name1, String value1, String name2, String value2) throws NamingException {
    if (name1.equals(name2)) {
      return mockAttributes(new NameValues(name1, value1, value2));
    } else {
      return mockAttributes(new NameValues(name1, value1), new NameValues(name2, value2));
    }
  }

  private static Attributes mockAttributes(NameValues... namedValues) throws NamingException {
    Attributes attributes =  new BasicAttributes();
    for (NameValues namedValue : namedValues) {
      Attribute attr = new BasicAttribute(namedValue.name);
      for (String value : namedValue.values) {
        attr.add(value);
      }
      attributes.put(attr);
    }
    return attributes;
  }

  private static final class NameValues {
    final String name;
    final String[] values;

    public NameValues(String name, String... values) {
      this.name = name;
      this.values = values;
    }
  }
}
