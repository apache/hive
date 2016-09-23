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

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.SearchResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.apache.hive.service.auth.ldap.LdapTestUtils.*;

@RunWith(MockitoJUnitRunner.class)
public class TestSearchResultHandler {

  SearchResultHandler handler;

  @Test
  public void testHandle() throws NamingException {
    MockResultCollection resultCollection = MockResultCollection.create()
        .addSearchResultWithDns("1")
        .addSearchResultWithDns("2", "3");
    handler = new SearchResultHandler(resultCollection);
    List<String> expected = Arrays.asList("1", "2");
    final List<String> actual = new ArrayList<>();
    handler.handle(new SearchResultHandler.RecordProcessor() {
      @Override
      public boolean process(SearchResult record) throws NamingException {
        actual.add(record.getNameInNamespace());
        return actual.size() < 2;
      }
    });
    assertEquals(expected, actual);
    assertAllNamingEnumerationsClosed(resultCollection);
  }

  @Test
  public void testGetAllLdapNamesNoRecords() throws NamingException {
    MockResultCollection resultCollection = MockResultCollection.create()
        .addEmptySearchResult();
    handler = new SearchResultHandler(resultCollection);
    List<String> actual = handler.getAllLdapNames();
    assertEquals("Resultset size", 0, actual.size());
    assertAllNamingEnumerationsClosed(resultCollection);
  }

  @Test
  public void testGetAllLdapNamesWithExceptionInNamingEnumerationClose() throws NamingException {
    MockResultCollection resultCollection = MockResultCollection.create()
        .addSearchResultWithDns("1")
        .addSearchResultWithDns("2");
    doThrow(NamingException.class).when(resultCollection.iterator().next()).close();
    handler = new SearchResultHandler(resultCollection);
    List<String> actual = handler.getAllLdapNames();
    assertEquals("Resultset size", 2, actual.size());
    assertAllNamingEnumerationsClosed(resultCollection);
  }

  @Test
  public void testGetAllLdapNames() throws NamingException {
    String objectDn1 = "cn=a1,dc=b,dc=c";
    String objectDn2 = "cn=a2,dc=b,dc=c";
    String objectDn3 = "cn=a3,dc=b,dc=c";
    MockResultCollection resultCollection = MockResultCollection.create()
        .addSearchResultWithDns(objectDn1)
        .addSearchResultWithDns(objectDn2, objectDn3);
    handler = new SearchResultHandler(resultCollection);
    List<String> expected = Arrays.asList(objectDn1, objectDn2, objectDn3);
    Collections.sort(expected);
    List<String> actual = handler.getAllLdapNames();
    Collections.sort(actual);
    assertEquals(expected, actual);
    assertAllNamingEnumerationsClosed(resultCollection);
  }

  @Test
  public void testGetAllLdapNamesAndAttributes() throws NamingException {
    SearchResult searchResult1 = mockSearchResult("cn=a1,dc=b,dc=c",
        mockAttributes("attr1", "attr1value1"));
    SearchResult searchResult2 = mockSearchResult("cn=a2,dc=b,dc=c",
        mockAttributes("attr1", "attr1value2", "attr2", "attr2value1"));
    SearchResult searchResult3 = mockSearchResult("cn=a3,dc=b,dc=c",
        mockAttributes("attr1", "attr1value3", "attr1", "attr1value4"));
    SearchResult searchResult4 = mockSearchResult("cn=a4,dc=b,dc=c",
        mockEmptyAttributes());

    MockResultCollection resultCollection = MockResultCollection.create()
        .addSearchResults(searchResult1)
        .addSearchResults(searchResult2, searchResult3)
        .addSearchResults(searchResult4);

    handler = new SearchResultHandler(resultCollection);
    List<String> expected = Arrays.asList(
        "cn=a1,dc=b,dc=c", "attr1value1",
        "cn=a2,dc=b,dc=c", "attr1value2", "attr2value1",
        "cn=a3,dc=b,dc=c", "attr1value3", "attr1value4",
        "cn=a4,dc=b,dc=c");
    Collections.sort(expected);
    List<String> actual = handler.getAllLdapNamesAndAttributes();
    Collections.sort(actual);
    assertEquals(expected, actual);
    assertAllNamingEnumerationsClosed(resultCollection);
  }

  @Test
  public void testHasSingleResultNoRecords() throws NamingException {
    MockResultCollection resultCollection = MockResultCollection.create()
        .addEmptySearchResult();
    handler = new SearchResultHandler(resultCollection);
    assertFalse(handler.hasSingleResult());
    assertAllNamingEnumerationsClosed(resultCollection);
  }

  @Test
  public void testHasSingleResult() throws NamingException {
    MockResultCollection resultCollection = MockResultCollection.create()
        .addSearchResultWithDns("1");
    handler = new SearchResultHandler(resultCollection);
    assertTrue(handler.hasSingleResult());
    assertAllNamingEnumerationsClosed(resultCollection);
  }

  @Test
  public void testHasSingleResultManyRecords() throws NamingException {
    MockResultCollection resultCollection = MockResultCollection.create()
        .addSearchResultWithDns("1")
        .addSearchResultWithDns("2");
    handler = new SearchResultHandler(resultCollection);
    assertFalse(handler.hasSingleResult());
    assertAllNamingEnumerationsClosed(resultCollection);
  }

  @Test(expected = NamingException.class)
  public void testGetSingleLdapNameNoRecords() throws NamingException {
    MockResultCollection resultCollection = MockResultCollection.create()
        .addEmptySearchResult();
    handler = new SearchResultHandler(resultCollection);
    try {
      handler.getSingleLdapName();
    } finally {
      assertAllNamingEnumerationsClosed(resultCollection);
    }
  }

  @Test
  public void testGetSingleLdapName() throws NamingException {
    String objectDn = "cn=a,dc=b,dc=c";
    MockResultCollection resultCollection = MockResultCollection.create()
        .addEmptySearchResult()
        .addSearchResultWithDns(objectDn);

    handler = new SearchResultHandler(resultCollection);
    String expected = objectDn;
    String actual = handler.getSingleLdapName();
    assertEquals(expected, actual);
    assertAllNamingEnumerationsClosed(resultCollection);
  }

  private void assertAllNamingEnumerationsClosed(MockResultCollection resultCollection) throws NamingException {
    for (NamingEnumeration<SearchResult> namingEnumeration : resultCollection) {
      verify(namingEnumeration, atLeastOnce()).close();
    }
  }

  private static final class MockResultCollection extends AbstractCollection<NamingEnumeration<SearchResult>> {

    List<NamingEnumeration<SearchResult>> results = new ArrayList<>();

    static MockResultCollection create() {
      return new MockResultCollection();
    }

    MockResultCollection addSearchResultWithDns(String... dns) throws NamingException {
      results.add(mockNamingEnumeration(dns));
      return this;
    }

    MockResultCollection addSearchResults(SearchResult... dns) throws NamingException {
      results.add(mockNamingEnumeration(dns));
      return this;
    }

    MockResultCollection addEmptySearchResult() throws NamingException {
      addSearchResults();
      return this;
    }

    @Override
    public Iterator<NamingEnumeration<SearchResult>> iterator() {
      return results.iterator();
    }

    @Override
    public int size() {
      return results.size();
    }
  }
}
