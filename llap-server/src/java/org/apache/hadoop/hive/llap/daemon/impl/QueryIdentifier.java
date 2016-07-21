/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.llap.daemon.impl;

/**
 * An identifier for a query, which is unique.
 *
 * This is based on the AppId and dagId.
 *
 * At the moment, It's possible for Hive to use the same "hive.query.id" if a single query
 * is split into stages - which prevents this identifier being used as the unique id.
 *
 * Not exposing getters to allow this to evolve - i.e. retain the uniqueness constraint.
 *
 */
public final class QueryIdentifier {

  private final String appIdentifier;
  private final int dagIdentifier;


  public QueryIdentifier(String appIdentifier, int dagIdentifier) {
    this.appIdentifier = appIdentifier;
    this.dagIdentifier = dagIdentifier;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().isAssignableFrom(o.getClass())) {
      return false;
    }

    QueryIdentifier that = (QueryIdentifier) o;

    if (dagIdentifier != that.dagIdentifier) {
      return false;
    }
    return appIdentifier.equals(that.appIdentifier);

  }

  @Override
  public int hashCode() {
    int result = appIdentifier.hashCode();
    result = 31 * result + dagIdentifier;
    return result;
  }

  @Override
  public String toString() {
    return "QueryIdentifier{" +
        "appIdentifier='" + appIdentifier + '\'' +
        ", dagIdentifier=" + dagIdentifier +
        '}';
  }
}
