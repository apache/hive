/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.registry.storage.core.search;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchQuery implements Serializable {
    private static final long serialVersionUID = 3394075873934901992L;
    private String nameSpace;
    private List<OrderBy> orderByFields;
    private WhereClause whereClause;

    private SearchQuery() {
    }

    private SearchQuery(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    public static SearchQuery searchFrom(String nameSpace) {
        return new SearchQuery(nameSpace);
    }

    public SearchQuery where(WhereClause whereClause) {
        if(this.whereClause != null) {
            throw new IllegalArgumentException("where clause is already defined for this search query.");
        }
        this.whereClause = whereClause;
        return this;
    }

    public SearchQuery orderBy(OrderBy... orderByFields) {
        if(this.orderByFields != null) {
            throw new IllegalArgumentException("orderBy fields are already defined for this search query.");
        }

        this.orderByFields = Collections.unmodifiableList(Arrays.asList(orderByFields));
        return this;
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public List<OrderBy> getOrderByFields() {
        return orderByFields;
    }

    public WhereClause getWhereClause() {
        return whereClause;
    }

    @Override
    public String toString() {
        return "SearchQuery{" +
                "nameSpace='" + nameSpace + '\'' +
                ", orderByFields=" + orderByFields +
                ", clause=" + whereClause +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SearchQuery that = (SearchQuery) o;

        if (nameSpace != null ? !nameSpace.equals(that.nameSpace) : that.nameSpace != null) return false;
        if (orderByFields != null ? !orderByFields.equals(that.orderByFields) : that.orderByFields != null)
            return false;
        return whereClause != null ? whereClause.equals(that.whereClause) : that.whereClause == null;
    }

    @Override
    public int hashCode() {
        int result = nameSpace != null ? nameSpace.hashCode() : 0;
        result = 31 * result + (orderByFields != null ? orderByFields.hashCode() : 0);
        result = 31 * result + (whereClause != null ? whereClause.hashCode() : 0);
        return result;
    }

    public static void main(String[] args) {
        SearchQuery searchQuery = SearchQuery.searchFrom("store")
                .where(WhereClause.begin()
                               .contains("name", "foo")
                               .and()
                               .gt("amount", 500)
                       .combine()
                ).orderBy(OrderBy.asc("name"), OrderBy.desc("amount"));


    }

}
