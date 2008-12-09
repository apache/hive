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

package org.apache.hadoop.hive.ql.parse;

/**
 * Enumeration that encapsulates the various event types that are seen
 * while processing the parse tree (in an implementation of the ParseTreeProcessor).
 * These event types are used to register the different event processors with
 * the parse tree processor.
 *
 */
public enum ASTEvent {
	
	/**
	 * Query event
	 */
	QUERY("QUERY"),
	
	/**
	 * Union
	 */
	UNION("UNION"),
	
	/**
	 * Source Table (table in the from clause)
	 */
	SRC_TABLE("SRC_TABLE"),

	/**
	 * Any type of Destination (this fires for hdfs directory, local directory and table)
	 */
	DESTINATION("DESTINATION"),
	
	/**
	 * Select clause
	 */
	SELECT_CLAUSE("SELECT_CLAUSE"),
	
	/**
	 * Join clause
	 */
	JOIN_CLAUSE("JOIN_CLAUSE"),
	
	/**
	 * Where clause
	 */
	WHERE_CLAUSE("WHERE_CLAUSE"),
	
	/**
	 * CLusterby clause
	 */
	CLUSTERBY_CLAUSE("CLUSTERBY_CLAUSE"),
	
	/**
	 * Group by clause
	 */
	GROUPBY_CLAUSE("GROUPBY_CLAUSE"),
	
	/**
	 * Limit clause
	 */
	LIMIT_CLAUSE("LIMIT_CLAUSE"),
	
	/**
	 * Subquery
	 */
	SUBQUERY("SUBQUERY");
	
	/**
	 * The name of the event (string representation of the event)
	 */
	private final String name;
	
	/**
	 * Constructs the event
	 * 
	 * @param name The name(String representation of the event)
	 */
	ASTEvent(String name) {
      this.name = name;
	}
	
	/**
	 * String representation of the event
	 * 
	 * @return String
	 */
	@Override
	public String toString() {
		return name;
	}
}
