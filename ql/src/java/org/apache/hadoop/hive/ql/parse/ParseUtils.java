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

import org.antlr.runtime.tree.CommonTree;

/**
 * Library of utility functions used in the parse code
 *
 */
public class ParseUtils {
	
	/**
	 * Tests whether the parse tree node is a join token
	 * 
	 * @param node The parse tree node
	 * @return boolean
	 */
	public static boolean isJoinToken(CommonTree node) {
		if ((node.getToken().getType() == HiveParser.TOK_JOIN)
				|| (node.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN)
				|| (node.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN)
				|| (node.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN))
			return true;

		return false;
	}
}
