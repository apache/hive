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
 * Implementation of a parse tree processor. This processor does a depth first walk
 * of the parse tree and calls the associated event processors
 */
public class DefaultASTProcessor implements ASTProcessor {

	/**
	 * The dispatcher used to dispatch ParseTreeEvents to the ParseTreeEventProcessors
	 */
	private ASTEventDispatcher dispatcher;
	
	/**
	 * Processes the parse tree
	 * 
	 * @see org.apache.hadoop.hive.ql.parse.ASTProcessor#process(org.antlr.runtime.tree.CommonTree)
	 */
	@Override
	public void process(CommonTree ast) {

		// Base case
		if (ast.getToken() == null) {
			return;
		}

		switch (ast.getToken().getType()) {
		case HiveParser.TOK_SELECTDI:
		case HiveParser.TOK_SELECT:
			dispatcher.dispatch(ASTEvent.SELECT_CLAUSE, ast);
			break;

		case HiveParser.TOK_WHERE:
			dispatcher.dispatch(ASTEvent.WHERE_CLAUSE, ast);
			break;

		case HiveParser.TOK_DESTINATION:
			dispatcher.dispatch(ASTEvent.DESTINATION, ast);
			break;

		case HiveParser.TOK_FROM:

			// Check if this is a subquery
			CommonTree frm = (CommonTree) ast.getChild(0);
			if (frm.getToken().getType() == HiveParser.TOK_TABREF) {
				dispatcher.dispatch(ASTEvent.SRC_TABLE, ast);
			} else if (frm.getToken().getType() == HiveParser.TOK_SUBQUERY) {
				dispatcher.dispatch(ASTEvent.SUBQUERY, ast);
			} else if (ParseUtils.isJoinToken(frm)) {
				dispatcher.dispatch(ASTEvent.JOIN_CLAUSE, ast);
			}
			break;

		case HiveParser.TOK_CLUSTERBY:
			dispatcher.dispatch(ASTEvent.CLUSTERBY_CLAUSE, ast);
			break;

		case HiveParser.TOK_GROUPBY:
			dispatcher.dispatch(ASTEvent.GROUPBY_CLAUSE, ast);
			break;

		case HiveParser.TOK_LIMIT:
			dispatcher.dispatch(ASTEvent.LIMIT_CLAUSE, ast);
			break;
		default:
			break;
		}

		// Iterate over the rest of the children
		int child_count = ast.getChildCount();
		for (int child_pos = 0; child_pos < child_count; ++child_pos) {
			// Recurse
			process((CommonTree) ast.getChild(child_pos));
		}
	}

	/**
	 * Sets the dispatcher for the parse tree processor
	 * 
	 * @see org.apache.hadoop.hive.ql.parse.ASTProcessor#register(org.apache.hadoop.hive.ql.parse.ASTEvent, org.apache.hadoop.hive.ql.parse.ParseTreeEventProcessor)
	 */
	@Override
	public void setDispatcher(ASTEventDispatcher dispatcher) {
		
		this.dispatcher = dispatcher;
	}

}
