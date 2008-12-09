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
 * Dispatches ParseTreeEvent to the appropriate ParseTreeEventProcessor
 */
public interface ASTEventDispatcher {
	
	/**
	 * Registers the event processor with the event
	 * 
	 * @param evt The parse tree event
	 * @param evt_p The associated parse tree event processor
	 */
	void register(ASTEvent evt, ASTEventProcessor evt_p);
	
	/**
	 * Dispatches the parse tree event to a registered event processor
	 * 
	 * @param evt The parse tree event to dispatch
	 * @param pt The parse subtree to dispatch to the event processor
	 */
	void dispatch(ASTEvent evt, CommonTree pt);
}
