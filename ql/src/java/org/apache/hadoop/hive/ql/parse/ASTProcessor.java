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
 * Interface that a parse tree processor needs to implement
 */
public interface ASTProcessor {
  
	/**
	 * Sets the event dispatcher for the processors
	 * 
	 * @param dispatcher The parse tree event dispatcher
	 */
	void setDispatcher(ASTEventDispatcher dispatcher);
	
	/**
	 * Processes the parse tree and calls the registered event processors
	 * for the associated parse tree events
	 * 
	 * @param pt The parse tree to process
	 */
	void process(CommonTree pt);
}

