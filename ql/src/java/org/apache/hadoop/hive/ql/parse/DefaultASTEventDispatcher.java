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

import java.util.HashMap;
import java.util.ArrayList;

import org.antlr.runtime.tree.CommonTree;

/**
 * Implementation of a default ParseTreeEventDispatcher. This dispatcher calls
 * the associated ParseTreeEventProcessors in the order in which they were
 * registered for the event
 *
 */
public class DefaultASTEventDispatcher implements
		ASTEventDispatcher {
	
	/**
	 * Stores the mapping from the ParseTreeEvent to the list of ParseTreeEventProcessors.
	 * The later are stored in the order that they were registered.
	 */
	private HashMap<ASTEvent, ArrayList<ASTEventProcessor>> dispatchMap;

	/**
	 * Constructs the default event dispatcher
	 */
	public  DefaultASTEventDispatcher() {
		dispatchMap = new HashMap<ASTEvent, ArrayList<ASTEventProcessor>>();
	}
	
	/**
	 * Dispatches the parse subtree to all the event processors registered for the
	 * event in the order that they were registered.
	 * 
	 * @see org.apache.hadoop.hive.ql.parse.ASTEventDispatcher#dispatch(org.apache.hadoop.hive.ql.parse.ASTEvent, org.antlr.runtime.tree.CommonTree)
	 */
	@Override
	public void dispatch(ASTEvent evt, CommonTree pt) {
		
		ArrayList<ASTEventProcessor> evtp_l = dispatchMap.get(evt);
		if (evtp_l == null) {
			return;
		}

		for(ASTEventProcessor evt_p: evtp_l) {
			// Do the actual dispatch
			evt_p.process(pt);
		}
	}

	/**
	 * Registers the event processor for the event.
	 * 
	 * @see org.apache.hadoop.hive.ql.parse.ASTEventDispatcher#register(org.apache.hadoop.hive.ql.parse.ASTEvent, org.apache.hadoop.hive.ql.parse.ASTEventProcessor)
	 */
	@Override
	public void register(ASTEvent evt, ASTEventProcessor evt_p) {

		ArrayList<ASTEventProcessor> evtp_l = dispatchMap.get(evt);
		if (evtp_l == null) {
			evtp_l = new ArrayList<ASTEventProcessor>();
			dispatchMap.put(evt, evtp_l);
		}

		evtp_l.add(evt_p);
	}
}
