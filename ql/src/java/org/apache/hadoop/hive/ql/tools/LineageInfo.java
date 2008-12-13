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


package org.apache.hadoop.hive.ql.tools;

import java.io.IOException;
import java.util.TreeSet;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.ASTEvent;
import org.apache.hadoop.hive.ql.parse.ASTEventProcessor;
import org.apache.hadoop.hive.ql.parse.DefaultASTEventDispatcher;
import org.apache.hadoop.hive.ql.parse.DefaultASTProcessor;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * 
 * This class prints out the lineage info. 
 * It takes sql as input and prints lineage info.
 * Currently this prints only input and output tables for a given sql. 
 *  Later we can expand to add join tables etc.
 *
 */
public class LineageInfo  implements ASTEventProcessor {

	/**
	 * Stores input tables in sql
	 */
	TreeSet<String> inputTableList = new TreeSet<String>();
	/**
	 * Stores output tables in sql
	 */
	TreeSet<String> OutputTableList= new TreeSet<String>();

	/**
	 * 
	 * @return java.util.TreeSet 
	 */
	public TreeSet<String> getInputTableList() {
		return inputTableList;
	}

	/**
	 * @return java.util.TreeSet
	 */
	public TreeSet<String> getOutputTableList() {
		return OutputTableList;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.ql.parse.ASTEventProcessor#process(org.antlr.runtime.tree.CommonTree)
	 */
	public void process(CommonTree pt) {

		switch (pt.getToken().getType()) {

		case HiveParser.TOK_DESTINATION: {
			if (pt.getChild(0).getType() == HiveParser.TOK_TAB) {
				OutputTableList.add(pt.getChild(0).getChild(0).getText())	;
			}

		}
		break;
		case HiveParser.TOK_FROM: {
			CommonTree tabRef = (CommonTree) pt.getChild(0);
			String table_name = tabRef.getChild(0).getText();
			inputTableList.add(table_name);
		}
		break;
		}
	}
	/**
	 *  parses given query and gets the lineage info.
	 * @param query
	 * @throws ParseException
	 */
	public void getLineageInfo(String query) throws ParseException
	{

		/*
		 *  Get the AST tree
		 */
		ParseDriver pd = new ParseDriver();
		CommonTree tree = pd.parse(query);

		while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
			tree = (CommonTree) tree.getChild(0);
		}

		/*
		 * initialize Event Processor and dispatcher.
		 */
		inputTableList.clear();
		OutputTableList.clear();
		DefaultASTEventDispatcher dispatcher = new DefaultASTEventDispatcher();
		dispatcher.register(ASTEvent.SRC_TABLE, this);
		dispatcher.register(ASTEvent.DESTINATION, this);

		DefaultASTProcessor eventProcessor = new DefaultASTProcessor();

		eventProcessor.setDispatcher(dispatcher);
		eventProcessor.process(tree);
	}

	public static void main(String[] args) throws IOException, ParseException,
	SemanticException {

		String query = args[0];

		LineageInfo lep = new LineageInfo();

		lep.getLineageInfo(query);

		for (String tab : lep.getInputTableList()) {
			System.out.println("InputTable=" + tab);
		}

		for (String tab : lep.getOutputTableList()) {
			System.out.println("OutputTable=" + tab);
		}
	}
}
