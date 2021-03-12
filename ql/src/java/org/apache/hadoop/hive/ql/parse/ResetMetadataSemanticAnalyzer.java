package org.apache.hadoop.hive.ql.parse;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.engine.EngineWork;

import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.ql.metadata.HiveUtils.unparseIdentifier;

/*
 Semantic analyzer for reset table metadata commands like REFRESH/INVALIDATE
 For now it works only for REFRESH command
 */
public class ResetMetadataSemanticAnalyzer extends SemanticAnalyzer {
    public ResetMetadataSemanticAnalyzer(QueryState queryState) throws SemanticException {
        super(queryState);
    }

    @Override
    public void analyzeInternal(ASTNode root) throws SemanticException {
        LOG.debug("In Reset metadata semantic analyzer");
        assert root.getType() == HiveParser.TOK_REFRESH_TABLE;
        ASTNode tableNode = (ASTNode) root.getChild(0);
        String tableNameString = getUnescapedName((ASTNode) tableNode.getChild(0));
        Map<String, String> partitionSpec = getPartSpec((ASTNode) tableNode.getChild(1));
        Table table = getTable(tableNameString, true);

        checkEligibility(table, partitionSpec, tableNode);
        LOG.debug("Reset metadata analysis completed");
        super.analyzeInternal(root);
    }

    private void checkEligibility(Table tbl, Map<String, String> partitionSpec, ASTNode tableNode)
            throws SemanticException {
        assert tbl.getTTable() != null;
        if(!isImpalaPlan(conf)) {
            throw new SemanticException("REFRESH command works only for Impala execution Engine");
        }
        if(!MetaStoreUtils.isExternalTable(tbl.getTTable())) {
            String message = "Table " + tbl.getFullyQualifiedName() + " is not an external table. " +
                    "Refresh is allowed only for external tables";
            throw new SemanticException(message);
        }
        // if partitonSpec is specified than validate that all partitions are specified
        if(partitionSpec != null) {
            validatePartSpec(tbl, partitionSpec, tableNode, conf, true);
        }
        return;
    }

}
