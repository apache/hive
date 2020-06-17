package org.apache.hadoop.hive.ql.reexec;

import com.cronutils.utils.VisibleForTesting;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

public class ReExecuteLostAMQueryPlugin implements IReExecutionPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(ReExecuteLostAMQueryPlugin.class);
    private boolean retryPossible;

    // Lost am container have exit code -100, due to node failures.
    private Pattern lostAMContainerErrorPattern = Pattern.compile(".*AM Container for .* exited .* exitCode: -100.*");

    class LocalHook implements ExecuteWithHookContext {

        @Override
        public void run(HookContext hookContext) throws Exception {
            if (hookContext.getHookType() == HookContext.HookType.ON_FAILURE_HOOK) {
                Throwable exception = hookContext.getException();

                if (exception != null && exception.getMessage() != null
                        && lostAMContainerErrorPattern.matcher(exception.getMessage()).matches()) {
                    retryPossible = true;
                }
            }
        }
    }
    @Override
    public void initialize(Driver driver) {
        driver.getHookRunner().addOnFailureHook(new LocalHook());
    }

    @Override
    public void beforeExecute(int executionIndex, boolean explainReOptimization) {
    }

    @Override
    public boolean shouldReExecute(int executionNum) {
        return retryPossible;
    }

    @Override
    public void prepareToReExecute() {
    }

    @Override
    public boolean shouldReExecute(int executionNum, PlanMapper oldPlanMapper, PlanMapper newPlanMapper) {
        return retryPossible;
    }

    @Override
    public void afterExecute(PlanMapper planMapper, boolean successfull) {
    }
}
