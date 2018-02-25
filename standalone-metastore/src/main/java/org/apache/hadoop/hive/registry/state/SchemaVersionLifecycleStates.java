/*
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
package org.apache.hadoop.hive.registry.state;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.registry.CompatibilityResult;
import org.apache.hadoop.hive.registry.SchemaBranch;
import org.apache.hadoop.hive.registry.SchemaMetadata;
import org.apache.hadoop.hive.registry.SchemaMetadataInfo;
import org.apache.hadoop.hive.registry.SchemaValidationLevel;
import org.apache.hadoop.hive.registry.SchemaVersionInfo;
import org.apache.hadoop.hive.registry.errors.IncompatibleSchemaException;
import org.apache.hadoop.hive.registry.errors.SchemaBranchNotFoundException;
import org.apache.hadoop.hive.registry.errors.SchemaNotFoundException;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Schema version life cycle state flow is stored so that it can be seen how a specific version
 * changes its state till it finally reaches the terminal states. Once it reaches the terminal state, those entries
 * can be removed after a configured time. This may be useful for looking at changes of the schema version history.
 */
public final class SchemaVersionLifecycleStates {

    public static final InbuiltSchemaVersionLifecycleState INITIATED = new InitiatedState();
    public static final InbuiltSchemaVersionLifecycleState START_REVIEW = new StartReviewState();
    public static final InbuiltSchemaVersionLifecycleState CHANGES_REQUIRED = new ChangesRequiredState();
    public static final InbuiltSchemaVersionLifecycleState REVIEWED = new ReviewedState();
    public static final InbuiltSchemaVersionLifecycleState ENABLED = new EnabledState();
    public static final InbuiltSchemaVersionLifecycleState DISABLED = new DisabledState();
    public static final InbuiltSchemaVersionLifecycleState ARCHIVED = new ArchivedState();
    public static final InbuiltSchemaVersionLifecycleState DELETED = new DeletedState();

    private static final class InitiatedState extends AbstractInbuiltSchemaLifecycleState {

        private InitiatedState() {
            super("INITIATED", (byte) 1, "Schema version is initialized, It can either go to review or enabled states.");
        }

        @Override
        public void startReview(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, SchemaNotFoundException {
            transitionToStartReview(context);
        }

        @Override
        public void enable(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
            transitionToEnableState(context);
        }

        @Override
        public Collection<Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>> getTransitionActions() {
            return Lists.newArrayList(createStartReviewTransitionActionPair(getId()), createArchiveTransitionAction(getId()), createDeleteTransitionActionPair(getId()));
        }

        @Override
        public void delete(SchemaVersionLifecycleContext context) throws SchemaLifecycleException, SchemaNotFoundException {
            transitionToDeleteState(context);
        }

        @Override
        public String toString() {
            return "InitiatedState{" + super.toString() + "}";
        }
    }

    private static final class StartReviewState extends AbstractInbuiltSchemaLifecycleState {
        // schemaReviewExecutor is given successful and failure states.
        // it should invoke custom state and return to success/failure state eventually.

        private StartReviewState() {
            super("StartReview", (byte) 2, "Initiates the process for reviewing with the given custom state");
        }

        public void startReview(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, SchemaNotFoundException {
            transitionToStartReview(context);
        }

        @Override
        public Collection<Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>> getTransitionActions() {
            return Collections.emptyList();
        }

        @Override
        public String toString() {
            return "StartReviewState{" + super.toString() + "}";
        }

    }

    private static final class ChangesRequiredState extends AbstractInbuiltSchemaLifecycleState {

        public ChangesRequiredState() {
            super("ChangesRequired",
                  (byte) 3,
                  "Requires changes to be done in this schema"
                 );
        }

        @Override
        public void startReview(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, SchemaNotFoundException {
            transitionToStartReview(context);
        }

        @Override
        public void delete(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, SchemaNotFoundException {
            transitionToDeleteState(context);
        }

        @Override
        public Collection<Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>> getTransitionActions() {
            return Lists.newArrayList(createStartReviewTransitionActionPair(getId()), createDeleteTransitionActionPair(getId()));
        }

        @Override
        public String toString() {
            return "ChangesRequiredState{" + super.toString() + "}";
        }

    }

    private static final class ReviewedState extends AbstractInbuiltSchemaLifecycleState {

        private ReviewedState() {
            super("Reviewed",
                  (byte) 4,
                  "This schema version is successfully reviewed"
                 );
        }

        @Override
        public void enable(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
            transitionToEnableState(context);
        }

        @Override
        public void archive(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, SchemaNotFoundException {
            transitionToArchiveState(context);
        }

        @Override
        public Collection<Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>>
        getTransitionActions() {
            return Lists.newArrayList(createEnableTransitionAction(getId()), createArchiveTransitionAction(getId()));
        }

        @Override
        public String toString() {
            return "ReviewedState{" + super.toString() + "}";
        }

    }

    private static Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>
    createDeleteTransitionActionPair(
            Byte sourceStateId) {
        return Pair.of(new SchemaVersionLifecycleStateTransition(sourceStateId,
                                                                 DELETED.getId(),
                                                                 "Delete",
                                                                 "Deletes the schema version"),
                       context -> {
                           try {
                               transitionToDeleteState(context);
                           } catch (SchemaNotFoundException e) {
                               throw new SchemaLifecycleException(e);
                           }
                       });
    }

    private static Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>
    createStartReviewTransitionActionPair(Byte sourceStateId) {
        return Pair.of(new SchemaVersionLifecycleStateTransition(sourceStateId, START_REVIEW.getId(),
                                                                 "StartReview",
                                                                 "Starts review state"),
                       context -> {
                           try {
                               transitionToStartReview(context);
                           } catch (SchemaNotFoundException e) {
                               throw new SchemaLifecycleException(e);
                           }
                       });
    }


    private static Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>
    createDisableAction(Byte sourceStateId) {
        return Pair.of(new SchemaVersionLifecycleStateTransition(sourceStateId,
                                                                 DISABLED.getId(),
                                                                 "Disable",
                                                                 "Disables the schema version"),
                       context -> {
                           try {
                               transitionToDisableState(context);
                           } catch (SchemaNotFoundException e) {
                               throw new SchemaLifecycleException(e);
                           }
                       });
    }

    private static Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>
    createArchiveTransitionAction(Byte sourceStateId) {
        return Pair.of(new SchemaVersionLifecycleStateTransition(sourceStateId,
                                                                 ARCHIVED.getId(),
                                                                 "Archive",
                                                                 "Archives the schema version"),
                       context -> {
                           try {
                               transitionToArchiveState(context);
                           } catch (SchemaNotFoundException e) {
                               throw new SchemaLifecycleException(e);
                           }
                       });
    }

    private static Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>
    createEnableTransitionAction(Byte sourceStateId) {
        return Pair.of(new SchemaVersionLifecycleStateTransition(sourceStateId,
                                                                 ENABLED.getId(),
                                                                 "Enable",
                                                                 "Enables the schema version"),
                       context -> {
                           try {
                               transitionToEnableState(context);
                           } catch (SchemaNotFoundException | IncompatibleSchemaException | SchemaBranchNotFoundException e) {
                               throw new SchemaLifecycleException(e);
                           }
                       });
    }

    private static void transitionToDisableState(SchemaVersionLifecycleContext context) throws SchemaLifecycleException, SchemaNotFoundException {
        context.setState(DISABLED);
        context.updateSchemaVersionState();
    }

    private static void transitionToStartReview(SchemaVersionLifecycleContext context) throws SchemaLifecycleException, SchemaNotFoundException {
        context.setState(START_REVIEW);
        // execute start review process, updation of the state should be done by schemaReviewExecutor
        context.getCustomSchemaStateExecutor().executeReviewState(context);
    }

    private static void transitionToArchiveState(SchemaVersionLifecycleContext context) throws SchemaLifecycleException, SchemaNotFoundException {
        context.setState(ARCHIVED);
        context.updateSchemaVersionState();
    }

    private static void transitionToDeleteState(SchemaVersionLifecycleContext context) throws SchemaLifecycleException, SchemaNotFoundException {
        context.setState(DELETED);
        context.updateSchemaVersionState();
        context.getSchemaVersionService().deleteSchemaVersion(context.getSchemaVersionId());
    }

    private static final class EnabledState extends AbstractInbuiltSchemaLifecycleState {

        private EnabledState() {
            super("Enabled",
                  (byte) 5,
                  "Schema version is enabled"
                 );
        }

        @Override
        public void disable(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, SchemaNotFoundException {
            transitionToDisableState(context);
        }

        @Override
        public void archive(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, SchemaNotFoundException {
            transitionToArchiveState(context);
        }

        @Override
        public Collection<Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>> getTransitionActions() {
            return Lists.newArrayList(createDisableAction(getId()), createArchiveTransitionAction(getId()));

        }

        @Override
        public String toString() {
            return "EnabledState{" + super.toString() + "}";
        }

    }

    private static final class DisabledState extends AbstractInbuiltSchemaLifecycleState {
        private DisabledState() {
            super("Disabled",
                  (byte) 6,
                  "Schema version is disabled"
                 );
        }

        @Override
        public void enable(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
            transitionToEnableState(context);
        }

        @Override
        public void archive(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, SchemaNotFoundException {
            transitionToArchiveState(context);
        }

        @Override
        public Collection<Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>> getTransitionActions() {
            return Lists.newArrayList(createEnableTransitionAction(getId()), createArchiveTransitionAction(getId()));

        }

        @Override
        public String toString() {
            return "DisabledState{" + super.toString() + "}";
        }

    }

    private static void checkCompatibility(SchemaVersionService schemaVersionService,
                                           SchemaMetadata schemaMetadata,
                                           String toSchemaText,
                                           String fromSchemaText) throws IncompatibleSchemaException {
        CompatibilityResult compatibilityResult = schemaVersionService.checkForCompatibility(schemaMetadata,
                                                                                             toSchemaText,
                                                                                             fromSchemaText);
        if (!compatibilityResult.isCompatible()) {
            String errMsg = String.format("Given schema is not compatible with latest schema versions. \n" +
                                                  "Error location: [%s] \n" +
                                                  "Error encountered is: [%s]",
                                          compatibilityResult.getErrorLocation(),
                                          compatibilityResult.getErrorMessage());
            throw new IncompatibleSchemaException(errMsg);
        }
    }

    private static final class ArchivedState extends AbstractInbuiltSchemaLifecycleState {
        // TERMINAL STATE
        private ArchivedState() {
            super("Archived",
                  (byte) 7,
                  "Schema is archived and it is a terminal state"
                 );
        }

        @Override
        public void delete(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaLifecycleException, SchemaNotFoundException {
            transitionToDeleteState(schemaVersionLifecycleContext);
        }

        @Override
        public String toString() {
            return "ArchivedState{" + super.toString() + "}";
        }

        @Override
        public Collection<Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>> getTransitionActions() {
            return Collections.emptyList();
        }
    }

    private static final class DeletedState extends AbstractInbuiltSchemaLifecycleState {
        // TERMINAL STATE
        private DeletedState() {
            super("Deleted",
                  (byte) 8,
                  "Schema is deleted and it is a terminal state"
                 );
        }

        @Override
        public String toString() {
            return "DeletedState{" + super.toString() + "}";
        }

        @Override
        public Collection<Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>> getTransitionActions() {
            return Collections.emptyList();
        }
    }

    public static void transitionToEnableState(SchemaVersionLifecycleContext context)
            throws SchemaNotFoundException, IncompatibleSchemaException, SchemaLifecycleException, SchemaBranchNotFoundException {
        Long schemaVersionId = context.getSchemaVersionId();
        SchemaVersionService schemaVersionService = context.getSchemaVersionService();

        SchemaMetadataInfo schemaMetadataInfo = schemaVersionService.getSchemaMetadata(schemaVersionId);
        SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
        String schemaName = schemaMetadata.getName();
        SchemaValidationLevel validationLevel = schemaMetadata.getValidationLevel();

        SchemaVersionInfo schemaVersionInfo = schemaVersionService.getSchemaVersionInfo(schemaVersionId);
        int schemaVersion = schemaVersionInfo.getVersion();
        String schemaText = schemaVersionInfo.getSchemaText();
        List<SchemaVersionInfo> allEnabledSchemaVersions =
                schemaVersionService.getAllSchemaVersions(SchemaBranch.MASTER_BRANCH, schemaName)
                                    .stream()
                                    .filter(x -> SchemaVersionLifecycleStates.ENABLED.getId().equals(x.getStateId()))
                                    .collect(Collectors.toList());

        if (!allEnabledSchemaVersions.isEmpty()) {
            if (validationLevel.equals(SchemaValidationLevel.ALL)) {
                for (SchemaVersionInfo curSchemaVersionInfo : allEnabledSchemaVersions) {
                    int curVersion = curSchemaVersionInfo.getVersion();
                    if (curVersion < schemaVersion) {
                        checkCompatibility(schemaVersionService,
                                           schemaMetadata,
                                           schemaText,
                                           curSchemaVersionInfo.getSchemaText());
                    } else {
                        checkCompatibility(schemaVersionService,
                                           schemaMetadata,
                                           curSchemaVersionInfo.getSchemaText(),
                                           schemaText);
                    }
                }
            } else if (validationLevel.equals(SchemaValidationLevel.LATEST)) {
                List<SchemaVersionInfo> sortedSchemaVersionInfos = new ArrayList<>(allEnabledSchemaVersions);
                sortedSchemaVersionInfos.sort(Comparator.comparingInt(SchemaVersionInfo::getVersion));
                int i = 0;
                int size = sortedSchemaVersionInfos.size();
                for (; i < size && sortedSchemaVersionInfos.get(i).getVersion() < schemaVersion; i++) {
                    String fromSchemaText = sortedSchemaVersionInfos.get(i).getSchemaText();
                    checkCompatibility(schemaVersionService, schemaMetadata, schemaText, fromSchemaText);
                }
                for (; i < size && sortedSchemaVersionInfos.get(i).getVersion() > schemaVersion; i++) {
                    String toSchemaText = sortedSchemaVersionInfos.get(i).getSchemaText();
                    checkCompatibility(schemaVersionService, schemaMetadata, toSchemaText, schemaText);
                }
            }
        }
        context.setState(ENABLED);
        context.updateSchemaVersionState();
    }
}
