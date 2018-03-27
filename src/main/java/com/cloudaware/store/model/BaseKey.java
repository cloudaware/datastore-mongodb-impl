/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudaware.store.model;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static com.cloudaware.store.model.Validator.validateDatabase;
import static com.cloudaware.store.model.Validator.validateKind;
import static com.cloudaware.store.model.Validator.validateNamespace;

/**
 * Base class for keys.
 */
public abstract class BaseKey implements Serializable {

    private static final long serialVersionUID = -5897863400209818325L;

    private final String projectId;
    private final String namespace;
    private final ImmutableList<PathElement> path;

    BaseKey(final String projectId, final String namespace, final ImmutableList<PathElement> path) {
        Preconditions.checkArgument(!path.isEmpty(), "Path must not be empty");
        this.projectId = projectId;
        this.namespace = namespace;
        this.path = path;
    }

    /**
     * Returns the key's projectId.
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * Returns the key's namespace or {@code null} if not provided.
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Returns an immutable list with the key's ancestors.
     */
    public List<PathElement> getAncestors() {
        return getPath().subList(0, getPath().size() - 1);
    }

    /**
     * Returns an immutable list of the key's path (ancestors + self).
     */
    final List<PathElement> getPath() {
        return path;
    }

    /**
     * get last Path Element
     */
    final PathElement getLeaf() {
        return getPath().get(getPath().size() - 1);
    }

    /**
     * Returns the key's kind.
     */
    public String getKind() {
        return getLeaf().getKind();
    }

    abstract BaseKey getParent();

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("projectId", projectId)
                .add("namespace", namespace)
                .add("path", path)
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getProjectId(), getNamespace(), getPath());
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof BaseKey)) {
            return false;
        }
        final BaseKey other = (BaseKey) obj;
        return Objects.equals(getProjectId(), other.getProjectId())
                && Objects.equals(getNamespace(), other.getNamespace())
                && Objects.equals(getPath(), other.getPath());
    }

    /**
     * Base class for key builders.
     *
     * @param <B> the key builder.
     */
    public abstract static class Builder<B extends Builder<B>> {

        private static final int MAX_PATH = 100;
        final List<PathElement> ancestors;
        String projectId = "";
        String namespace = "";
        String kind;

        Builder(final String projectId) {
            this.projectId = validateDatabase(projectId);
            ancestors = new LinkedList<>();
        }

        Builder(final String projectId, final String kind) {
            this(projectId);
            this.kind = validateKind(kind);
        }

        Builder(final BaseKey copyFrom) {
            projectId = copyFrom.getProjectId();
            namespace = copyFrom.getNamespace();
            ancestors = new LinkedList<>(copyFrom.getAncestors());
            kind = copyFrom.getKind();
        }

        /**
         * return self
         * @return
         */
        @SuppressWarnings("unchecked")
        B self() {
            return (B) this;
        }

        /**
         * Adds an ancestor for this key.
         */
        public B addAncestor(final PathElement ancestor) {
            Preconditions.checkState(ancestors.size() < MAX_PATH, "path can have at most 100 elements");
            ancestors.add(ancestor);
            return self();
        }

        /**
         * Adds the provided ancestors to the key.
         */
        public B addAncestors(final PathElement ancestor, final PathElement... other) {
            return addAncestors(ImmutableList.<PathElement>builder().add(ancestor).add(other).build());
        }

        /**
         * Adds the provided ancestors to the key.
         */
        public B addAncestors(final Iterable<PathElement> ancestorIterable) {
            final ImmutableList<PathElement> list = ImmutableList.copyOf(ancestorIterable);
            Preconditions.checkState(this.ancestors.size() + list.size() < MAX_PATH,
                    "path can have at most 100 elements");
            this.ancestors.addAll(list);
            return self();
        }

        /**
         * Sets the kind of the key.
         */
        public B setKind(final String kindArg) {
            this.kind = validateKind(kindArg);
            return self();
        }

        /**
         * Sets the project ID of the key.
         */
        public B setProjectId(final String projectIdArg) {
            this.projectId = validateDatabase(projectIdArg);
            return self();
        }

        /**
         * Sets the namespace of the key.
         */
        public B setNamespace(final String namespaceArg) {
            this.namespace = validateNamespace(namespaceArg);
            return self();
        }

        protected abstract BaseKey build();
    }
// todo: create new method
//  com.google.datastore.v1.Key toPb() {
//    com.google.datastore.v1.Key.Builder keyPb = com.google.datastore.v1.Key.newBuilder();
//    com.google.datastore.v1.PartitionId.Builder partitionIdPb =
//            com.google.datastore.v1.PartitionId.newBuilder();
//    partitionIdPb.setProjectId(projectId);
//    partitionIdPb.setNamespaceId(namespace);
//    keyPb.setPartitionId(partitionIdPb.build());
//    for (PathElement pathEntry : path) {
//      keyPb.addPath(pathEntry.toPb());
//    }
//    return keyPb.build();
//  }
}
