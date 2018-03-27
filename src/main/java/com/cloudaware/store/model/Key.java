package com.cloudaware.store.model;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Base64;

/**
 * A key that is guaranteed to be complete and could be used to reference a
 * Google Cloud Datastore {@link Entity}.
 * This class is immutable.
 *
 * @see <a href="https://cloud.google.com/datastore/docs/concepts/entities">Google Cloud Datastore
 * Entities, Properties, and Keys</a>
 */
public final class Key extends IncompleteKey {

    private static final long serialVersionUID = 2563249643000943477L;

    Key(final String projectId, final String namespace, final ImmutableList<PathElement> path) {
        super(projectId, namespace, path);
        Preconditions.checkArgument(getNameOrId() != null);
    }

    /**
     * Create a {@code Key} given its URL safe encoded form.
     *
     * @throws IllegalArgumentException when decoding fails
     */
    public static Key fromUrlSafe(final String urlSafe) {
//        try {
        final GsonBuilder gson = new GsonBuilder();
        gson.registerTypeAdapter(Key.class, new KeyDeserializer());
        return gson.create().fromJson(new String(Base64.getDecoder().decode(urlSafe)), Key.class);
//            String utf8Str = URLDecoder.decode(urlSafe, UTF_8.name());
        //todo: key decoder
//            com.google.datastore.v1.Key.Builder builder = com.google.datastore.v1.Key.newBuilder();
//            TextFormat.merge(utf8Str, builder);
//            return fromPb(builder.build());
//            return null;
//        } catch (UnsupportedEncodingException e) {
//            throw new IllegalStateException("Unexpected decoding exception", e);
////        } catch (TextFormat.ParseException e) {
////            throw new IllegalArgumentException("Could not parse key", e);
//        }
    }

    public static Builder newBuilder(final String projectId, final String kind, final String name) {
        return new Builder(projectId, kind, name);
    }

    public static Builder newBuilder(final String projectId, final String kind, final long id) {
        return new Builder(projectId, kind, id);
    }

    public static Builder newBuilder(final Key copyFrom) {
        return new Builder(copyFrom);
    }

    public static Builder newBuilder(final IncompleteKey copyFrom, final String name) {
        return new Builder(copyFrom, name);
    }

    public static Builder newBuilder(final IncompleteKey copyFrom, final long id) {
        return new Builder(copyFrom, id);
    }

    public static Builder newBuilder(final Key parent, final String kind, final String name) {
        final Builder builder = newBuilder(parent.getProjectId(), kind, name);
        addParentToBuilder(parent, builder);
        return builder;
    }

    public static Builder newBuilder(final Key parent, final String kind, final long id) {
        final Builder builder = newBuilder(parent.getProjectId(), kind, id);
        addParentToBuilder(parent, builder);
        return builder;
    }
//todo: key deserializer
//    static Key fromPb(com.google.datastore.v1.Key keyPb) {
//        IncompleteKey key = IncompleteKey.fromPb(keyPb);
//        Preconditions.checkState(key instanceof Key, "Key is not complete");
//        return (Key) key;
//    }

    private static void addParentToBuilder(final Key parent, final Builder builder) {
        builder.setNamespace(parent.getNamespace());
        builder.addAncestors(parent.getAncestors());
        if (parent.hasId()) {
            builder.addAncestors(PathElement.of(parent.getKind(), parent.getId()));
        } else {
            builder.addAncestors(PathElement.of(parent.getKind(), parent.getName()));
        }
    }

    public boolean hasId() {
        return getLeaf().hasId();
    }

    /**
     * Returns the key's id or {@code null} if it has a name instead.
     */
    public Long getId() {
        return getLeaf().getId();
    }

    public boolean hasName() {
        return getLeaf().hasName();
    }

    /**
     * Returns the key's name or {@code null} if it has an id instead.
     */
    public String getName() {
        return getLeaf().getName();
    }

    /**
     * Returns the key's ID (as {@link Long}) or name (as {@link String}). Never {@code null}.
     */
    public Object getNameOrId() {
        return getLeaf().getNameOrId();
    }

    /**
     * Returns the key in an encoded form that can be used as part of a URL.
     */
    public String toUrlSafe() {
//        try {
        //todo: key encoder
        return Base64.getEncoder().encodeToString(new Gson().toJson(this).getBytes());
//            return URLEncoder.encode("", UTF_8.name());
//            return URLEncoder.encode(TextFormat.printToString(toPb()), UTF_8.name());
//        } catch (UnsupportedEncodingException e) {
//            throw new IllegalStateException("Unexpected encoding exception", e);
//        }
    }

    public static final class Builder extends BaseKey.Builder<Builder> {

        private String name;
        private Long id;

        private Builder(final String projectId, final String kind, final String name) {
            super(projectId, kind);
            this.name = name;
        }

        private Builder(final String projectId, final String kind, final long id) {
            super(projectId, kind);
            this.id = id;
        }

        private Builder(final IncompleteKey copyFrom, final String name) {
            super(copyFrom);
            this.name = name;
        }

        private Builder(final IncompleteKey copyFrom, final long id) {
            super(copyFrom);
            this.id = id;
        }

        private Builder(final Key copyFrom) {
            super(copyFrom);
            if (copyFrom.hasId()) {
                id = copyFrom.getId();
            } else {
                name = copyFrom.getName();
            }
        }

        /**
         * Sets the name of this key.
         */
        public Builder setName(final String name) {
            this.name = name;
            id = null;
            return this;
        }

        /**
         * Sets the ID of this key.
         */
        public Builder setId(final long id) {
            this.id = id;
            name = null;
            return this;
        }

        @Override
        public Key build() {
            final ImmutableList.Builder<PathElement> pathBuilder =
                    ImmutableList.<PathElement>builder().addAll(ancestors);
            if (id == null) {
                pathBuilder.add(PathElement.of(kind, name));
            } else {
                pathBuilder.add(PathElement.of(kind, id));
            }
            return new Key(projectId, namespace, pathBuilder.build());
        }
    }
}
