package org.github.dbjo.rdb.demo;

import com.google.protobuf.*;

import java.io.IOException;
import java.util.Objects;

/**
 * Minimal hand-written stand-in for protoc output for:
 *
 * message User {
 *   string id = 1;
 *   optional string email = 2;
 *   optional string name = 3;
 * }
 *
 * - proto3 default for id (no presence)
 * - proto3 optional presence for email/name (hasEmail/hasName)
 */
@SuppressWarnings("all")
public final class User extends AbstractMessageLite<User, User.Builder> {

    // Field numbers
    public static final int ID_FIELD_NUMBER    = 1;
    public static final int EMAIL_FIELD_NUMBER = 2;
    public static final int NAME_FIELD_NUMBER  = 3;

    // Wire tags (field_number << 3 | wire_type), wire_type=2 for length-delimited strings
    private static final int TAG_ID    = (ID_FIELD_NUMBER << 3) | 2;    // 10
    private static final int TAG_EMAIL = (EMAIL_FIELD_NUMBER << 3) | 2; // 18
    private static final int TAG_NAME  = (NAME_FIELD_NUMBER << 3) | 2;  // 26

    private static final User DEFAULT_INSTANCE = new User("", false, "", false, "");
    private static final Parser<User> PARSER = new AbstractParser<>() {
        @Override
        public User parsePartialFrom(CodedInputStream input, ExtensionRegistryLite ext)
                throws InvalidProtocolBufferException {
            try {
                Builder b = User.newBuilder();
                b.mergeFrom(input, ext);
                return b.buildPartial();
            } catch (InvalidProtocolBufferException e) {
                throw e;
            } catch (Exception e) {
                throw new InvalidProtocolBufferException(e);
            }
        }
    };

    private final String id;          // proto3 scalar: no presence
    private final boolean hasEmail;
    private final String email;
    private final boolean hasName;
    private final String name;

    private int memoizedSize = -1;

    private User(String id, boolean hasEmail, String email, boolean hasName, String name) {
        this.id = id == null ? "" : id;
        this.hasEmail = hasEmail;
        this.email = email == null ? "" : email;
        this.hasName = hasName;
        this.name = name == null ? "" : name;
    }

    public static User getDefaultInstance() { return DEFAULT_INSTANCE; }
    public static Parser<User> parser() { return PARSER; }

    public static Builder newBuilder() { return new Builder(); }
    @Override public Builder newBuilderForType() { return newBuilder(); }
    @Override public Builder toBuilder() { return newBuilder().mergeFrom(this); }
    @Override public User getDefaultInstanceForType() { return DEFAULT_INSTANCE; }
    @Override public Parser<User> getParserForType() { return PARSER; }

    // --- Getters ---
    public String getId() { return id; }

    public boolean hasEmail() { return hasEmail; }
    public String getEmail() { return email; }

    public boolean hasName() { return hasName; }
    public String getName() { return name; }

    // --- Serialization ---
    @Override
    public void writeTo(CodedOutputStream output) throws IOException {
        if (!id.isEmpty()) {
            output.writeString(ID_FIELD_NUMBER, id);
        }
        if (hasEmail) {
            output.writeString(EMAIL_FIELD_NUMBER, email);
        }
        if (hasName) {
            output.writeString(NAME_FIELD_NUMBER, name);
        }
    }

    @Override
    public int getSerializedSize() {
        int size = memoizedSize;
        if (size != -1) return size;

        size = 0;
        if (!id.isEmpty()) {
            size += CodedOutputStream.computeStringSize(ID_FIELD_NUMBER, id);
        }
        if (hasEmail) {
            size += CodedOutputStream.computeStringSize(EMAIL_FIELD_NUMBER, email);
        }
        if (hasName) {
            size += CodedOutputStream.computeStringSize(NAME_FIELD_NUMBER, name);
        }

        memoizedSize = size;
        return size;
    }

    public static User parseFrom(byte[] data) throws InvalidProtocolBufferException {
        return PARSER.parseFrom(data);
    }

    // --- Basic equality (useful in tests) ---
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof User other)) return false;
        return id.equals(other.id)
                && hasEmail == other.hasEmail
                && email.equals(other.email)
                && hasName == other.hasName
                && name.equals(other.name);
    }

    @Override
    public int hashCode() {
        int h = id.hashCode();
        h = 31 * h + Boolean.hashCode(hasEmail);
        h = 31 * h + email.hashCode();
        h = 31 * h + Boolean.hashCode(hasName);
        h = 31 * h + name.hashCode();
        return h;
    }

    @Override
    public boolean isInitialized() {
        return true; // no required fields in proto3
    }

    @Override
    public String toString() {
        return "User{" +
                "id='" + id + '\'' +
                ", email='" + email + '\'' +
                ", name='" + name + '\'' +
                '}';
    }

    // --- Builder ---
    public static final class Builder extends AbstractMessageLite.Builder<User, Builder> {
        private String id = "";
        private boolean hasEmail;
        private String email = "";
        private boolean hasName;
        private String name = "";

        private Builder() {}

        @Override
        public Builder clear() {
            id = "";
            hasEmail = false;
            email = "";
            hasName = false;
            name = "";
            return this;
        }

        @Override
        public Builder clone() {
            Builder b = new Builder();
            b.id = this.id;
            b.hasEmail = this.hasEmail;
            b.email = this.email;
            b.hasName = this.hasName;
            b.name = this.name;
            return b;
        }

        @Override
        public User getDefaultInstanceForType() {
            return User.getDefaultInstance();
        }

        @Override
        public boolean isInitialized() {
            return true; // proto3: no required fields
        }

        @Override
        protected Builder internalMergeFrom(User message) {
            return mergeFrom(message);
        }

        public Builder setId(String id) {
            this.id = Objects.requireNonNull(id, "id");
            return this;
        }

        public Builder clearId() {
            this.id = "";
            return this;
        }

        public Builder setEmail(String email) {
            this.email = Objects.requireNonNull(email, "email");
            this.hasEmail = true;
            return this;
        }

        public Builder clearEmail() {
            this.email = "";
            this.hasEmail = false;
            return this;
        }

        public Builder setName(String name) {
            this.name = Objects.requireNonNull(name, "name");
            this.hasName = true;
            return this;
        }

        public Builder clearName() {
            this.name = "";
            this.hasName = false;
            return this;
        }

        public Builder mergeFrom(User other) {
            if (other == null) return this;
            if (!other.id.isEmpty()) this.id = other.id;
            if (other.hasEmail) { this.hasEmail = true; this.email = other.email; }
            if (other.hasName)  { this.hasName  = true; this.name  = other.name; }
            return this;
        }

        @Override
        public User build() {
            return buildPartial();
        }

        @Override
        public User buildPartial() {
            return new User(id, hasEmail, email, hasName, name);
        }

        @Override
        public Builder mergeFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry)
                throws IOException {
            while (true) {
                final int tag = input.readTag();
                if (tag == 0) break;

                switch (tag) {
                    case TAG_ID -> this.id = input.readStringRequireUtf8();
                    case TAG_EMAIL -> {
                        this.email = input.readStringRequireUtf8();
                        this.hasEmail = true;
                    }
                    case TAG_NAME -> {
                        this.name = input.readStringRequireUtf8();
                        this.hasName = true;
                    }
                    default -> {
                        if (!input.skipField(tag)) return this;
                    }
                }
            }
            return this;
        }
    }
}
