package com.michalklempa.flink.protobuf;

import com.google.protobuf.Message;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class ProtoTypeInfo<T extends Message> extends TypeInformation<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> type;
    private final Set<String> formerNames;

    public ProtoTypeInfo(Class<T> type) {
        this(type, Collections.emptySet());
    }

    public ProtoTypeInfo(Class<T> type, Collection<String> formerNames) {
        this.type = type;
        this.formerNames = Set.copyOf(formerNames);
    }

    public static <T extends Message> ProtoTypeInfo<T> proto(Class<T> type) {
        return new ProtoTypeInfo<>(type);
    }

    public static <T extends Message> ProtoTypeInfo<T> proto(Class<T> type, String... formerNames) {
        return new ProtoTypeInfo<>(type, Set.of(formerNames));
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<T> getTypeClass() {
        return type;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        return new ProtoSerializer<>(type, formerNames);
    }

    @Override
    public String toString() {
        return "ProtoTypeInfo<" + type.getName() + ">";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ProtoTypeInfo && type.equals(((ProtoTypeInfo<?>) o).type);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(type);
    }

    @Override
    public boolean canEqual(Object o) {
        return o instanceof ProtoTypeInfo;
    }
}
