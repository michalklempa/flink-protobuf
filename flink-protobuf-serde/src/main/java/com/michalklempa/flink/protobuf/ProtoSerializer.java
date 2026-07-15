package com.michalklempa.flink.protobuf;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.Objects;

public class ProtoSerializer<T extends Message> extends TypeSerializer<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> type;
    private transient Parser<T> parser;

    public ProtoSerializer(Class<T> type) {
        this.type = type;
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<T> duplicate() {
        return new ProtoSerializer<>(type);
    }

    @Override
    public T createInstance() {
        try {
            return type.cast(type.getMethod("getDefaultInstance").invoke(null));
        } catch (ReflectiveOperationException e) {
            throw new FlinkRuntimeException("Cannot resolve default instance for " + type.getName(), e);
        }
    }

    @Override
    public T copy(T from) {
        return from;
    }

    @Override
    public T copy(T from, T reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        if (record == null) {
            target.writeInt(-1);
            return;
        }
        byte[] bytes = record.toByteArray();
        target.writeInt(bytes.length);
        target.write(bytes);
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        int length = source.readInt();
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        return parser().parseFrom(bytes);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int length = source.readInt();
        target.writeInt(length);
        if (length > 0) {
            target.write(source, length);
        }
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        throw new UnsupportedOperationException(
                "ProtoSerializer supports operator-to-operator serialization only."
                        + " State serialization is unsupported (no schema evolution):"
                        + " do not use protobuf types in state descriptors, windows or joins"
                        + " with checkpointing enabled. Type: " + type.getName());
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ProtoSerializer && type.equals(((ProtoSerializer<?>) o).type);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(type);
    }

    @SuppressWarnings("unchecked")
    private Parser<T> parser() {
        if (parser == null) {
            try {
                parser = (Parser<T>) type.getMethod("parser").invoke(null);
            } catch (ReflectiveOperationException e) {
                throw new FlinkRuntimeException("Cannot resolve protobuf parser for " + type.getName(), e);
            }
        }
        return parser;
    }
}
