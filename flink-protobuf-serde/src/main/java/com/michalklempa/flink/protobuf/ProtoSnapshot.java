package com.michalklempa.flink.protobuf;

import com.google.protobuf.Message;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class ProtoSnapshot<T extends Message> implements TypeSerializerSnapshot<T> {

    private static final int VERSION = 1;

    private Class<T> type;

    public ProtoSnapshot() {
    }

    public ProtoSnapshot(Class<T> type) {
        this.type = type;
    }

    @Override
    public int getCurrentVersion() {
        return VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        out.writeUTF(type.getName());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readSnapshot(int version, DataInputView in, ClassLoader loader) throws IOException {
        try {
            type = (Class<T>) Class.forName(in.readUTF(), false, loader);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    @Override
    public TypeSerializer<T> restoreSerializer() {
        return new ProtoSerializer<>(type);
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> serializer) {
        if (serializer instanceof ProtoSerializer && type.equals(((ProtoSerializer<?>) serializer).type())) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }
        return TypeSerializerSchemaCompatibility.incompatible();
    }
}
