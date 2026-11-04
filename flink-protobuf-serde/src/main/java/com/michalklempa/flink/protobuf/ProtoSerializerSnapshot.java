package com.michalklempa.flink.protobuf;

import com.google.protobuf.Message;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

public class ProtoSerializerSnapshot<T extends Message> implements TypeSerializerSnapshot<T> {

    private static final int VERSION = 1;

    private String typeName;
    private Class<T> type;

    /**
     * Flink instantiates this class reflectively by name when reading a checkpoint, so a public
     * no-arg constructor is mandatory. Fields set later by readSnapshot, hence not final.
     *
     * RISK: the fully qualified name of THIS class is written into every checkpoint
     * (TypeSerializerSnapshot.writeVersionedSnapshot). Renaming or moving it breaks restore of
     * every checkpoint taken with the old name. Decide the final name before first release.
     */
    public ProtoSerializerSnapshot() {
    }

    public ProtoSerializerSnapshot(Class<T> type) {
        this.type = type;
        this.typeName = type.getName();
    }

    /**
     * Version of the binary format written by writeSnapshot.
     *
     * RISK: readSnapshot must handle every version ever shipped. Bump this whenever the written
     * layout changes, and never reuse a number.
     */
    @Override
    public int getCurrentVersion() {
        return VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        out.writeUTF(typeName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {
        if (readVersion != 1) {
            throw new IOException("Unrecognized ProtoSerializerSnapshot version " + readVersion);
        }
        typeName = in.readUTF();
        try {
            Class<?> resolved = Class.forName(typeName, false, userCodeClassLoader);
            type = Message.class.isAssignableFrom(resolved) ? (Class<T>) resolved : null;
        } catch (ClassNotFoundException | LinkageError e) {
            type = null;
        }
    }

    @Override
    public TypeSerializer<T> restoreSerializer() {
        checkState(type != null,
                "Protobuf message class %s from the checkpoint is not on the classpath or is not a com.google.protobuf.Message",
                typeName);
        return new ProtoSerializer<>(type);
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            TypeSerializer<T> newSerializer) {
        if (!(newSerializer instanceof ProtoSerializer)) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }
        ProtoSerializer<T> proto = (ProtoSerializer<T>) newSerializer;
        if (proto.getType().getName().equals(typeName) || proto.getFormerNames().contains(typeName)) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }
        return TypeSerializerSchemaCompatibility.incompatible();
    }
}
