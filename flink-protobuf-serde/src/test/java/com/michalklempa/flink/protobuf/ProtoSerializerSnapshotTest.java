package com.michalklempa.flink.protobuf;

import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class ProtoSerializerSnapshotTest {

    private static final String MISSING = "com.example.Missing";

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T extends Message> TypeSerializerSnapshot<T> writeAndRead(TypeSerializer<T> serializer) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(128);
        TypeSerializerSnapshot.writeVersionedSnapshot(out, serializer.snapshotConfiguration());
        return (TypeSerializerSnapshot<T>) (TypeSerializerSnapshot) TypeSerializerSnapshot.readVersionedSnapshot(
                new DataInputDeserializer(out.getCopyOfBuffer()), getClass().getClassLoader());
    }

    private <T extends Message> ProtoSerializerSnapshot<T> readRaw(int version, String className) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(128);
        out.writeUTF(className);
        ProtoSerializerSnapshot<T> snapshot = new ProtoSerializerSnapshot<>();
        snapshot.readSnapshot(version, new DataInputDeserializer(out.getCopyOfBuffer()), getClass().getClassLoader());
        return snapshot;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static TypeSerializerSchemaCompatibility<?> resolve(TypeSerializerSnapshot<?> snapshot, TypeSerializer<?> serializer) {
        return ((TypeSerializerSnapshot) snapshot).resolveSchemaCompatibility((TypeSerializer) serializer);
    }

    @Test
    void roundTripsThroughVersionedSnapshot() throws Exception {
        ProtoSerializer<Timestamp> serializer = new ProtoSerializer<>(Timestamp.class);
        TypeSerializerSnapshot<Timestamp> restored = writeAndRead(serializer);

        assertEquals(serializer, restored.restoreSerializer());
        assertTrue(resolve(restored, serializer).isCompatibleAsIs());
    }

    @Test
    void restoredSerializerDeserializesOldBytes() throws Exception {
        ProtoSerializer<Timestamp> serializer = new ProtoSerializer<>(Timestamp.class);
        Timestamp value = Timestamp.newBuilder().setSeconds(1751000000).setNanos(42).build();
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(value, out);

        TypeSerializer<Timestamp> restored = writeAndRead(serializer).restoreSerializer();
        assertEquals(value, restored.deserialize(new DataInputDeserializer(out.getCopyOfBuffer())));
    }

    @Test
    void differentMessageClassIsIncompatible() throws Exception {
        TypeSerializerSnapshot<Timestamp> snapshot = writeAndRead(new ProtoSerializer<>(Timestamp.class));
        assertTrue(resolve(snapshot, new ProtoSerializer<>(StringValue.class)).isIncompatible());
    }

    @Test
    void nonProtoSerializerIsIncompatible() throws Exception {
        TypeSerializerSnapshot<Timestamp> snapshot = writeAndRead(new ProtoSerializer<>(Timestamp.class));
        assertTrue(resolve(snapshot, StringSerializer.INSTANCE).isIncompatible());
    }

    @Test
    void formerNameMakesRenamedClassCompatible() throws Exception {
        TypeSerializerSnapshot<Timestamp> snapshot = writeAndRead(new ProtoSerializer<>(Timestamp.class));
        ProtoSerializer<StringValue> renamed = new ProtoSerializer<>(
                StringValue.class, Collections.singleton(Timestamp.class.getName()));
        assertTrue(resolve(snapshot, renamed).isCompatibleAsIs());
    }

    @Test
    void formerNamesDoNotAffectEquality() {
        ProtoSerializer<Timestamp> plain = new ProtoSerializer<>(Timestamp.class);
        ProtoSerializer<Timestamp> aliased = new ProtoSerializer<>(Timestamp.class, Collections.singleton(MISSING));
        assertEquals(plain, aliased);
        assertEquals(plain.hashCode(), aliased.hashCode());
    }

    @Test
    void missingClassReadsButCannotRestore() throws Exception {
        ProtoSerializerSnapshot<Timestamp> snapshot = readRaw(1, MISSING);
        IllegalStateException e = assertThrows(IllegalStateException.class, snapshot::restoreSerializer);
        assertTrue(e.getMessage().contains(MISSING));
    }

    @Test
    void missingClassIsCompatibleWhenDeclaredAsFormerName() throws Exception {
        ProtoSerializerSnapshot<Timestamp> snapshot = readRaw(1, MISSING);
        assertTrue(resolve(snapshot, new ProtoSerializer<>(Timestamp.class, Collections.singleton(MISSING))).isCompatibleAsIs());
        assertFalse(resolve(snapshot, new ProtoSerializer<>(Timestamp.class)).isCompatibleAsIs());
    }

    @Test
    void nonMessageClassCannotRestore() throws Exception {
        ProtoSerializerSnapshot<Timestamp> snapshot = readRaw(1, String.class.getName());
        assertThrows(IllegalStateException.class, snapshot::restoreSerializer);
    }

    @Test
    void rejectsUnknownVersion() {
        assertThrows(IOException.class, () -> readRaw(2, Timestamp.class.getName()));
    }

    @Test
    void typeInfoPassesFormerNamesToSerializer() {
        ProtoTypeInfo<Timestamp> info = ProtoTypeInfo.proto(Timestamp.class, MISSING);
        ProtoSerializer<Timestamp> serializer = (ProtoSerializer<Timestamp>) info.createSerializer(new org.apache.flink.api.common.ExecutionConfig());
        assertTrue(serializer.getFormerNames().contains(MISSING));
        assertTrue(serializer.duplicate() instanceof ProtoSerializer);
        assertTrue(((ProtoSerializer<Timestamp>) serializer.duplicate()).getFormerNames().contains(MISSING));
        assertEquals(ProtoTypeInfo.proto(Timestamp.class), info);
    }

    @Test
    void protoRejectsDuplicateFormerNames() {
        assertThrows(IllegalArgumentException.class,
                () -> ProtoTypeInfo.proto(Timestamp.class, MISSING, MISSING));
    }

    @Test
    void protoRejectsNullFormerName() {
        assertThrows(NullPointerException.class,
                () -> ProtoTypeInfo.proto(Timestamp.class, (String) null));
    }
}
