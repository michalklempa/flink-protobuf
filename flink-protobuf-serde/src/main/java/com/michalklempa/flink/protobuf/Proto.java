package com.michalklempa.flink.protobuf;

import com.google.protobuf.AbstractMessage;

import java.lang.reflect.Type;

public final class Proto {

    private Proto() {
    }

    public static void register() {
        try {
            Class<?> extractor = Class.forName("org.apache.flink.api.java.typeutils.TypeExtractor");
            Object existing = extractor.getMethod("getTypeInfoFactory", Type.class)
                    .invoke(null, AbstractMessage.class);
            if (existing instanceof ProtoFactory) {
                return;
            }
            extractor.getMethod("registerFactory", Type.class, Class.class)
                    .invoke(null, AbstractMessage.class, ProtoFactory.class);
        } catch (NoSuchMethodException e) {
            throw new UnsupportedOperationException(
                    "Global registration requires Flink 1.19+ (TypeExtractor.registerFactory), use ProtoTypeInfo.proto() on 1.18",
                    e);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot register ProtoFactory", e);
        }
    }
}
