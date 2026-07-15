package com.michalklempa.flink.protobuf;

import com.google.protobuf.Message;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.Map;

public class ProtoFactory<T extends Message> extends TypeInfoFactory<T> {

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<T> createTypeInfo(Type type, Map<String, TypeInformation<?>> parameters) {
        if (!(type instanceof Class) || !Message.class.isAssignableFrom((Class<?>) type)) {
            throw new IllegalArgumentException("Not a protobuf message type: " + type);
        }
        return new ProtoTypeInfo<>((Class<T>) type);
    }
}
