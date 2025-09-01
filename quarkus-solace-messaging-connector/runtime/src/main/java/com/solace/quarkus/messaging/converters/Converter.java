package com.solace.quarkus.messaging.converters;

import org.osgi.annotation.versioning.ProviderType;

import java.io.Serializable;

@ProviderType
public interface Converter {
    @FunctionalInterface
    public interface BytesToObject<T extends Serializable> extends Converter {
        T convert(byte[] var1) throws RuntimeException;
    }

    @FunctionalInterface
    public interface ObjectToBytes<T extends Serializable> extends Converter {
        byte[] toBytes(T var1);
    }
}
