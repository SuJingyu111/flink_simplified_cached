/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.commons.math3.util.Pair;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;

/**
 * {@link ValueState} implementation that stores state in RocksDB.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 */
class RocksDBValueState<K, N, V> extends AbstractRocksDBState<K, N, V>
        implements InternalValueState<K, N, V> {

    /**
     * Creates a new {@code RocksDBValueState}.
     *
     * @param columnFamily The RocksDB column family that this state is associated to.
     * @param namespaceSerializer The serializer for the namespace.
     * @param valueSerializer The serializer for the state.
     * @param defaultValue The default value for the state.
     * @param backend The backend for which this state is bind to.
     */
    private RocksDBValueState(
            ColumnFamilyHandle columnFamily,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue,
            RocksDBKeyedStateBackend<K> backend) {

        super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return backend.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public V value() {
        try {
            byte[] key = serializeCurrentKeyWithGroupAndNamespace();
            String keyString = b2String(key);
            if (this.cache.has(keyString)) {
                return (V) this.cache.get(keyString);
            }
            byte[] valueBytes =
                    backend.db.get(columnFamily, serializeCurrentKeyWithGroupAndNamespace());

            if (valueBytes == null) {
                return getDefaultValue();
            }

            dataInputView.setBuffer(valueBytes);
            V value = valueSerializer.deserialize(dataInputView);
            this.cache.update(keyString, value);
            return value;
        } catch (IOException | RocksDBException e) {
            throw new FlinkRuntimeException("Error while retrieving data from RocksDB.", e);
        }
    }

    @Override
    public void update(V value) {
        if (value == null) {
            clear();
            return;
        }

        try {
            byte[] key = serializeCurrentKeyWithGroupAndNamespace();
            String keyString = b2String(key);
            logger.info("value(): 1: ");
            logger.info(keyString);
            logger.info("value(): 2: ");
            logger.info(b2String(s2ByteArray(keyString)));

            Pair<K, V> evictedKV = this.cache.update(keyString, value);

            if (evictedKV != null) {
                backend.db.put(
                        columnFamily,
                        writeOptions,
                        s2ByteArray((String) evictedKV.getKey()),
                        serializeValue(evictedKV.getValue()));
            }

        } catch (Exception e) {
            throw new FlinkRuntimeException("Error while adding data to RocksDB", e);
        }
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            RocksDBKeyedStateBackend<K> backend) {
        return (IS)
                new RocksDBValueState<>(
                        registerResult.f0,
                        registerResult.f1.getNamespaceSerializer(),
                        registerResult.f1.getStateSerializer(),
                        stateDesc.getDefaultValue(),
                        backend);
    }
}
