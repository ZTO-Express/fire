/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.restore;

import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.state.RocksDBWriteBatchWrapper;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.BackendStateType;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.END_OF_KEY_GROUP_MARK;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.clearMetaDataFollowsFlag;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.hasMetaDataFollowsFlag;
import static org.apache.flink.runtime.state.StateUtil.unexpectedStateHandleException;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Encapsulates the process of restoring a RocksDB instance from a full snapshot. */
public class RocksDBFullRestoreOperation<K> extends AbstractRocksDBRestoreOperation<K> {
    /** Current key-groups state handle from which we restore key-groups. */
    private KeyGroupsStateHandle currentKeyGroupsStateHandle;
    /** Current input stream we obtained from currentKeyGroupsStateHandle. */
    private FSDataInputStream currentStateHandleInStream;
    /** Current data input view that wraps currentStateHandleInStream. */
    private DataInputView currentStateHandleInView;
    /**
     * Current list of ColumnFamilyHandles for all column families we restore from
     * currentKeyGroupsStateHandle.
     */
    private List<ColumnFamilyHandle> currentStateHandleKVStateColumnFamilies;
    /**
     * The compression decorator that was used for writing the state, as determined by the meta
     * data.
     */
    private StreamCompressionDecorator keygroupStreamCompressionDecorator;

    /** Write batch size used in {@link RocksDBWriteBatchWrapper}. */
    private final long writeBatchSize;

    private final PriorityQueueFlag queueRestoreEnabled;

    public RocksDBFullRestoreOperation(
            KeyGroupRange keyGroupRange,
            int keyGroupPrefixBytes,
            int numberOfTransferringThreads,
            CloseableRegistry cancelStreamRegistry,
            ClassLoader userCodeClassLoader,
            Map<String, RocksDbKvStateInfo> kvStateInformation,
            StateSerializerProvider<K> keySerializerProvider,
            File instanceBasePath,
            File instanceRocksDBPath,
            DBOptions dbOptions,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            RocksDBNativeMetricOptions nativeMetricOptions,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> restoreStateHandles,
            @Nonnull RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            @Nonnegative long writeBatchSize,
            Long writeBufferManagerCapacity,
            PriorityQueueFlag queueRestoreEnabled) {
        super(
                keyGroupRange,
                keyGroupPrefixBytes,
                numberOfTransferringThreads,
                cancelStreamRegistry,
                userCodeClassLoader,
                kvStateInformation,
                keySerializerProvider,
                instanceBasePath,
                instanceRocksDBPath,
                dbOptions,
                columnFamilyOptionsFactory,
                nativeMetricOptions,
                metricGroup,
                restoreStateHandles,
                ttlCompactFiltersManager,
                writeBufferManagerCapacity);
        checkArgument(writeBatchSize >= 0, "Write batch size have to be no negative.");
        this.writeBatchSize = writeBatchSize;
        this.queueRestoreEnabled = queueRestoreEnabled;
    }

    /** Restores all key-groups data that is referenced by the passed state handles. */
    @Override
    public RocksDBRestoreResult restore()
            throws IOException, StateMigrationException, RocksDBException {
        openDB();
        for (KeyedStateHandle keyedStateHandle : restoreStateHandles) {
            if (keyedStateHandle != null) {

                if (!(keyedStateHandle instanceof KeyGroupsStateHandle)) {
                    throw unexpectedStateHandleException(
                            KeyGroupsStateHandle.class, keyedStateHandle.getClass());
                }
                this.currentKeyGroupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
                restoreKeyGroupsInStateHandle();
            }
        }
        return new RocksDBRestoreResult(
                this.db, defaultColumnFamilyHandle, nativeMetricMonitor, -1, null, null);
    }

    /** Restore one key groups state handle. */
    private void restoreKeyGroupsInStateHandle()
            throws IOException, StateMigrationException, RocksDBException {
        try {
            logger.info("Starting to restore from state handle: {}.", currentKeyGroupsStateHandle);
            // TODO: ------------ start：二次开发代码 --------------- //
            long startRestore = System.currentTimeMillis();
            // TODO: ------------ end：二次开发代码 --------------- //

            currentStateHandleInStream = currentKeyGroupsStateHandle.openInputStream();
            cancelStreamRegistry.registerCloseable(currentStateHandleInStream);
            currentStateHandleInView = new DataInputViewStreamWrapper(currentStateHandleInStream);
            restoreKVStateMetaData();
            restoreKVStateData();

            // TODO: ------------ start：二次开发代码 --------------- //
            logger.info("Finished restoring from state handle: {}, Elapsed：{}ms.", currentKeyGroupsStateHandle, System.currentTimeMillis() - startRestore);
            // TODO: ------------ end：二次开发代码 --------------- //
        } finally {
            if (cancelStreamRegistry.unregisterCloseable(currentStateHandleInStream)) {
                IOUtils.closeQuietly(currentStateHandleInStream);
            }
        }
    }

    /**
     * Restore the KV-state / ColumnFamily meta data for all key-groups referenced by the current
     * state handle.
     */
    private void restoreKVStateMetaData() throws IOException, StateMigrationException {
        // TODO: ------------ start：二次开发代码 --------------- //
        logger.info("Starting to restore KV state metadata.");
        long startRestoreMetaData = System.currentTimeMillis();
        // TODO: ------------ end：二次开发代码 --------------- //

        KeyedBackendSerializationProxy<K> serializationProxy =
                readMetaData(currentStateHandleInView);

        this.keygroupStreamCompressionDecorator =
                serializationProxy.isUsingKeyGroupCompression()
                        ? SnappyStreamCompressionDecorator.INSTANCE
                        : UncompressedStreamCompressionDecorator.INSTANCE;

        List<StateMetaInfoSnapshot> restoredMetaInfos =
                serializationProxy.getStateMetaInfoSnapshots();
        currentStateHandleKVStateColumnFamilies = new ArrayList<>(restoredMetaInfos.size());
        // TODO: ------------ start：二次开发代码 --------------- //
        double index = 0;
        // TODO: ------------ end：二次开发代码 --------------- //

        for (StateMetaInfoSnapshot restoredMetaInfo : restoredMetaInfos) {
            if (restoredMetaInfo.getBackendStateType() == BackendStateType.PRIORITY_QUEUE
                    && queueRestoreEnabled == PriorityQueueFlag.THROW_ON_PRIORITY_QUEUE) {
                throw new StateMigrationException(
                        "Can not restore savepoint taken with RocksDB timers enabled with Heap timers!");
            }
            // TODO: ------------ start：二次开发代码 --------------- //
            index += 1;
            long start = System.currentTimeMillis();
            // TODO: ------------ end：二次开发代码 --------------- //

            RocksDbKvStateInfo registeredStateCFHandle =
                    getOrRegisterStateColumnFamilyHandle(null, restoredMetaInfo);
            currentStateHandleKVStateColumnFamilies.add(registeredStateCFHandle.columnFamilyHandle);
            // TODO: ------------ start：二次开发代码 --------------- //
            BigDecimal progress = new BigDecimal(((index / restoredMetaInfos.size()) * 100)).setScale(2, BigDecimal.ROUND_HALF_UP);
            logger.info("Restore KV state metadata progress: {}/{}({}%), Elapsed：{}ms.", (int) index, restoredMetaInfos.size(), progress, System.currentTimeMillis() - start);
            // TODO: ------------ end：二次开发代码 --------------- //
        }
        // TODO: ------------ start：二次开发代码 --------------- //
        logger.info("Finished restoring KV state metadata, total:{}, Elapsed：{}ms.", (int) index, System.currentTimeMillis() - startRestoreMetaData);
        // TODO: ------------ end：二次开发代码 --------------- //
    }

    /**
     * Restore the KV-state / ColumnFamily data for all key-groups referenced by the current state
     * handle.
     */
    private void restoreKVStateData() throws IOException, RocksDBException {
        // TODO: ------------ start：二次开发代码 --------------- //
        logger.info("Starting to restore KV state data.");
        long startRestoreData = System.currentTimeMillis();
        double index = 0;
        // TODO: ------------ end：二次开发代码 --------------- //

        // for all key-groups in the current state handle...
        try (RocksDBWriteBatchWrapper writeBatchWrapper =
                     new RocksDBWriteBatchWrapper(db, writeBatchSize)) {
            // TODO: ------------ start：二次开发代码 --------------- //
            long listSize = 100;
            try {
                // 通过反射获取进度的总计，用于日志打印总进度
                KeyGroupRangeOffsets keyGroupRangeOffsets = currentKeyGroupsStateHandle.getGroupRangeOffsets();
                Field offsetsFiled = keyGroupRangeOffsets.getClass().getDeclaredField("offsets");
                offsetsFiled.setAccessible(true);
                long[] offsets = (long[]) offsetsFiled.get(keyGroupRangeOffsets);
                if (offsets != null && offsets.length != 0) listSize = offsets.length;
            } catch (Exception e) {
                logger.warn("获取进度总计失败，对任务无任何影响", e);
            }
            // TODO: ------------ end：二次开发代码 --------------- //

            for (Tuple2<Integer, Long> keyGroupOffset :
                    currentKeyGroupsStateHandle.getGroupRangeOffsets()) {
                int keyGroup = keyGroupOffset.f0;
                index += 1;
                long start = System.currentTimeMillis();
                // Check that restored key groups all belong to the backend
                Preconditions.checkState(
                        keyGroupRange.contains(keyGroup),
                        "The key group must belong to the backend");

                long offset = keyGroupOffset.f1;
                // not empty key-group?
                if (0L != offset) {
                    currentStateHandleInStream.seek(offset);
                    try (InputStream compressedKgIn =
                                 keygroupStreamCompressionDecorator.decorateWithCompression(
                                         currentStateHandleInStream)) {
                        DataInputViewStreamWrapper compressedKgInputView =
                                new DataInputViewStreamWrapper(compressedKgIn);
                        // TODO this could be aware of keyGroupPrefixBytes and write only one byte
                        // if possible
                        int kvStateId = compressedKgInputView.readShort();
                        ColumnFamilyHandle handle =
                                currentStateHandleKVStateColumnFamilies.get(kvStateId);
                        // insert all k/v pairs into DB
                        boolean keyGroupHasMoreKeys = true;
                        while (keyGroupHasMoreKeys) {
                            byte[] key =
                                    BytePrimitiveArraySerializer.INSTANCE.deserialize(
                                            compressedKgInputView);
                            byte[] value =
                                    BytePrimitiveArraySerializer.INSTANCE.deserialize(
                                            compressedKgInputView);
                            if (hasMetaDataFollowsFlag(key)) {
                                // clear the signal bit in the key to make it ready for insertion
                                // again
                                clearMetaDataFollowsFlag(key);
                                writeBatchWrapper.put(handle, key, value);
                                // TODO this could be aware of keyGroupPrefixBytes and write only
                                // one byte if possible
                                kvStateId =
                                        END_OF_KEY_GROUP_MARK & compressedKgInputView.readShort();
                                if (END_OF_KEY_GROUP_MARK == kvStateId) {
                                    keyGroupHasMoreKeys = false;
                                } else {
                                    handle = currentStateHandleKVStateColumnFamilies.get(kvStateId);
                                }
                            } else {
                                writeBatchWrapper.put(handle, key, value);
                            }
                        }
                    }
                }
                BigDecimal progress = new BigDecimal((index / listSize) * 100).setScale(2, BigDecimal.ROUND_HALF_UP);
                // TODO: ------------ start：二次开发代码 --------------- //
                logger.info("Restore KV state data progress: {}/{}({}%), Elapsed：{}ms.", (int) index, listSize, progress, System.currentTimeMillis() - start);
                // TODO: ------------ end：二次开发代码 --------------- //
            }
        }
        // TODO: ------------ start：二次开发代码 --------------- //
        logger.info("Finished restoring KV state data, total:{}, Elapsed：{}ms.", (int) index, System.currentTimeMillis() - startRestoreData);
        // TODO: ------------ end：二次开发代码 --------------- //
    }
}
