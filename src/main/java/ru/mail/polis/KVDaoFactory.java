/*
 * Copyright 2018 (c) Vadim Tsesko <incubos@yandex.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

/**
 * Custom {@link KVDao} factory
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
public final class KVDaoFactory {
    private static final long MAX_HEAP = 128 * 1024 * 1024;

    private KVDaoFactory() {
        // Not instantiatable
    }

    /**
     * Construct a {@link KVDao} instance.
     *
     * @param data local disk folder to persist the data to
     * @return a storage instance
     */
    @NotNull
    public static KVDao create(@NotNull final File data) throws IOException{
        if (Runtime.getRuntime().maxMemory() > MAX_HEAP) {
            throw new IllegalStateException("The heap is too big. Consider setting Xmx.");
        }

        if (!data.exists()) {
            throw new IllegalArgumentException("Path doesn't exist: " + data);
        }

        if (!data.isDirectory()) {
            throw new IllegalArgumentException("Path is not a directory: " + data);
        }

        return new LSMDao(data);
    }

    private static class LSMDao implements KVDao {
        final private int MEM_TABLE_TRASH_HOLD = 1024 * 40;
        final private String STORAGE_DIR;

        final private SortedMap<ByteBuffer, Value> memTable = new TreeMap<>();

        final private SnapshotHolder holder;

        private Long memTablesize = 0L;

        public LSMDao(final File dir) throws IOException {
            this.STORAGE_DIR = dir + File.separator;
            this.holder = new SnapshotHolder(this.STORAGE_DIR);
        }

        @NotNull
        @Override
        public byte[] get(@NotNull byte[] key) throws IOException, NoSuchElementException {
            Value value = this.memTable.get(ByteBuffer.wrap(key));
            if (value == null) {
                return this.holder.get(key);
            } else {
                if (value.getValue() == SnapshotHolder.REMOVED_VALUE) {
                    throw new NoSuchElementException();
                } else {
                    return value.getValue();
                }
            }
        }

        @Override
        public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
            this.memTable.put(ByteBuffer.wrap(key), new Value(value, System.currentTimeMillis()));
            this.memTablesize += key.length + value.length;
            if (memTablesize >= MEM_TABLE_TRASH_HOLD) {
                this.holder.save(this.memTable);
                this.memTable.clear();
                this.memTablesize = 0L;
            }
        }

        @Override
        public void remove(@NotNull byte[] key) {
            byte[] value = this.memTable.get(ByteBuffer.wrap(key)).getValue();
            if (value != null) {
                if (value != SnapshotHolder.REMOVED_VALUE) {
                    this.memTable.put(
                            ByteBuffer.wrap(key),
                            new Value(SnapshotHolder.REMOVED_VALUE, System.currentTimeMillis()));
                }
            } else if (this.holder.contains(key)) {
                this.memTable.put(
                        ByteBuffer.wrap(key),
                        new Value(SnapshotHolder.REMOVED_VALUE, System.currentTimeMillis()));
            }
        }

        @Override
        public void close() throws IOException {
            this.holder.save(this.memTable);
        }

        private class Value {
            private byte[] value;
            private long timeStamp;

            public Value(byte[] value, @NotNull long timeStamp) {
                this.value = value;
                this.timeStamp = timeStamp;
            }

            public byte[] getValue() {
                return value;
            }

            public void setValue(byte[] value) {
                this.value = value;
            }

            public Long getTimeStamp() {
                return timeStamp;
            }

            public void setTimeStamp(@NotNull long timeStamp) {
                this.timeStamp = timeStamp;
            }
        }

        private static class SnapshotHolder {
            final public static byte[] REMOVED_VALUE = new byte[0];
            final public int REMOVED_MARK = -1;

            final private Map<ByteBuffer, Value> sSMap = new HashMap<>();
            final private File storage;

            private Long fileNumber = 0L;

            public SnapshotHolder(String dir) throws IOException {
                this.storage = new File(dir);
                if (!this.storage.exists()) throw new IOException();
                HashMap<ByteBuffer, Long> timeStamps = new HashMap<>();
                try {
                    java.nio.file.Files.walkFileTree(
                            this.storage.toPath(),
                            new SimpleFileVisitor<Path>() {
                                private void fetchData(@NotNull final Path file) throws IOException {
                                    RandomAccessFile randomAccessFile = new RandomAccessFile(file.toFile(), "r");
                                    long currentTStamp = randomAccessFile.readLong();
                                    int amount = randomAccessFile.readInt();
                                    for (int i = 0; i < amount; i++) {
                                        byte[] bytes = new byte[randomAccessFile.readInt()];
                                        randomAccessFile.read(bytes);
                                        int offset = randomAccessFile.readInt();
                                        long timeStamp = randomAccessFile.readLong();
                                        ByteBuffer keyBuffer = ByteBuffer.wrap(bytes);
                                        if (offset == REMOVED_MARK) {
                                            Long itemTStamp = timeStamps.get(keyBuffer);
                                            if (itemTStamp == null) {
                                                timeStamps.put(keyBuffer, currentTStamp);
                                            } else {
                                                if (itemTStamp < currentTStamp) {
                                                    sSMap.put(keyBuffer, new LSMDao.SnapshotHolder.Value(null, timeStamp));
                                                    timeStamps.put(keyBuffer, currentTStamp);
                                                }
                                            }
                                        } else {
                                            Long itemTStamp = timeStamps.get(keyBuffer);
                                            if (itemTStamp == null) {
                                                timeStamps.put(keyBuffer, currentTStamp);
                                                sSMap.put(
                                                        keyBuffer,
                                                        new LSMDao.SnapshotHolder.Value(
                                                                Long.parseLong(file.toFile().getName()), timeStamp));
                                            } else {
                                                if (itemTStamp < currentTStamp) {
                                                    sSMap.put(
                                                            keyBuffer,
                                                            new LSMDao.SnapshotHolder.Value(
                                                                    Long.parseLong(file.toFile().getName()),
                                                                    timeStamp));
                                                    timeStamps.put(keyBuffer, currentTStamp);
                                                }
                                            }
                                        }
                                    }
                                    if (fileNumber <= Long.parseLong(file.toFile().getName())) {
                                        fileNumber = Long.parseLong(file.toFile().getName());
                                        fileNumber++;
                                    }
                                    randomAccessFile.close();
                                }

                                @NotNull
                                @Override
                                public FileVisitResult visitFile(@NotNull final Path file, @NotNull final BasicFileAttributes attrs)
                                        throws IOException {
                                    fetchData(file);
                                    return FileVisitResult.CONTINUE;
                                }
                            });
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }

            public byte[] get(byte[] key) throws IOException, NoSuchElementException {
                LSMDao.SnapshotHolder.Value index = this.sSMap.get(ByteBuffer.wrap(key));
                if (index == null) throw new NoSuchElementException();
                if (index.getIndex() == null) throw new NoSuchElementException();
                File source = new File(this.storage.toString() + File.separator + index.getIndex().toString());
                if (!source.canRead() || !source.exists() || !source.isFile()) throw new IOException();
                RandomAccessFile randomAccessFile = new RandomAccessFile(source, "r");
                randomAccessFile.skipBytes(Long.BYTES);
                int valueOffset = REMOVED_MARK;
                int amount = randomAccessFile.readInt();
                for (int i = 0; i < amount; i++) {
                    byte[] bytes = new byte[randomAccessFile.readInt()];
                    randomAccessFile.read(bytes);
                    int offSet = randomAccessFile.readInt();
                    if (Arrays.equals(bytes, key)) {
                        if (offSet != REMOVED_MARK) {
                            valueOffset = offSet;
                        } else throw new NoSuchElementException();
                    }
                }

                randomAccessFile.skipBytes(valueOffset);
                byte[] value = new byte[randomAccessFile.readInt()];
                randomAccessFile.read(value);
                randomAccessFile.close();
                return value;

            }

            public boolean contains(byte[] key) {
                return this.sSMap.containsKey(ByteBuffer.wrap(key));
            }

            public void save(SortedMap<ByteBuffer, LSMDao.Value> source) throws IOException {
                int offset = 0;
                File dist = new File(this.storage + File.separator + fileNumber.toString());
                if (!dist.createNewFile())
                    throw new IOException();

                ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
                OutputStream outputStream = new FileOutputStream(dist);
                outputStream.write(ByteBuffer.allocate(Long.BYTES).putLong(System.currentTimeMillis()).array());
                outputStream.write(intBuffer.putInt(source.size()).array());
                intBuffer.clear();

                for (Map.Entry<ByteBuffer, LSMDao.Value> entry : source.entrySet()) {
                    outputStream.write(intBuffer.putInt(entry.getKey().array().length).array());
                    intBuffer.clear();
                    outputStream.write(entry.getKey().array());
                    if (entry.getValue().getValue() == REMOVED_VALUE) {
                        outputStream.write(intBuffer.putInt(REMOVED_MARK).array());
                        intBuffer.clear();
                    } else {
                        outputStream.write(intBuffer.putInt(offset).array());
                        intBuffer.clear();
                        offset += Integer.BYTES + entry.getValue().getValue().length;
                    }
                    outputStream.write(ByteBuffer.allocate(Long.BYTES).putLong(entry.getValue().getTimeStamp()).array());
                }

                for (Map.Entry<ByteBuffer, LSMDao.Value> entry : source.entrySet()) {
                    outputStream.write(intBuffer.putInt(entry.getValue().getValue().length).array());
                    intBuffer.clear();
                    outputStream.write(entry.getValue().getValue());
                }
                outputStream.flush();
                outputStream.close();
                for (Map.Entry<ByteBuffer, LSMDao.Value> entry : source.entrySet()) {
                    this.sSMap.put(
                            entry.getKey(),
                            new LSMDao.SnapshotHolder.Value(fileNumber, entry.getValue().getTimeStamp()));
                }
                fileNumber++;
            }

            private class Value {
                private Long index;
                private long timeStamp;

                public Value(Long index, @NotNull long timeStamp) {
                    this.index = index;
                    this.timeStamp = timeStamp;
                }

                public Long getIndex() {
                    return index;
                }

                public void setIndex(Long index) {
                    this.index = index;
                }

                public long getTimeStamp() {
                    return timeStamp;
                }

                public void setTimeStamp(@NotNull long timeStamp) {
                    this.timeStamp = timeStamp;
                }
            }
        }
    }
}
