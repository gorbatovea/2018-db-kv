package LSMDao;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

public class LSMDao implements KVDao {
    private final static String DEFAULT_CONFIG_PATH = "configuration.properties";
    private final static String PROP_LSM_MEMORY_TABLE_SIZE = "lsm.memory.table.size";
    private final static int IN_KBITES = 1024;
    private final static int DEFAULT_MEM_TABLE_TRASH_HOLD = 40 * IN_KBITES;
    private final int MEM_TABLE_TRASH_HOLD;
    private final String STORAGE_DIR;

    private final Map<ByteBuffer, Value> memTable = new HashMap<>();

    private final SnapshotHolder holder;

    private int memTableSize = 0;

    public LSMDao(final File dir) throws IOException {
        this.STORAGE_DIR = dir + File.separator;
        this.holder = new SnapshotHolder(this.STORAGE_DIR);

        int tableSize;

        try {
            Properties properties = new Properties();
            properties.load(ClassLoader.getSystemResourceAsStream(DEFAULT_CONFIG_PATH));
            tableSize = Integer.parseInt(properties.getProperty(PROP_LSM_MEMORY_TABLE_SIZE)) * IN_KBITES;
            if (tableSize < 1) throw new IllegalArgumentException();
        } catch (Exception e) {
            e.printStackTrace();
            tableSize = DEFAULT_MEM_TABLE_TRASH_HOLD;
        }

        this.MEM_TABLE_TRASH_HOLD = tableSize;
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws IOException, NoSuchElementException {
        ByteBuffer keyBuffer = ByteBuffer.wrap(key);
        synchronized (this) {
            Optional<Value> value = Optional.ofNullable(this.memTable.get(keyBuffer));

            return value.isPresent() ? value
                    .filter(v -> v.getBytes() != SnapshotHolder.REMOVED_VALUE)
                    .orElseThrow(NoSuchElementException::new)
                    .getBytes()
                    : this.holder.get(keyBuffer);
        }
    }

    public LSMDao.Value getWithMeta(@NotNull byte[] key) throws IOException, NoSuchElementException {
        ByteBuffer keyBuffer = ByteBuffer.wrap(key);
        Optional<Value> value;

        synchronized (this) {
            value = Optional.ofNullable(this.memTable.get(keyBuffer));
        }

        if (value.isPresent()) {
            return value
                    .map(v -> v.getBytes() == SnapshotHolder.REMOVED_VALUE ? new Value(null, v.getTimeStamp()) : v)
                    .get();
        }

        return this.holder.getWithMeta(keyBuffer);
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        ByteBuffer keyBuffer = ByteBuffer.wrap(key);
        synchronized (this) {
            Value v = new Value(value, System.currentTimeMillis());
            this.memTable.put(keyBuffer, v);
            this.memTableSize += key.length + v.getSize();
            flush();
        }

    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        ByteBuffer keyBuffer = ByteBuffer.wrap(key);
        synchronized (this) {
            Value wrapper = this.memTable.get(keyBuffer);
            if (wrapper == null) {
                if (this.holder.contains(keyBuffer)) {
                    Value v = new Value(SnapshotHolder.REMOVED_VALUE, System.currentTimeMillis());
                    this.memTable.put(keyBuffer, v);
                    this.memTableSize += key.length + v.getSize();
                }
            } else {
                byte[] value = wrapper.getBytes();
                if (value != SnapshotHolder.REMOVED_VALUE) {
                    Value v = new Value(SnapshotHolder.REMOVED_VALUE, System.currentTimeMillis());
                    this.memTableSize -= wrapper.getSize();
                    this.memTableSize += v.getSize();
                    this.memTable.put(keyBuffer, v);
                }
            }
            flush();
        }
    }

    private void flush() throws IOException {
        if (memTableSize >= MEM_TABLE_TRASH_HOLD) {
            this.holder.save(this.memTable, this.memTableSize);
            this.memTable.clear();
            this.memTableSize = 0;
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            this.holder.save(this.memTable, memTableSize);
            this.memTable.clear();
            this.holder.close();
        }
    }

    public static class Value {
        private final byte[] value;
        private final long timeStamp;

        public Value(byte[] value, long timeStamp) {
            this.value = value;
            this.timeStamp = timeStamp;
        }

        public byte[] getBytes() {
            return value;
        }

        public Long getTimeStamp() {
            return timeStamp;
        }

        public long getSize() {
            return this.value.length + Long.BYTES;
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
            HashMap<ByteBuffer, Long> marks = new HashMap<>();
            try {
                java.nio.file.Files.walkFileTree(
                        this.storage.toPath(),
                        new SimpleFileVisitor<Path>() {
                            private void fetchData(@NotNull final Path file) throws IOException {
                                RandomAccessFile randomAccessFile = new RandomAccessFile(file.toFile(), "r");
                                long fileMark = randomAccessFile.readLong();
                                if (fileNumber < fileMark) fileNumber = fileMark;
                                int amount = randomAccessFile.readInt();
                                for (int i = 0; i < amount; i++) {
                                    byte[] bytes = new byte[randomAccessFile.readInt()];
                                    randomAccessFile.read(bytes);
                                    int offset = randomAccessFile.readInt();
                                    long timeStamp = randomAccessFile.readLong();
                                    ByteBuffer keyBuffer = ByteBuffer.wrap(bytes);
                                    if (offset == REMOVED_MARK) {
                                        Long lastMark = marks.get(keyBuffer);
                                        if (lastMark == null) {
                                            marks.put(keyBuffer, fileMark);
                                        } else {
                                            if (lastMark < fileMark) {
                                                sSMap.put(keyBuffer, new LSMDao.SnapshotHolder.Value(null, timeStamp));
                                                marks.put(keyBuffer, fileMark);
                                            }
                                        }
                                    } else {
                                        Long itemTStamp = marks.get(keyBuffer);
                                        if (itemTStamp == null) {
                                            marks.put(keyBuffer, fileMark);
                                            sSMap.put(
                                                    keyBuffer,
                                                    new LSMDao.SnapshotHolder.Value(
                                                            Long.parseLong(file.toFile().getName()), timeStamp));
                                        } else {
                                            if (itemTStamp < fileMark) {
                                                sSMap.put(
                                                        keyBuffer,
                                                        new LSMDao.SnapshotHolder.Value(
                                                                Long.parseLong(file.toFile().getName()),
                                                                timeStamp));
                                                marks.put(keyBuffer, fileMark);
                                            }
                                        }
                                    }
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

        public LSMDao.Value getWithMeta(final ByteBuffer key) throws IOException, NoSuchElementException {

            Value value;
            synchronized (this) {
                value = sSMap.get(key);
            }

            if (value == null) {
                throw new NoSuchElementException();
            } else if (value.getIndex() == null) {
                return new LSMDao.Value(null, value.getTimeStamp());
            } else {
                return new LSMDao.Value(this.get(key), value.getTimeStamp());
            }
        }

        public byte[] get(final ByteBuffer key) throws IOException, NoSuchElementException {

            LSMDao.SnapshotHolder.Value index;
            synchronized (this) {
                index = this.sSMap.get(key);
            }

            if (index == null) throw new NoSuchElementException();
            if (index.getIndex() == null) throw new NoSuchElementException();
            File source = new File(this.storage.toString() + File.separator + index.getIndex().toString());
            if (!source.canRead() || !source.exists() || !source.isFile()) throw new IOException();

            RandomAccessFile randomAccessFile = new RandomAccessFile(source, "r");
            MappedByteBuffer mappedByteBuffer =
                    randomAccessFile
                            .getChannel()
                            .map(FileChannel.MapMode.READ_ONLY, 0, source.length());

            mappedByteBuffer.position(Long.BYTES);

            int valueOffset = REMOVED_MARK;
            int amount = mappedByteBuffer.getInt();
            for (int i = 0; i < amount; i++) {
                byte[] bytes = new byte[mappedByteBuffer.getInt()];
                mappedByteBuffer.get(bytes);
                int offSet = mappedByteBuffer.getInt();
                mappedByteBuffer.getLong();
                if (Arrays.equals(bytes, key.array())) {
                    if (offSet != REMOVED_MARK) {
                        valueOffset = offSet;
                    } else throw new NoSuchElementException();
                }
            }

            mappedByteBuffer.position(mappedByteBuffer.position() + valueOffset);

            byte[] value = new byte[mappedByteBuffer.getInt()];
            mappedByteBuffer.get(value);
            randomAccessFile.close();
            return value;
        }

        public boolean contains(final ByteBuffer key) {
            return this.sSMap.containsKey(key);
        }

        public void save(final Map<ByteBuffer, LSMDao.Value> source, final int bytes) throws IOException {
            synchronized (this) {
                if (source.size() == 0) return;
                int offset = 0;
                File dist = new File(this.storage + File.separator + fileNumber.toString());
                if (!dist.createNewFile()) throw new IOException();

                int size = Long.BYTES
                        + Integer.BYTES
                        + (Integer.BYTES + Integer.BYTES + Integer.BYTES) * source.size()
                        + bytes;

                ByteBuffer buffer = ByteBuffer.allocate(size)
                        .putLong(this.fileNumber)
                        .putInt(source.size());

                for (Map.Entry<ByteBuffer, LSMDao.Value> entry : source.entrySet()) {

                    buffer.putInt(entry.getKey().capacity())
                            .put(entry.getKey().array());

                    if (entry.getValue().getBytes() == REMOVED_VALUE) {
                        buffer.putInt(REMOVED_MARK);
                    } else {
                        buffer.putInt(offset);
                        offset += Integer.BYTES + entry.getValue().getBytes().length;
                    }

                    buffer.putLong(entry.getValue().getTimeStamp());
                }

                for (Map.Entry<ByteBuffer, LSMDao.Value> entry : source.entrySet()) {
                    buffer
                            .putInt(entry.getValue().getBytes().length)
                            .put(entry.getValue().getBytes());
                }

                OutputStream outputStream = new FileOutputStream(dist);
                outputStream.write(buffer.array());
                outputStream.flush();
                outputStream.close();

                for (Map.Entry<ByteBuffer, LSMDao.Value> entry : source.entrySet()) {
                    if (entry.getValue().getBytes() != REMOVED_VALUE) {
                        this.sSMap.put(
                                entry.getKey(),
                                new Value(fileNumber, entry.getValue().getTimeStamp())
                        );
                    } else {
                        this.sSMap.put(
                                entry.getKey(),
                                new Value(null, entry.getValue().getTimeStamp())
                        );
                    }
                }
                this.fileNumber++;
            }
        }

        public void close() {
            this.sSMap.clear();
        }

        private class Value {
            private final Long index;
            private final long timeStamp;

            public Value(final Long index, final long timeStamp) {
                this.index = index;
                this.timeStamp = timeStamp;
            }

            public Long getIndex() {
                return index;
            }

            public long getTimeStamp() {
                return timeStamp;
            }
        }
    }
}
