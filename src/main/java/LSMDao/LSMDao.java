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
    final private int MEM_TABLE_TRASH_HOLD = 1024 * 40;
    //    final private int MEM_TABLE_TRASH_HOLD = 1024 * 10;
    final private String STORAGE_DIR;

    final private SortedMap<ByteBuffer, Value> memTable = new TreeMap<>();

    final private SnapshotHolder holder;

    private int memTableSize = 0;

    public LSMDao(final File dir) throws IOException {
        this.STORAGE_DIR = dir + File.separator;
        this.holder = new SnapshotHolder(this.STORAGE_DIR);
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws IOException, NoSuchElementException {
        synchronized (this) {
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
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        synchronized (this) {
            Value v = new Value(value, System.currentTimeMillis());
            this.memTable.put(ByteBuffer.wrap(key), v);
            this.memTableSize += key.length + v.getSize();
            flush();
        }

    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException{
        synchronized (this) {
            Value wrapper = this.memTable.get(ByteBuffer.wrap(key));
            if (wrapper == null) {
                if (this.holder.contains(key)) {
                    Value v = new Value(SnapshotHolder.REMOVED_VALUE, System.currentTimeMillis());
                    this.memTable.put(ByteBuffer.wrap(key), v);
                    this.memTableSize += v.getSize();
                }
            } else {
                byte[] value = wrapper.getValue();
                if (value != SnapshotHolder.REMOVED_VALUE) {
                    Value v = new Value(SnapshotHolder.REMOVED_VALUE, System.currentTimeMillis());
                    this.memTableSize -=  wrapper.getSize();
                    this.memTableSize += v.getSize();
                    this.memTable.put(ByteBuffer.wrap(key), v);
                }
            }
            flush();
        }
    }

    private void flush() throws IOException{
        if (memTableSize >= MEM_TABLE_TRASH_HOLD) {
            this.holder.save(this.memTable, this.memTableSize);
            this.memTable.clear();
            this.memTableSize = 0;
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            synchronized (this.holder) {
                this.holder.save(this.memTable, memTableSize);
                this.memTable.clear();
                this.holder.close();
            }
        }
    }

    public LSMDao.Value getWithMeta(@NotNull byte[] key) throws IOException, NoSuchElementException{
        Value value;
        synchronized (this) {
            value = this.memTable.get(ByteBuffer.wrap(key));
        }
        if (value == null) {
            return this.holder.getWithMeta(key);
        } else if (value.getValue() == SnapshotHolder.REMOVED_VALUE) {
            return new Value(null, value.getTimeStamp());
        } else {
            return new Value(value.getValue(), value.getTimeStamp());
        }
    }

    public static class Value {
        private byte[] value;
        private long timeStamp;

        public Value(byte[] value, long timeStamp) {
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

        public void setTimeStamp(long timeStamp) {
            this.timeStamp = timeStamp;
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
            LSMDao.SnapshotHolder.Value index;
            synchronized (this) {
                index = this.sSMap.get(ByteBuffer.wrap(key));
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
                if (Arrays.equals(bytes, key)) {
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

        public LSMDao.Value getWithMeta(byte[] key) throws IOException, NoSuchElementException{
            SnapshotHolder.Value value = sSMap.get(ByteBuffer.wrap(key));
            if (value == null) throw new NoSuchElementException();
            else if (value.getIndex() == null)
                return new LSMDao.Value(null, value.getTimeStamp());
            else
                return new LSMDao.Value(this.get(key), value.getTimeStamp());
        }

        public boolean contains(byte[] key) {
            return this.sSMap.containsKey(ByteBuffer.wrap(key));
        }

        public void save(SortedMap<ByteBuffer, LSMDao.Value> source, int bytes) throws IOException {
            int offset = 0;
            synchronized (this) {
                File dist = new File(this.storage + File.separator + fileNumber.toString());
                if (!dist.createNewFile()) throw new IOException();

                int size = Long.BYTES
                        + Integer.BYTES
                        + (Integer.BYTES + Integer.BYTES + Integer.BYTES) * source.size()
                        + bytes;

                ByteBuffer buffer = ByteBuffer.allocate(size)
                        .putLong(System.currentTimeMillis())
                        .putInt(source.size());

                for (Map.Entry<ByteBuffer, LSMDao.Value> entry : source.entrySet()) {

                    buffer
                            .putInt(entry.getKey().capacity())
                            .put(entry.getKey());

                    if (entry.getValue().getValue() == REMOVED_VALUE) {
                        buffer.putInt(REMOVED_MARK);
                    } else {
                        buffer.putInt(offset);
                        offset += Integer.BYTES + entry.getValue().getValue().length;
                    }

                    buffer.putLong(entry.getValue().getTimeStamp());
                }

                for (Map.Entry<ByteBuffer, LSMDao.Value> entry : source.entrySet()) {
                    buffer
                            .putInt(entry.getValue().getValue().length)
                            .put(entry.getValue().getValue());
                }

                OutputStream outputStream = new FileOutputStream(dist);
                outputStream.write(buffer.array());
                outputStream.flush();
                outputStream.close();
                fileNumber++;

                for (Map.Entry<ByteBuffer, LSMDao.Value> entry : source.entrySet()) {
                    this.sSMap.put(
                            entry.getKey(),
                            new LSMDao.SnapshotHolder.Value(fileNumber, entry.getValue().getTimeStamp())
                    );
                }
            }
        }

        public void close() {
            this.sSMap.clear();
        }

        private class Value {
            private Long index;
            private long timeStamp;

            public Value(Long index, long timeStamp) {
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

            public void setTimeStamp(long timeStamp) {
                this.timeStamp = timeStamp;
            }
        }
    }
}
