package storage.component;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author nedis
 * @since 1.0
 */
public final class Serializer {

    public void writePair(final DataOutputStream dataOutputStream,
                          final String key,
                          final String value) throws IOException {
        if (key.length() > Byte.MAX_VALUE) {
            throw new IllegalArgumentException(format("Max supported key length is %s, but actual is %s", Byte.MAX_VALUE, key.length()));
        }

        // Write key
        final byte[] keyBytes = key.getBytes(UTF_8);
        dataOutputStream.writeByte(keyBytes.length);
        dataOutputStream.write(keyBytes);

        // Write value
        if (value == null) {
            dataOutputStream.writeShort(0);
        } else {
            final byte[] valueBytes = value.getBytes(UTF_8);
            dataOutputStream.writeShort(valueBytes.length);
            dataOutputStream.write(valueBytes);
        }
    }

    public Map.Entry<String, String> readPair(final DataInputStream dataInputStream) throws IOException {
        // Read key
        final int keyLength = dataInputStream.readByte();
        final byte[] keyBytes = new byte[keyLength];
        if (dataInputStream.read(keyBytes) != keyLength) {
            throw new IllegalStateException("Invalid data: key bytes not found");
        }
        final String key = new String(keyBytes, UTF_8);

        // Read value
        final int valueLength = dataInputStream.readShort();
        final String value;
        if (valueLength > 0) {
            final byte[] valueBytes = new byte[valueLength];
            if (dataInputStream.read(valueBytes) != valueLength) {
                throw new IllegalStateException("Invalid data: value bytes not found");
            }
            value = new String(valueBytes, UTF_8);
        } else {
            value = null;
        }
        return new EntryImpl(key, value);
    }

    /**
     * @author nedis
     * @since 1.0
     */
    private static final class EntryImpl implements Map.Entry<String, String> {

        private final String key;

        private final String value;

        public EntryImpl(final String key,
                         final String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String setValue(final String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return "EntryImpl{" + "key='" + key + '\'' +
                    ", value='" + value + '\'' +
                    '}';
        }
    }
}
