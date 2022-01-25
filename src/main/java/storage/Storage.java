package storage;

import storage.component.ChangesLog;
import storage.component.Memtable;

import java.io.IOException;

/**
 * @author nedis
 * @since 1.0
 */
public final class Storage {

    private final Memtable memtable;

    private final ChangesLog changesLog;

    public Storage(final Memtable memtable,
                   final ChangesLog changesLog) {
        this.memtable = memtable;
        this.changesLog = changesLog;
    }

    public synchronized void put(final String key,
                                 final String value) throws IOException {
        changesLog.add(key, value);
        memtable.put(key, value);
    }

    public synchronized String get(final String key) {
        return memtable.get(key);
    }

    public synchronized void remove(final String key) throws IOException {
        changesLog.add(key, null);
        memtable.remove(key);
    }
}
