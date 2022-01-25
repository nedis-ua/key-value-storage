package storage.component;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeSet;

/**
 * @author nedis
 * @since 1.0
 */
public final class Restorer {

    private final File logsDirectory;

    private final Serializer serializer;

    public Restorer(final File logsDirectory,
                    final Serializer serializer) {
        this.logsDirectory = logsDirectory;
        this.serializer = serializer;
    }

    public void restoreMemtable(final Memtable memtable) throws IOException {
        final Collection<File> logFiles = getOrderedLogFiles();
        for (final File logFile : logFiles) {
            restore(memtable, logFile);
        }
    }

    private void restore(final Memtable memtable,
                         final File logFile) throws IOException {
        try (DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(logFile)))) {
            while (true) {
                try {
                    final Map.Entry<String, String> entry = serializer.readPair(dataInputStream);
                    memtable.put(entry.getKey(), entry.getValue());
                } catch (final EOFException endOfFile) {
                    break;
                } catch (final IllegalStateException ignored) {
                    // do nothing if commit log file has invalid content
                }
            }
        }
    }

    /**
     * Returns 0, 1, 2, ..., 10, 11, 12, ... files
     */
    private Collection<File> getOrderedLogFiles() {
        final File[] files = logsDirectory.listFiles();
        if (files != null) {
            final TreeSet<File> treeSet = new TreeSet<>(Comparator.comparingInt(o -> Integer.parseInt(o.getName())));
            treeSet.addAll(Arrays.asList(files));
            return treeSet;
        } else {
            return Collections.emptySet();
        }
    }
}
