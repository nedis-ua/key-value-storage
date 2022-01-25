package storage.component;

import java.io.IOException;

/**
 * @author nedis
 * @since 1.0
 */
public final class ChangesLog {

    private final Serializer serializer;

    private ChangesLogFile changesLogFile;

    public ChangesLog(final Serializer serializer,
                      final ChangesLogFile changesLogFile) {
        this.serializer = serializer;
        this.changesLogFile = changesLogFile;
    }

    public void add(final String key,
                    final String value) throws IOException {
        serializer.writePair(changesLogFile.getDataOutputStream(), key, value);
        changesLogFile.flush();
        changesLogFile.nextChangesLogFile()
                .ifPresent(logFile -> changesLogFile = logFile);
    }
}
