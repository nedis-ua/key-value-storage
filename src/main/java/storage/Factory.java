package storage;

import storage.component.ChangesLog;
import storage.component.ChangesLogFile;
import storage.component.Memtable;
import storage.component.Restorer;
import storage.component.Serializer;
import storage.merger.OldCommitLogMerger;

import java.io.IOException;

/**
 * @author nedis
 * @since 1.0
 */
public final class Factory {

    private final Storage storage;

    private final OldCommitLogMerger oldCommitLogMerger;

    public Factory(final Config config) throws IOException {
        final Serializer serializer = new Serializer();
        final Memtable memtable = new Memtable();

        final Restorer restorer = new Restorer(config.getLogsDirectory(), serializer);
        restorer.restoreMemtable(memtable);

        final ChangesLogFile changesLogFile = new ChangesLogFile(config.getLogsDirectory(), config.getMaxFileSizeInBytes());
        final ChangesLog changesLog = new ChangesLog(serializer, changesLogFile);

        storage = new Storage(memtable, changesLog);

        oldCommitLogMerger = new OldCommitLogMerger(config.getLogsDirectory(), config.getMaxFileSizeInBytes(), files -> files.size() > 2, serializer);
    }

    public Storage getStorage(){
        return storage;
    }

    public OldCommitLogMerger getOldCommitLogMerger() {
        return oldCommitLogMerger;
    }
}
