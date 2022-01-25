package storage;

import java.io.File;

/**
 * @author nedis
 * @since 1.0
 */
public final class Config {

    private static final File DEFAULT_LOGS_DIRECTORY = new File("data").getAbsoluteFile();

    private static final long DEFAULT_MAX_FILE_SIZE_IN_BYTES = 10 * 1024 * 1024; // 10 Mb

    private File logsDirectory = DEFAULT_LOGS_DIRECTORY;

    private long maxFileSizeInBytes = DEFAULT_MAX_FILE_SIZE_IN_BYTES;

    public Config setLogsDirectory(final File logsDirectory) {
        this.logsDirectory = logsDirectory;
        return this;
    }

    public Config setMaxFileSizeInBytes(final long maxFileSizeInBytes) {
        this.maxFileSizeInBytes = maxFileSizeInBytes;
        return this;
    }

    public File getLogsDirectory() {
        return logsDirectory;
    }

    public long getMaxFileSizeInBytes() {
        return maxFileSizeInBytes;
    }
}
