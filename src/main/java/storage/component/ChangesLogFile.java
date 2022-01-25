package storage.component;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

/**
 * @author nedis
 * @since 1.0
 */
public final class ChangesLogFile {

    private final File logFile;

    private final long maxFileSizeInBytes;

    private final DataOutputStream dataOutputStream;

    public ChangesLogFile(final File logsDirectory,
                          final long maxFileSizeInBytes) {
        this.logFile = findOrCreateLogFile(logsDirectory, maxFileSizeInBytes);
        this.maxFileSizeInBytes = maxFileSizeInBytes;
        this.dataOutputStream = createDataOutputStream();
    }

    private ChangesLogFile(final long maxFileSizeInBytes,
                           final File logFile) {
        this.logFile = logFile;
        this.maxFileSizeInBytes = maxFileSizeInBytes;
        this.dataOutputStream = createDataOutputStream();
    }

    private DataOutputStream createDataOutputStream() {
        try {
            return new CustomDataOutputStream(
                    (int) logFile.length(),
                    new BufferedOutputStream(
                            new FileOutputStream(logFile, logFile.exists())
                    )
            );
        } catch (final FileNotFoundException impossibleException) {
            throw new IllegalStateException(impossibleException);
        }
    }

    public DataOutputStream getDataOutputStream() {
        return dataOutputStream;
    }

    public void flush() throws IOException {
        dataOutputStream.flush();
    }

    public Optional<ChangesLogFile> nextChangesLogFile() throws IOException {
        if (dataOutputStream.size() >= maxFileSizeInBytes) {
            dataOutputStream.flush();
            dataOutputStream.close();
            final File logsDirectory = logFile.getParentFile();
            final int currentFileNumber = Integer.parseInt(logFile.getName());
            return Optional.of(
                    new ChangesLogFile(maxFileSizeInBytes, new File(logsDirectory, String.valueOf(currentFileNumber + 1)))
            );
        } else {
            return Optional.empty();
        }
    }

    private static File findOrCreateLogFile(final File logsDirectory,
                                            final long maxFileSizeInBytes) {
        final Optional<File> optionalFileWithMaxNumber = getFileWithMaxNumber(logsDirectory);
        if (optionalFileWithMaxNumber.isPresent()) {
            final File fileWithMaxNumber = optionalFileWithMaxNumber.get();
            if (fileWithMaxNumber.length() < maxFileSizeInBytes) {
                return fileWithMaxNumber;
            } else {
                return new File(logsDirectory, String.valueOf(Integer.parseInt(fileWithMaxNumber.getName())) + 1);
            }
        }
        return new File(logsDirectory, "10");
    }

    private static Optional<File> getFileWithMaxNumber(final File logsDirectory) {
        final String[] listFileNames = logsDirectory.list();
        if (listFileNames != null && listFileNames.length > 0) {
            int maxName = Integer.parseInt(listFileNames[0]);
            for (int i = 1; i < listFileNames.length; i++) {
                final int currentName = Integer.parseInt(listFileNames[i]);
                if (currentName > maxName) {
                    maxName = currentName;
                }
            }
            return Optional.of(new File(logsDirectory, String.valueOf(maxName)));
        } else {
            return Optional.empty();
        }
    }

    /**
     * @author nedis
     * @since 1.0
     */
    private static final class CustomDataOutputStream extends DataOutputStream {

        public CustomDataOutputStream(final int fileSize,
                                      final OutputStream out) {
            super(out);
            written = fileSize;
        }
    }
}
