package storage.merger;

import storage.component.Memtable;
import storage.component.Serializer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Predicate;

/**
 * @author nedis
 * @since 1.0
 */
public class OldCommitLogMerger implements Runnable {

    private final File logsDirectory;

    private final File mergedFile;

    private final long maxFileSizeInBytes;

    private final Predicate<List<File>> runPredicate;

    private final Serializer serializer;

    public OldCommitLogMerger(final File logsDirectory,
                              final long maxFileSizeInBytes,
                              final Predicate<List<File>> runPredicate,
                              final Serializer serializer) {
        this.logsDirectory = logsDirectory;
        this.maxFileSizeInBytes = maxFileSizeInBytes;
        this.runPredicate = runPredicate;
        this.mergedFile = new File(logsDirectory, "0").getAbsoluteFile();
        this.serializer = serializer;
    }

    @Override
    public void run() {
        try {
            final List<File> filesToMerge = findFilesToMergeExceptLastLog();
            if (runPredicate.test(filesToMerge)) {
                merge(filesToMerge);
                removeOldFiles(filesToMerge);
                splitMergedFile();
                removeFile(mergedFile);
                System.out.println("Merge successful");
            } else {
                System.out.println("Nothing to merge");
            }
        } catch (final IOException exception) {
            exception.printStackTrace();
        }
    }

    private List<File> findFilesToMergeExceptLastLog() {
        final File[] files = logsDirectory.listFiles();
        if (files != null) {
            final TreeSet<File> treeSet = new TreeSet<>(Comparator.comparingInt(o -> Integer.parseInt(o.getName())));
            treeSet.addAll(Arrays.asList(files));
            return new ArrayList<>(treeSet).subList(0, treeSet.size() - 1);
        } else {
            return Collections.emptyList();
        }
    }

    private void merge(final List<File> filesToMerge) throws IOException {
        final Memtable memtable = new Memtable();
        for (final File file : filesToMerge) {
            try(DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
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

        try (DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(mergedFile)))) {
            for (final Map.Entry<String, String> entry : memtable.entrySet()) {
                serializer.writePair(dataOutputStream, entry.getKey(), entry.getValue());
            }
        }
    }

    private void removeOldFiles(final List<File> filesToMerge) {
        filesToMerge.forEach(this::removeFile);
    }

    private void splitMergedFile() throws IOException {
        int index = 1;
        try (DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(mergedFile)))) {
            while (true) {
                try {
                    final File destFile = new File(logsDirectory, String.valueOf(index++));
                    try (DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(destFile)))) {
                        int copied = 0;
                        while (true) {
                            copied += copyDataPart(dataInputStream, dataOutputStream);
                            if (copied >= maxFileSizeInBytes) {
                                dataOutputStream.flush();
                                break;
                            }
                        }
                    }
                } catch (final EOFException eofException) {
                    break;
                }
            }
        }
    }

    private int copyDataPart(final DataInputStream dataInputStream,
                             final DataOutputStream dataOutputStream) throws IOException {
        // Read key
        final int keyLength = dataInputStream.readByte();
        dataOutputStream.writeByte(keyLength);
        final byte[] keyBytes = new byte[keyLength];
        if (dataInputStream.read(keyBytes) != keyLength) {
            throw new IllegalStateException("Invalid data: key bytes not found");
        }
        dataOutputStream.write(keyBytes);
        // Read value
        final int valueLength = dataInputStream.readShort();
        dataOutputStream.writeShort(valueLength);
        if (valueLength > 0) {
            final byte[] valueBytes = new byte[valueLength];
            if (dataInputStream.read(valueBytes) != valueLength) {
                throw new IllegalStateException("Invalid data: value bytes not found");
            }
            dataOutputStream.write(valueBytes);
            return 1 + keyLength + 4 + valueLength;
        } else {
            return 1 + keyLength + 4;
        }
    }

    private void removeFile(final File file) {
        if (!file.delete()) {
            System.err.println("Can't delete file: " + file.getAbsolutePath());
        }
    }
}
