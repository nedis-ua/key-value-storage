package storage;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author nedis
 * @since 1.0
 */
public class TestLauncher {

    private static final int THREAD_COUNT = 8;

    public static void main(String[] args) throws IOException {
        final Config config = new Config()
                .setMaxFileSizeInBytes(150);
        final Factory factory = new Factory(config);

        final Storage storage = factory.getStorage();

        final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        readValues(storage, executorService);

        sleep(Duration.ofSeconds(1));

        writeValues(storage, executorService);

        sleep(Duration.ofSeconds(1));

        readValues(storage, executorService);

        sleep(Duration.ofSeconds(1));

        executorService.shutdown();

        factory.getOldCommitLogMerger().run();
    }

    private static void writeValues(final Storage storage,
                                    final ExecutorService executorService) {
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int index = i;
            executorService.submit(() -> {
                sleep(Duration.ofMillis(100));
                try {
                    final String value = storage.get("key-" + index);
                    if (value == null) {
                        storage.put("key-" + index, "value-" + index);
                    } else {
                        storage.put("key-" + index, value + index);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private static void readValues(final Storage storage,
                                   final ExecutorService executorService) {
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int index = i;
            executorService.submit(() -> {
                sleep(Duration.ofMillis(100));
                System.out.println("key-" + index + " -> " + storage.get("key-" + index));
            });
        }
    }

    private static void sleep(final Duration duration) {
        try {
            TimeUnit.MILLISECONDS.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
