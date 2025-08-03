import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import java.util.concurrent.BlockingQueue;
import java.io.IOException;

public class BatchProducer {
    public static void produceBatches(Path inputPath, int batchSize, BlockingQueue<List<String>> queue, int numConsumers) throws IOException, InterruptedException {
        List<String> batch = new ArrayList<>(batchSize);
        try (Stream<String> lines = Files.lines(inputPath)) {
            for (String line : (Iterable<String>) lines::iterator) {
                batch.add(line);
                if (batch.size() == batchSize) {
                    queue.put(new ArrayList<>(batch));
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) queue.put(batch);
        }

        // Poison pills
        for (int i = 0; i < numConsumers; i++) {
            queue.put(Collections.emptyList());
        }
    }
}