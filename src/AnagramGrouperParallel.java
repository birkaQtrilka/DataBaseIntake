import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class AnagramGrouperParallel extends AnagramGrouper{
    private int numThreads;
    private int batchSize;

    public AnagramGrouperParallel(String filePath, int numThreads, int batchSize) {
        super(filePath);
        this.batchSize = batchSize;
        this.numThreads = numThreads;
    }

    @Override
    public HashMap<AnagramKey, List<String>> getTable() throws Exception {
        final int BATCH_SIZE = batchSize;
        java.util.concurrent.BlockingQueue<List<String>> queue = new java.util.concurrent.LinkedBlockingQueue<>(100); // queue of batches
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<HashMap<AnagramKey, List<String>>>> futures = new ArrayList<>();

        // Poison pill for signaling end of file (empty list)
        final List<String> POISON_PILL = new ArrayList<>();

        // Start consumers
        for (int i = 0; i < numThreads; i++) {
            futures.add(executor.submit(() -> {
                HashMap<AnagramKey, List<String>> localTable = new HashMap<>();
                try {
                    while (true) {
                        List<String> batch = queue.take();
                        if (batch == POISON_PILL) break;
                        for (String line : batch) {
                            AnagramKey key = new AnagramKey(line);
                            localTable.computeIfAbsent(key, k -> new ArrayList<>()).add(line);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return localTable;
            }));
        }

        // Producer: read file and put batches in queue
        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            List<String> batch = new ArrayList<>(BATCH_SIZE);
            //using a custom iterator to handle checked exceptions (queue.put)
            for (java.util.Iterator<String> it = lines.iterator(); it.hasNext(); ) {
                batch.add(it.next());
                if (batch.size() == BATCH_SIZE) {
                    queue.put(new ArrayList<>(batch));
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                queue.put(batch);
            }
        }
        // Add poison pills to stop consumers
        for (int i = 0; i < numThreads; i++) {
            queue.put(POISON_PILL);
        }

        // Merge results
        HashMap<AnagramKey, List<String>> table = new HashMap<>();
        for (Future<HashMap<AnagramKey, List<String>>> future : futures) {
            HashMap<AnagramKey, List<String>> localTable = future.get();
            for (var entry : localTable.entrySet()) {
                table.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).addAll(entry.getValue());
            }
        }
        executor.shutdown();
        return table;
    }

    @Override
    public void writeTableToFile(HashMap<AnagramKey, List<String>> table, String outputPath) throws Exception {
        try (java.io.BufferedWriter writer = java.nio.file.Files.newBufferedWriter(java.nio.file.Paths.get(outputPath))) {
            for (List<String> group : table.values()) {
                writer.write(String.join(" ", group));
                System.out.println("Writing group: " + group);
                writer.newLine();
            }
        }
    }
}