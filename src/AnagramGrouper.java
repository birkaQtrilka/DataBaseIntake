import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public final class AnagramGrouper {
    //prevent instantiation
    private AnagramGrouper(){}

    public static HashMap<AnagramKey, List<String>> getTableInParallel(String filePath) throws Exception {
        return getTableInParallel(filePath, 16, 100000);
    }
    
    public static HashMap<AnagramKey, List<String>> getTableInParallel(String filePath, int numThreads, int batchSize) throws Exception {
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

    public static HashMap<AnagramKey, List<String>> getTableSequentially(String filePath) throws Exception {
        HashMap<AnagramKey, List<String>> table = new HashMap<>();
        try (Stream<String> lines = Files.lines(Paths.get(filePath)) ) {
            lines.forEach(line -> {
                AnagramKey key = new AnagramKey(line);
                if (table.containsKey(key)) {
                    table.get(key).add(line);
                    return;
                } 
                List<String> newList = new ArrayList<>();
                newList.add(line);
                table.put(key, newList);
            });
        }

        return table;
    }

    public static void writeTableToFile(HashMap<AnagramKey, List<String>> table, String outputPath) throws Exception {
        try (java.io.BufferedWriter writer = java.nio.file.Files.newBufferedWriter(java.nio.file.Paths.get(outputPath))) {
            for (List<String> group : table.values()) {
                writer.write(String.join(" ", group));
                System.out.println("Writing group: " + group);
                writer.newLine();
            }
        }
    }

}
