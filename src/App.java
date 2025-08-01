
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.stream.Stream;

public class App {
    public static void main(String[] args) throws Exception {
        String filePath = "sample.txt";
        var table = getAnagramTableSync(filePath); 
        writeAnagramGroupsToFile(table, "output.txt");
    }

    public static void Test1() throws Exception {
        int runs = 30;
        String filePath = "anagrams_1_000_000_000.txt";

        System.out.println("\n\nDifferent batch sizes for 8 threads: \n");
        ParallelTest(filePath, runs, 8, 1000); 
        ParallelTest(filePath, runs, 8, 10_000); 
        ParallelTest(filePath, runs, 8, 100_000); 
        ParallelTest(filePath, runs, 8, 1_000_000); 

        System.out.println("\n\nDifferent batch sizes for 12 threads: \n");
        ParallelTest(filePath, runs, 12, 1000); 
        ParallelTest(filePath, runs, 12, 10_000); 
        ParallelTest(filePath, runs, 12, 100_000); 
        ParallelTest(filePath, runs, 12, 1_000_000); 

        System.out.println("\n\nDifferent batch sizes for 16 threads: \n");
        ParallelTest(filePath, runs, 16, 1000); 
        ParallelTest(filePath, runs, 16, 10_000); 
        ParallelTest(filePath, runs, 16, 100_000); 
        ParallelTest(filePath, runs, 16, 1_000_000); 
    }

    public static void ParallelTest(String filePath, int runs, int numThreads, int batchSize) throws Exception {
        double totalParallel = 0;
        System.out.println("Using " + numThreads + " threads. Batch size: " + batchSize);
        for (int i = 0; i < runs; i++) {
            System.gc();
            Thread.sleep(100);
            long startTime = System.nanoTime();
            HashMap<AnagramKey, List<String>> tableParallel  = getAnagramTableParallel(filePath, numThreads, batchSize);
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            totalParallel += durationMs;
        }
        double avgParallel = totalParallel / runs;
        System.out.printf("Parallel average execution time: %.2f ms\n", avgParallel);

    }

    public static void SyncTest(String filePath, int runs) throws Exception {
        double totalSync = 0;
        
        for (int i = 0; i < runs; i++) {
            System.gc();
            Thread.sleep(100);
            long startTime = System.nanoTime();
            HashMap<AnagramKey, List<String>> tableSync = getAnagramTableSync(filePath);
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            // System.out.printf("Sync Run %d: %.2f ms\n", i + 1, durationMs);
            totalSync += durationMs;
        }
        double avgSync = totalSync / runs;
        System.out.printf("Sync average execution time: %.2f ms\n", avgSync);
    }

    public static HashMap<AnagramKey, List<String>> getAnagramTableParallel(String filePath, int numThreads, int batchSize) throws Exception {
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


    public static HashMap<AnagramKey, List<String>> getAnagramTableSync(String filePath) throws Exception {
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

    public static void writeAnagramGroupsToFile(HashMap<AnagramKey, List<String>> table, String outputPath) throws Exception {
        try (java.io.BufferedWriter writer = java.nio.file.Files.newBufferedWriter(java.nio.file.Paths.get(outputPath))) {
            for (List<String> group : table.values()) {
                writer.write(String.join(" ", group));
                writer.newLine();
            }
        }
    }
    
}
