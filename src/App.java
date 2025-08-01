import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class App {
    public static void main(String[] args) throws Exception {
        int runs = 5;
        String filePath = "anagrams_1_000_000.txt";

        // Parallel version timing
        double totalParallel = 0;
        HashMap<AnagramKey, List<String>> tableParallel = null;
        for (int i = 0; i < runs; i++) {
            long startTime = System.nanoTime();
            tableParallel = getAnagramTableParallel(filePath);
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            System.out.printf("Parallel Run %d: %.2f ms\n", i + 1, durationMs);
            totalParallel += durationMs;
        }
        double avgParallel = totalParallel / runs;
        System.out.printf("Parallel average execution time: %.2f ms\n", avgParallel);
        System.out.println("Parallel table size: " + tableParallel.size());
        // Synchronous version timing
        double totalSync = 0;
        HashMap<AnagramKey, List<String>> tableSync = null;
        for (int i = 0; i < runs; i++) {
            long startTime = System.nanoTime();
            tableSync = getAnagramTableSync(filePath);
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            System.out.printf("Sync Run %d: %.2f ms\n", i + 1, durationMs);
            totalSync += durationMs;
        }
        double avgSync = totalSync / runs;
        System.out.printf("Sync average execution time: %.2f ms\n", avgSync);
        System.out.println("Sync table size: " + tableSync.size());
    }

    public static HashMap<AnagramKey, List<String>> getAnagramTableParallel(String filePath) throws Exception {
        int numThreads = Runtime.getRuntime().availableProcessors();

        List<String> allLines = Files.readAllLines(Paths.get(filePath));
        int totalLines = allLines.size();
        int chunkSize = (int) Math.ceil((double) totalLines / numThreads);
        // System.out.println("Using " + numThreads + " threads to process " + totalLines + " lines with chunk size " + chunkSize);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<HashMap<AnagramKey, List<String>>>> futures = new ArrayList<>();

        for (int i = 0; i < numThreads; i++) {
            int start = i * chunkSize;
            int end = Math.min(start + chunkSize, totalLines);
            if (start >= end) break;
            List<String> chunk = allLines.subList(start, end);
            futures.add(executor.submit(() -> {
                HashMap<AnagramKey, List<String>> localTable = new HashMap<>();
                for (String line : chunk) {
                    AnagramKey key = new AnagramKey(line);
                    localTable.computeIfAbsent(key, k -> new ArrayList<>()).add(line);
                }
                return localTable;
            }));
        }

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
        try (Stream<String> lines = Files.lines(Paths.get("anagrams_10000.txt"))) {
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
}
