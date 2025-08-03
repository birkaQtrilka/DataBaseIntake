import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class TestKit {
    public void Test2() throws Exception {
        String filePath = "anagrams_1_000_000_000.txt";

        //AsyncTest(filePath,4);
        System.out.println("testing sharding method");
        SyncTest(1, ()-> new AnagramGrouperSharding(
            filePath, 
            16, 
            100_000, 
            1000,
            "Shards", 
            10));
    }

    public void Test3() throws Exception {
        String filePath = "anagrams_1_000_000_000.txt";
        SequentialTest(filePath, 5);
        ParallelTest(filePath, 1, 16, 100000); 

    }

    public void Test1() throws Exception {
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

    public void ParallelTest(String filePath, int runs, int numThreads, int batchSize) throws Exception {
        double totalParallel = 0;
        System.out.println("Using " + numThreads + " threads. Batch size: " + batchSize);
        for (int i = 0; i < runs; i++) {
            System.gc();
            Thread.sleep(100);
            long startTime = System.nanoTime();
            new AnagramGrouperParallel(filePath, numThreads, batchSize).getTable();
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            totalParallel += durationMs;
        }
        double avgParallel = totalParallel / runs;
        System.out.printf("Parallel average execution time: %.2f ms\n", avgParallel);

    }

    public void AsyncTest(String filePath, int runs) throws Exception {
        // Ensure garbage collection before starting
        System.gc();
        Thread.sleep(100);

        int numThreads = 16;
        int batchSize = 1_000_000;

        ExecutorService executor = Executors.newFixedThreadPool(runs);

        long startTime = System.nanoTime();

        // Create a list of CompletableFutures
        List<CompletableFuture<HashMap<AnagramKey, List<String>>>> futures = new ArrayList<>();

        for (int i = 0; i < runs; i++) {
            CompletableFuture<HashMap<AnagramKey, List<String>>> future = CompletableFuture.supplyAsync(() -> {
                try {
                    return new AnagramGrouperParallel(filePath, numThreads, batchSize).getTable();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, executor);
            futures.add(future);
        }

        // Wait for all to complete
        futures.stream()
            .map(CompletableFuture::join)
            .collect(Collectors.toList());

        long endTime = System.nanoTime();
        executor.shutdown();

        double durationMs = (endTime - startTime) / 1_000_000.0;
        System.out.println("Parallel run of " + runs + " tasks took: " + durationMs + " ms");

        // Optionally, do something with results
        // System.out.println("Got " + results.size() + " tables");
    }

    public void SequentialTest(int runs, ThrowingSupplier<AnagramGrouper> factory) throws Exception {
        double totalSync = 0;
        
        for (int i = 0; i < runs; i++) {
            System.gc();
            Thread.sleep(100);
            long startTime = System.nanoTime();
            factory.get().getTable();
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            // System.out.printf("Sync Run %d: %.2f ms\n", i + 1, durationMs);
            totalSync += durationMs;
        }
        double avgSync = totalSync / runs;
        System.out.printf("Sync average execution time: %.2f ms\n", avgSync);
    }

    public void SyncTest(int runs, ThrowingSupplier<AnagramGrouper> factory) throws Exception {
        System.out.println("Running sequential test");
        System.gc();
        Thread.sleep(100);
        
        long startTime = System.nanoTime();
        for (int i = 0; i < runs; i++) {
            factory.get().getTable();
        }
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        System.out.printf("total time: %.2f ms\n", durationMs);
    }

    public void SyncTest(String filePath, int runs) throws Exception {
        SyncTest(runs, ()-> new AnagramGrouperSequential(filePath));
    }

    public void SequentialTest(String filePath, int runs) throws Exception {
        SequentialTest(runs, ()-> new AnagramGrouperSequential(filePath));
    }
}
