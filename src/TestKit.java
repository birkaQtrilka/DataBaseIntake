import java.util.HashMap;
import java.util.List;

public class TestKit {

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
            HashMap<AnagramKey, List<String>> tableParallel  = AnagramGrouper.getTableInParallel(filePath, numThreads, batchSize);
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            totalParallel += durationMs;
        }
        double avgParallel = totalParallel / runs;
        System.out.printf("Parallel average execution time: %.2f ms\n", avgParallel);

    }

    public void SequentialTest(String filePath, int runs) throws Exception {
        double totalSync = 0;
        
        for (int i = 0; i < runs; i++) {
            System.gc();
            Thread.sleep(100);
            long startTime = System.nanoTime();
            HashMap<AnagramKey, List<String>> tableSync = AnagramGrouper.getTableSequentially(filePath);
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            // System.out.printf("Sync Run %d: %.2f ms\n", i + 1, durationMs);
            totalSync += durationMs;
        }
        double avgSync = totalSync / runs;
        System.out.printf("Sync average execution time: %.2f ms\n", avgSync);
    }
}
