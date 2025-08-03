import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class AnagramGrouperSharding extends AnagramGrouper {
    private final Path filePath;
    private final int numThreads;
    private final int batchSize;
    private final int numShards;
    private final int outputMapCount;
    private final Path shardDir;
    private final Path groupedNamesDir;
    private final String extension = ".txt";

    public AnagramGrouperSharding(
        String filePath,
        int numThreads,
        int batchSize,
        int numShards, 
        String shardDirPath,
        int outputMapCount
        ) throws IOException {
        super(filePath);
        this.filePath = Paths.get(filePath);
        this.numThreads = numThreads;
        this.batchSize = batchSize;
        this.numShards = numShards;
        this.shardDir = Paths.get(shardDirPath);
        this.groupedNamesDir = Paths.get("GroupedFiles");
        this.outputMapCount = outputMapCount;

        Files.createDirectories(shardDir);
        Files.createDirectories(groupedNamesDir);
    }

    public HashMap<AnagramKey, List<String>> getTable() throws Exception {
        BlockingQueue<List<String>> queue = new LinkedBlockingQueue<>(100);
        ShardWriter shardWriter = new ShardWriter(shardDir, numShards);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        // Start consumers
        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    while (true) {
                        List<String> batch = queue.take();
                        if (batch.isEmpty()) break;
                        for (String word : batch) {
                            shardWriter.write(word);
                        }
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }

        // Produce batches
        BatchProducer.produceBatches(filePath, batchSize, queue, numThreads);

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);
        shardWriter.closeAll();

        processShards();
        mergeThreadOutputs(groupedNamesDir, groupedNamesDir.resolve("FinalOutput"));
        return null;
    }

    private void processShards() throws IOException {
        List<Path> nonEmptyShards = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            Path path = shardDir.resolve("shard_" + i + ".txt");
            if (Files.size(path) > 0) nonEmptyShards.add(path);
        }

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        int filesPerThread = (int) Math.ceil((double) nonEmptyShards.size() / numThreads);
        for (int i = 0; i < numThreads; i++) {
            int start = i * filesPerThread;
            int end = Math.min(start + filesPerThread, nonEmptyShards.size());
            if (start >= end) break;
            List<Path> subset = nonEmptyShards.subList(start, end);
            Path outputDir = groupedNamesDir.resolve("thread_" + i);
            Files.createDirectories(outputDir);
            executor.submit(new ShardProcessor(subset, outputDir));
        }

        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void mergeThreadOutputs(Path threadRootDir, Path finalOutputDir) throws IOException {
        finalOutputDir = Files.createDirectories(finalOutputDir);
        Map<String, Integer> nameCounts = new HashMap<>();
        System.out.println("Going through dir: " + threadRootDir.getFileName());

        try (DirectoryStream<Path> threadDirectories = Files.newDirectoryStream(threadRootDir)) {
            for (Path threadDir : threadDirectories) {
                if (!Files.isDirectory(threadDir) || threadDir.getFileName().toString().equals("FinalOutput")) continue; 

                try (DirectoryStream<Path> files = Files.newDirectoryStream(threadDir)) {
                    for (Path file : files) {
                        
                        String newName = ValidateDuplicates(nameCounts, file);
                        
                        Path target = finalOutputDir.resolve(newName);
                        Files.move(file, target, StandardCopyOption.REPLACE_EXISTING);
                    }
                }

                Files.delete(threadDir);
            }
        }
    }

    private String ValidateDuplicates(Map<String, Integer> nameCounts, Path file) 
    {
        String fileName = file.getFileName().toString();
        String baseName = fileName.substring(0, fileName.length() - 4); // removes ".txt"

        int count = nameCounts.getOrDefault(baseName, 0);
        nameCounts.put(baseName, count + 1);

        if(count == 0) return fileName;
        return baseName + "_" + count + extension;
    }

    @Override
    public void writeTableToFile(HashMap<AnagramKey, List<String>> table, String filePath) throws Exception {
        GroupedWriter.writeMapToFile(table, Paths.get(filePath));
    }

    public void writeTableToFile(String filePath) throws Exception {
        GroupedWriter.writeFromShards(shardDir, numShards, outputMapCount, Paths.get(filePath));
    }
}