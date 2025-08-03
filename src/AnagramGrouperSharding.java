import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class AnagramGrouperSharding extends AnagramGrouper{
    private final String filePath;
    private final int numThreads;
    private final int batchSize;
    private final int numShards;
    private final Path shardDir;
    private final Path groupedNamesDir;
    private final String shardDirPath;
    private final int outputMapCount;


    public AnagramGrouperSharding(
        String filePath, int numThreads, int batchSize, int numShards, 
        String shardDirPath, int outputMapCount
        ) throws IOException {
        super(filePath);
        this.filePath = filePath;
        this.numThreads = numThreads;
        this.batchSize = batchSize;
        this.numShards = numShards;
        this.shardDir = Paths.get(shardDirPath);
        this.shardDirPath = shardDirPath;
        this.outputMapCount = outputMapCount;
        this.groupedNamesDir = Paths.get("GroupedFiles");
        Files.createDirectories(shardDir);
        Files.createDirectories(groupedNamesDir);
    }

    public HashMap<AnagramKey, List<String>> getTable() throws Exception {
        BlockingQueue<List<String>> queue = new LinkedBlockingQueue<>(100);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<BufferedWriter> shardWriters = createShardWriters();

        // Poison pill
        List<String> POISON_PILL = new ArrayList<>();

        // Launch consumers
        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    while (true) {
                        // System.out.println(Thread.currentThread().getName());
                        
                        List<String> batch = queue.take();
                        if (batch == POISON_PILL) break;

                        for (String word : batch) {
                            AnagramKey key = new AnagramKey(word);
                            int shardId = (key.hashCode() & 0x7FFFFFFF) % numShards;

                            synchronized (shardWriters.get(shardId)) {
                                shardWriters.get(shardId).write(word);
                                shardWriters.get(shardId).newLine();
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        // Producer
        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            List<String> batch = new ArrayList<>(batchSize);
            for (Iterator<String> it = lines.iterator(); it.hasNext(); ) {
                batch.add(it.next());
                if (batch.size() == batchSize) {
                    queue.put(new ArrayList<>(batch));
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                queue.put(batch);
            }
        }

        // Poison pills to stop threads
        for (int i = 0; i < numThreads; i++) {
            queue.put(POISON_PILL);
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        // Close writers
        for (BufferedWriter writer : shardWriters) {
            writer.close();
        }
        divideShardsProcessing(numShards, numThreads);
        return null;
    }

    private void divideShardsProcessing(int numFiles, int numThreads) throws IOException
    {
        List<Path> files = new ArrayList<>();
        int numOfUsedFiles = 0;
        for (int i = 0; i < numFiles; i++) {
            Path path = shardDir.resolve("shard_" + i + ".txt");
            if(Files.size(path) == 0) continue;
            files.add(path);
            numOfUsedFiles++;
        }
        numFiles = numOfUsedFiles;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        int baseSize = numFiles / numThreads;
        int remainder = numFiles % numThreads;

        int start = 0;
        for (int i = 0; i < numThreads; i++) {
            int taskSize = baseSize + (i < remainder ? 1 : 0);
            int end = start + taskSize;

            List<Path> subList = files.subList(start, end);
            //using {} to avoid handling the exception
            final int threadIndex = i; 
            executor.submit(() -> {
                Path threadDir = groupedNamesDir.resolve("thread_" + threadIndex);
                Files.createDirectories(threadDir);
                processFilesInThread(subList, threadDir);
                return null;
            });

            start = end;
        }

        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    private void processFilesInThread(List<Path> files, Path threadDir) throws IOException
    {
        HashMap<AnagramKey,BufferedWriter> groupedFiles = new HashMap<>();
        for (Path file : files) {
            try(BufferedReader reader = Files.newBufferedReader(file) ){
                String line;
                while((line = reader.readLine()) != null) {
                    line = line.trim();
                    AnagramKey key = new AnagramKey(line);
                    BufferedWriter writer;
                    if(groupedFiles.containsKey(key)) {
                        writer = groupedFiles.get(key);
                    }
                    else {
                        //sorting for readability
                        char[] chars = line.toCharArray();
                        Arrays.sort(chars);
                        Path writerPath = threadDir.resolve( new String(chars) + ".txt" );
                        writer = Files.newBufferedWriter(
                            writerPath,
                            StandardOpenOption.CREATE, 
                            StandardOpenOption.APPEND
                        );
                        groupedFiles.put(key, writer);
                    }
                    writer.write(line);
                    writer.newLine();
                }
            }
        }
        for (BufferedWriter writer : groupedFiles.values()) {
            writer.close();
        }
    } 

    int betterABS(int i)
    {
        //math.abs returns negaive if the input is integer.minValue
        //this version will return 0 (a positive/non-negative)
        return (i & 0x7FFFFFFF);
    }

    private List<BufferedWriter> createShardWriters() throws IOException {
        List<BufferedWriter> writers = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            Path shardPath = shardDir.resolve("shard_" + i + ".txt");
            writers.add(Files.newBufferedWriter(shardPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING));
        }
        return writers;
    }

    @Override
    public void writeTableToFile(HashMap<AnagramKey, List<String>> table, String filePath) throws Exception {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePath))) {
            for (List<String> group : table.values()) {
                writer.write(String.join(" ", group));
                System.out.println("Writing group: " + group);
                writer.newLine();
            }
        }
    }

    public void writeTableToFile(String filePath) throws Exception {
        String shardPathPrefix = shardDirPath + "/shard_";
        int wordCount = 0;
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePath))) {
            for(int i = 0; i < numShards; i++) {
                Path shardPath = Paths.get(shardPathPrefix + i);
                if (!Files.exists(shardPath) || Files.size(shardPath) == 0) continue;

                try(BufferedReader reader = Files.newBufferedReader(shardPath)){
                    String line;
                    while((line = reader.readLine()) != null)
                    {
                        writer.write(line);
                        writer.newLine();
                        if(wordCount++ >= outputMapCount) return;
                    }
                }  
            }
        }
        
    }
}
