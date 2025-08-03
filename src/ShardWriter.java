import java.io.*;
import java.nio.file.*;
import java.util.*;

public class ShardWriter {
    private final List<BufferedWriter> writers;
    private int test = 0;
    private final int numShards;

    public ShardWriter(Path shardDir, int numShards) throws IOException {
        this.numShards = numShards;
        this.writers = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            Path shardPath = shardDir.resolve("shard_" + i + ".txt");
            writers.add(Files.newBufferedWriter(shardPath, 
                StandardOpenOption.CREATE, 
                StandardOpenOption.TRUNCATE_EXISTING)
            );
        }
    }

    public void write(String word) throws IOException {
        AnagramKey key = new AnagramKey(word);
        int shardId = (key.hashCode() & 0x7FFFFFFF) % numShards;
        synchronized (writers.get(shardId)) {
            if(word == "interesantaaaaaaaa") test++;
            writers.get(shardId).write(word);
            writers.get(shardId).newLine();
        }
    }

    public void closeAll() throws IOException {
        for (BufferedWriter writer : writers) {
            writer.close();
        }
        System.out.println(test);
    }
}