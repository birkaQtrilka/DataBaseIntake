import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.*;
import java.util.HashMap;
import java.util.*;
import java.io.IOException;

public class ShardProcessor implements Runnable {
    private final List<Path> shardFiles;
    private final Path outputDir;

    public ShardProcessor(List<Path> shardFiles, Path outputDir) {
        this.shardFiles = shardFiles;
        this.outputDir = outputDir;
    }

    @Override
    public void run() {
        Map<AnagramKey, BufferedWriter> groupedWriters = new HashMap<>();
        try {
            for (Path file : shardFiles) {
                try (BufferedReader reader = Files.newBufferedReader(file)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        final String copy = line; 
                        processShardLine(groupedWriters, copy);
                    }
                }
            }
            for (BufferedWriter writer : groupedWriters.values()) writer.close();
        } catch (IOException e) {
            throw new RuntimeException("Error processing shards", e);
        }
    }

    private void processShardLine(Map<AnagramKey, BufferedWriter> groupedWriters, String line) throws IOException{
        AnagramKey key = new AnagramKey(line);
        BufferedWriter writer = groupedWriters.computeIfAbsent(key, k -> {
            try {
                char[] chars = line.toCharArray();
                Arrays.sort(chars);
                Path outputPath = outputDir.resolve(new String(chars) + ".txt");
                return Files.newBufferedWriter(
                    outputPath, 
                    StandardOpenOption.CREATE, 
                    StandardOpenOption.TRUNCATE_EXISTING
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        writer.write(line);
        writer.newLine();
    } 
}