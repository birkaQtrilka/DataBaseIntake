import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class GroupedWriter {

    public static void writeMapToFile(Map<AnagramKey, List<String>> table, Path outputPath) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            for (List<String> group : table.values()) {
                writer.write(String.join(" ", group));
                writer.newLine();
            }
        }
    }

    public static void writeFromShards(Path shardDir, int numShards, int outputLimit, Path outputFile) throws IOException {
        int wordCount = 0;
        try (BufferedWriter writer = Files.newBufferedWriter(outputFile)) {
            for (int i = 0; i < numShards; i++) {
                Path shard = shardDir.resolve("shard_" + i + ".txt");
                if (!Files.exists(shard) || Files.size(shard) == 0) continue;

                try (BufferedReader reader = Files.newBufferedReader(shard)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        writer.write(line);
                        writer.newLine();
                        if (++wordCount >= outputLimit) return;
                    }
                }
            }
        }
    }
}