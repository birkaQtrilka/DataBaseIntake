import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class GroupedWriter {

    public static void writeMapToFile(Map<AnagramKey, List<String>> table, Path outputPath) throws IOException {
        if(table == null) return;
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            for (List<String> group : table.values()) {
                writer.write(String.join(" ", group));
                writer.newLine();
                System.out.println("Writing group: " + group);
            }
        }
    }
    
    public static void writeFromOutput(Path shardDir, int outputLimit, Path outputFile) throws IOException {
        int wordCount = 0;
        try (BufferedWriter writer = Files.newBufferedWriter(outputFile)) 
        {
            try (DirectoryStream<Path> files = Files.newDirectoryStream(shardDir)) 
            {
                for (Path file : files) 
                {
                    try (BufferedReader reader = Files.newBufferedReader(file)) 
                    {
                        String line;
                        while ((line = reader.readLine()) != null) 
                        {
                            writer.write(line);
                            writer.newLine();
                            if (++wordCount >= outputLimit) return;
                        }
                    }                        
                }
            }
        }
    }

}