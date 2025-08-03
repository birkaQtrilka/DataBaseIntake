import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

public class App {
    public static void main(String[] args) throws Exception {
        
        // var t = new TestKit();
        // t.Test2();
        // CompareShardsContent();        
        String filePath = "sample.txt";
        AnagramGrouper grouper = new AnagramGrouperSequential(filePath);
        HashMap<AnagramKey, List<String>> table = grouper.getTable();
        grouper.writeTableToFile(table, "output.txt");
        System.out.println("Press Enter to exit...");
        System.in.read(); 

    }

    private static void CompareShardsContent() throws Exception
    {
        Path p = Paths.get("GroupedFiles");
        int total = 0;
        try (DirectoryStream<Path> threadDirectories = Files.newDirectoryStream(p)) {
            for (Path threadDir : threadDirectories) {
                try (DirectoryStream<Path> files = Files.newDirectoryStream(threadDir)) {
                    for (Path file : files) {
                        try (Stream<String> lines = Files.lines(file)) {
                            total += lines.count();
                        }
                    }
                }
            }
        }
        p = Paths.get("anagrams_1_000_000_000.txt");
        long orig = 0;
        try (Stream<String> lines = Files.lines(p)) {
            orig = lines.count();
        }
        System.out.println(total + " comparing to original: " + orig);
    }
}
