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
        if (args.length > 0) {
            String runMode = args[0];
            if(runMode.equalsIgnoreCase("sequential"))
            {
                runDefault();
            } else if(runMode.equalsIgnoreCase("sharding")) {
                runSharding();
            }
            else {
                runDefault();
            }
        }
        else
            runDefault();
    }

    private static void runDefault() throws Exception {
        System.out.println("Running in sequential mode...");
        String filePath = "sample.txt";
        AnagramGrouper grouper = new AnagramGrouperSequential(filePath);
        HashMap<AnagramKey, List<String>> table = grouper.getTable();
        GroupedWriter.writeMapToFile(table, Paths.get("output.txt"));
        System.out.println("DONE! Press Enter to exit...");
        System.in.read(); 
    }

    private static void runSharding() throws Exception {
        System.out.println("Running in sharding mode...");

        String filePath = "anagrams_1_000_000_000.txt";
        new AnagramGrouperSharding(
            filePath,
            16, 
            100_000, 
            1000, 
            "Shards", 
            200
        ).getTable();
        System.out.println("result is in the folder GroupedFiles/FinalOutput");
        System.out.println("DONE! Press Enter to exit...");
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
