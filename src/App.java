import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Stream;

public class App {
    public static void main(String[] args) throws Exception {
        int runs = 50;
        double totalMs = 0;
        HashMap<AnagramKey, List<String>> table = null;
        for (int i = 0; i < runs; i++) {
            long startTime = System.nanoTime();
            table = getAnagramTable("anagrams_10000.txt");
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            System.out.printf("Run %d: %.2f ms\n", i + 1, durationMs);
            totalMs += durationMs;
        }
        double avgMs = totalMs / runs;

        // for (var entry : table.entrySet()) {
        //     System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
        // }
        System.out.printf("Average execution time: %.2f ms\n", avgMs);
    }

    public static HashMap<AnagramKey, List<String>> getAnagramTable(String filePath) throws Exception {
        HashMap<AnagramKey, List<String>> table = new HashMap<>();
        try (Stream<String> lines = Files.lines(Paths.get("anagrams_10000.txt"))) {
            lines.forEach(line -> {
                AnagramKey key = new AnagramKey(line);
                if (table.containsKey(key)) {
                    table.get(key).add(line);
                    return;
                } 
                List<String> newList = new ArrayList<>();
                newList.add(line);
                table.put(key, newList);
            });
        }

        return table;
    }
}
