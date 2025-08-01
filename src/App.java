import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.ArrayList;

public class App {
    public static void main(String[] args) throws Exception {
        Hashtable<String, List<String>> table = new Hashtable<>();
        try (BufferedReader reader = new BufferedReader(new FileReader("sample.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                char[] chars = line.toCharArray();
                Arrays.sort(chars);
                String sortedLine = new String(chars);
                if (table.containsKey(sortedLine)) {
                    table.get(sortedLine).add(line);
                } else {
                    List<String> newList = new ArrayList<>();
                    newList.add(line);
                    table.put(sortedLine, newList);
                }
            }
        }

        for (var entry : table.entrySet()) {
            System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
        }
    }
}
