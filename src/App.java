import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.ArrayList;

public class App {
    public static void main(String[] args) throws Exception {
        
        //each entry represents the length of the word
        //and the value is a list of words of that length
        //for example, table.get("abc") will return a list of words that are anagrams of "abc"
        @SuppressWarnings("unchecked")
        Hashtable<String, List<String>>[] tableArray = new Hashtable[10];
        for (int i = 0; i < tableArray.length; i++) {
            tableArray[i] = new Hashtable<>();
        }

        try (BufferedReader reader = new BufferedReader(new FileReader("sample.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                char[] chars = line.toCharArray();
                Arrays.sort(chars);
                Hashtable<String, List<String>> table = tableArray[chars.length];
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
        for (Hashtable<String, List<String>> table : tableArray) {
            for (var entry : table.entrySet()) {
                System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
            }
        }
    }
}
