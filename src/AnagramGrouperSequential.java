
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

public class AnagramGrouperSequential extends AnagramGrouper{

    public AnagramGrouperSequential(String filePath) {
        super(filePath);
    }
    
    @Override
    public HashMap<AnagramKey, List<String>> getTable() throws Exception {
        HashMap<AnagramKey, List<String>> table = new HashMap<>();
        try (Stream<String> lines = Files.lines(Paths.get(filePath)) ) {
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
