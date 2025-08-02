import java.util.HashMap;
import java.util.List;

public abstract class AnagramGrouper {
    public final String filePath;

    public AnagramGrouper(String filePath)
    {
        this.filePath = filePath;
    }

    public abstract HashMap<AnagramKey, List<String>> getTable() throws Exception;
    
    public abstract void writeTableToFile(HashMap<AnagramKey, List<String>> table, String filePath) throws Exception;

}
