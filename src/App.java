import java.util.HashMap;
import java.util.List;

public class App {
    public static void main(String[] args) throws Exception {
        String filePath = "sample.txt";
        HashMap<AnagramKey, List<String>> table = AnagramGrouper.getTableSequentially(filePath);
        AnagramGrouper.writeTableToFile(table, "output.txt");
        System.out.println("Press Enter to exit...");
        System.in.read(); 

    }
}
