
import java.util.Arrays;

public class AnagramKey {
    private final int[] count;

    //assumes all characters are lowercase a-z
    public AnagramKey(String line) {
        count = new int[26];
        for (char c : line.toCharArray()) {
            count[c - 'a']++;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnagramKey that = (AnagramKey) o;
        return Arrays.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(count);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count.length; i++) {
            builder.append(count[i]);
        }
        return builder.toString();
    }
}
