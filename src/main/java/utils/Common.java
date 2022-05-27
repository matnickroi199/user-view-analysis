package utils;

public class Common {
    public static long LongStr(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ignored) {
        }
        return 0;
    }

    public static int IntStr(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ignored) {
        }
        return 0;
    }
}
