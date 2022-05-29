package utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Common {
    public static class Arguments {
        public String date;
        public String device;
        
        public Arguments(String[] args) {
            if (args.length > 0) {
                this.date = args[0];
                if (args.length > 1) {
                    this.device = args[1].toLowerCase();
                }
            } else {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                Date current = new Date();
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(current);
                calendar.add(Calendar.DATE, -1);
                this.date = simpleDateFormat.format(calendar.getTime());
            }
        }
    }
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
