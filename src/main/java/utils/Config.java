package utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class Config {
    private static volatile Properties properties = null;

    public static Properties getProperties() {
        if (properties == null) {
            synchronized (Config.class) {
                if (properties == null) {
                    try {
                        InputStream stream = Files.newInputStream(Paths.get("resources/configs.conf"));
                        properties = new Properties();
                        properties.load(stream);
                        stream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return properties;
    }

}
