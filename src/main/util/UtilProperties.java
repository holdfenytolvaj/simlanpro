package util;

import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.Properties;

public class UtilProperties {
    private static Properties prop;

    static {
        prop = new Properties();
        //--- load defaults ------------------------------
        try {
            prop.load(new InputStreamReader(UtilProperties.class.getResourceAsStream("/config/default.properties")));
        } catch (Exception e) {
            throw new RuntimeException("default.properties has not been found!");
        }
        //--- load overwrites ----------------------------
        try {
            File f = new File("config.properties");
            if (f.exists()) {
                prop.load(new FileReader("config.properties"));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String get(String property) {
        return prop.getProperty(property);
    }
}
