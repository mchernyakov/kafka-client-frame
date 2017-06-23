package kafkatemplate.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;


public class ConfigUtil {

    private ConfigUtil() {
    }

    /**
     * Загрузка файла свойств
     *
     * @param file файл свойств
     * @return properties
     */
    public static Properties initProperties(String file, boolean unicode) throws RuntimeException {
        try {
            return load(file, unicode);
        } catch (IOException e) {
            throw new IllegalArgumentException(e.toString());
        }
    }

    private static Properties load(String file, boolean unicode) throws IOException {

        InputStream in = ConfigUtil.class.getClassLoader().getResourceAsStream(file);
        Properties prop = new Properties();

        if (in != null) {
            if (unicode)
                prop.load(new InputStreamReader(in, "UTF-8"));
            else
                prop.load(in);
        } else {
            throw new FileNotFoundException(file);
        }

        return prop;
    }

}
