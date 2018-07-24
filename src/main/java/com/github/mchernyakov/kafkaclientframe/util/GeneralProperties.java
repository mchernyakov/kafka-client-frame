package com.github.mchernyakov.kafkaclientframe.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Properties-wrapper with access for any type of props
 */
public final class GeneralProperties {

    private final Properties props;

    /**
     * Constructor
     *
     * @param propertiesFilePath file path
     */
    public GeneralProperties(String propertiesFilePath) {
        try {
            this.props = initProperties(propertiesFilePath, false);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Constructor
     *
     * @param propertiesFilePath file path
     * @param unicode            is UTF-8
     */
    public GeneralProperties(String propertiesFilePath, boolean unicode) {
        try {
            this.props = initProperties(propertiesFilePath, unicode);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private Properties initProperties(String file, boolean unicode) throws IOException {
        InputStream in = GeneralProperties.class.getClassLoader().getResourceAsStream(file);
        Properties prop = new Properties();

        if (in != null) {
            if (unicode) {
                prop.load(new InputStreamReader(in, "UTF-8"));
            } else {
                prop.load(in);
            }
        } else {
            throw new FileNotFoundException(file);
        }

        return prop;
    }

    public String getPropertyAsString(String key) {
        return props.getProperty(key);
    }

    public Integer getPropertyAsInt(String key) {
        return Integer.valueOf(props.getProperty(key));
    }

    public Long getPropertyAsLong(String key) {
        return Long.valueOf(props.getProperty(key));
    }

    public Boolean getPropertyAsBool(String key) {
        return Boolean.valueOf(props.getProperty(key));
    }

    public Double getPropertyAsDouble(String key) {
        return Double.valueOf(props.getProperty(key));
    }

    public Properties getProps() {
        return props;
    }
}
