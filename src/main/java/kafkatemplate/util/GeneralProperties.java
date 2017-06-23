package kafkatemplate.util;

import java.util.Properties;

/**
 * Класс-обертка над Properties для доступа к свойствам любого типа.
 */
public class GeneralProperties {

    private final Properties props;

    /**
     * Конструктор
     * @param propertiesFilePath путь к файлу свойств
     */
    public GeneralProperties(String propertiesFilePath) {
        this.props = ConfigUtil.initProperties(propertiesFilePath, false);
    }

    /**
     * Конструктор
     * @param propertiesFilePath путь к файлу свойств
     * @param unicode интерпретировать ли строки в кодировке UTF-8
     */
    public GeneralProperties(String propertiesFilePath, boolean unicode) {
        this.props = ConfigUtil.initProperties(propertiesFilePath, unicode);
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
