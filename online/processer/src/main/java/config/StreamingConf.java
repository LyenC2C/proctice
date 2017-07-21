package config;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class StreamingConf implements Serializable {
    private Logger logger = LoggerFactory.getLogger(StreamingConf.class);

    public static final long DEFAULT_WINDOW = 1000l * 60;
    public static final long DEFAULT_SLIDE = 1000l * 60;
    public static final long DEFAULT_DURATION = 1000l * 10;

    private Properties prop = new Properties();

    private String name;
    private Long duration;
    private String master;

    public void loadConfig(String configPath) {

        assert StringUtils.isNotBlank(configPath);

        InputStreamReader in = null;
        try {
            logger.info("load config file：" + new File(configPath).getAbsolutePath());
            in = new InputStreamReader(new FileInputStream(configPath), "utf-8");
            prop.load(in);

            name = getStringKey("name", "jstream");
            duration = getLongKey("duration", DEFAULT_DURATION);
            master = getStringKey("master", "local[*]");

        } catch (Exception e) {
            logger.error("error load config file!", e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (Exception e) {

                }
            }
        }
    }

    public String getStringKey(String key, String defaultV) {

        String res = prop.getProperty(key, defaultV);
        logger.info("read config[" + key + "]:" + res);
        return res;
    }

    public Long getLongKey(String key, Long defaultV) {

        Long res = defaultV;
        String val = prop.getProperty(key, getString(defaultV));
        try {
            res = Long.parseLong(val);
        } catch (Exception e) {
        }
        logger.info("read config[" + key + "]:" + res);
        return res;
    }

    public Integer getIntegerKey(String key, Integer defaultV) {

        Integer res = defaultV;
        String val = prop.getProperty(key, getString(defaultV));
        try {
            res = Integer.parseInt(val);
        } catch (Exception e) {
        }
        logger.info("read config[" + key + "]:" + res);
        return res;
    }

    private String getString(Object o) {
        return o == null ? null : o.toString();
    }

    public String getName() {
        return name;
    }

    public Long getDuration() {
        return duration;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }


}
