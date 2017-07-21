package utils;

import com.alibaba.fastjson.JSON;

import java.util.Map;

/**
 * Created by lyen on 17-6-27.
 */
public class JsonUtils {

    public static Map<String, Object> json2Map(String jsonStr) {

        Map<String, Object> map = (Map<String, Object>) JSON.parse(jsonStr);
        return map;
    }
}
