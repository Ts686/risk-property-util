package com.pingan.property.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * Json工具类
 */
public class JsonUtils {
    static final ObjectMapper mapper = new ObjectMapper();

    public static boolean isBadJson(String jsonInString) {
        return !(isGoodJson(jsonInString));
    }


    public static boolean isGoodJson(String jsonInString) {
        try {
            mapper.readTree(jsonInString);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static Map<String, String> parseJson2Map(String rootKey, String jsonStr) {
        HashMap<String, String> res = new HashMap<>();
        String newKey = null;
        try {
            JsonNode jsonNode = mapper.readTree(jsonStr);
            for (Iterator<Entry<String, JsonNode>> fields = jsonNode.fields(); fields.hasNext(); ) {
                Entry<String, JsonNode> entry = fields.next();
                String key = entry.getKey();
                JsonNode val = entry.getValue();
                if (isTextNode(val)) {
                    newKey = rootKey + "." + key;
                    res.put(newKey, val.toString());
                } else if (isArrayNode(val) && val.size() != 0) {
                    newKey = rootKey + "." + key + "[*]";
                    res.put(newKey, val.toString());
                    int i = 0;
                    for (Iterator<JsonNode> arrayNode = val.iterator(); arrayNode.hasNext(); ) {
                        JsonNode next = arrayNode.next();
                        if (isObjNode(next)) {
                            newKey = rootKey + "." + key + "[" + i + "]";
                            res.putAll(parseJson2Map(newKey, next.toString()));
                            i++;
                        }
                    }
                } else if (isObjNode(val)) {
                    newKey = rootKey + "." + key;
                    res.putAll(parseJson2Map(newKey, val.toString()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    public static boolean isTextNode(JsonNode value) {
        return value.isTextual();
    }

    public static boolean isArrayNode(JsonNode value) {
        return value.isArray();
    }

    public static boolean isObjNode(JsonNode value) {
        return value.isObject();
    }
    

}
