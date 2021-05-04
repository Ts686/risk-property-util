package com.pingan.property.hiveudf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.pingan.property.utils.JsonUtils;

public class PropertyRiskDetailUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 1) {
            throw new UDFArgumentLengthException(
                    "PropertyRiskDetailUDTF expected  one argument but found " + argOIs.length);
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("json_path");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("simple_json_path");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("json_val");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(
                fieldNames, fieldOIs);
    }


    @Override
    public void process(Object[] args) throws HiveException {
        List<String[]> res = getRiskDetails("$", args[0] == null ? null : args[0].toString());
        for (String[] row : res) {
            forward(row);
        }
    }

    public static List<String[]> getRiskDetails(String rootKey, String jsonStr) {
        ArrayList<String[]> res = new ArrayList<>();
        Map<String, String> jsonKV = JsonUtils.parseJson2Map(rootKey, jsonStr);
        for (Map.Entry<String, String> entry : jsonKV.entrySet()) {
            String jsonPath = entry.getKey();
            String jsonVal = entry.getValue();
            String simpleJsonPath = jsonPath.replaceAll("\\$\\.", "").replaceAll("[*\\[\\]]+", "");
            String[] record = {jsonPath, simpleJsonPath, jsonVal};
            res.add(record);
        }
        return res;
    }

    @Override
    public void close() throws HiveException {

    }

    public static void main(String[] args) throws Exception {
        String json = "{\"name\":\"mkyong\", \"age\":{\"name2\":\"prb\", \"age2\":\"18\"},\"list\":[{\"name3\":\"prb\", \"age3\":\"18\"},{\"name4\":\"lijie\", \"age4\":\"32\"}],\"list2\":[\"hello\",\"world\"]}";
        List<String[]> res = getRiskDetails("$", json);
        System.out.println(res);
    }
}
