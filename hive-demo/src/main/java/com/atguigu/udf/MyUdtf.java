package com.atguigu.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lc
 * @create 2021-05-22-15:28
 */
public class MyUdtf extends GenericUDTF {

    /**
     * 输入数据：hello ， atguigu ， hive
     * 输出数据：
     *          hello
     *          atguigu
     *          hive
     */

    /**
     * 自定义初始化方法
     * @param argOIs
     * @return
     * @throws UDFArgumentException
     */
    //输出数据的集合
    private ArrayList<String> outPutList = new ArrayList<>();


    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        //输出数据的默认列名，可以被别名覆盖
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("word");

        //输出数据的类型
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        //最终的返回值
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames , fieldOIs);
    }

    /**
     * 处理输入数据的方法
     * @param objects
     * @throws HiveException
     */
    @Override
    public void process(Object[] objects) throws HiveException {
        //1.取出输入数据
        String input = objects[0].toString();

        //2.按照逗号分割
        String[] words = input.split(",");

        // 3.遍历数据写出
        for (String word : words) {
            //清空集合
            outPutList.clear();

            //将数据放入集合
            outPutList.add(word);

            //输出集合
            forward(outPutList);
        }
    }

    /**
     * 收尾方法
     * @throws HiveException
     */
    @Override
    public void close() throws HiveException {

    }
}
