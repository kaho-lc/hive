package com.atguigu.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author lc
 * @create 2021-05-22-14:37
 */
public class MyUdf extends GenericUDF {
    /**ObjectInspector:类型的校验器
     * 校验数据的参数个数
     * @param objectInspectors
     * @return
     * @throws UDFArgumentException
     */
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1){
            throw new UDFArgumentException("参数个数不为1");
        }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 处理数据的方法
     * @param deferredObjects
     * @return
     * @throws HiveException
     */
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        //1.取出输入的数据
        String input = deferredObjects[0].get().toString();

        //2.判断输入数据是否为null
        if (input == null){
            return 0;
        }

        //3.返回输入数据的长度
        return input.length();
    }

    /**
     * 执行计划，一般不作处理
     * @param strings
     * @return
     */
    public String getDisplayString(String[] strings) {
        return null;
    }
}
