package org.apache.hadoop.metrics2.sink.udpsink;

import alps_Hadoop.demo.HMetrics;
import alps_Hadoop.demo.ThriftClient;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yubangxu on 2016/12/19.
 */
public class UdpSinkTest {

    public static void main(String[] args) {
        System.out.println("Hello Thrift");
        ThriftClient thriftClient = ThriftClient.getInstance("10.75.136.105",10020);
        HMetrics hMetrics = new HMetrics();
        hMetrics.setTime((int)(1300000000));
        hMetrics.setHostname("test");
        hMetrics.setName("testname");
        Map<String,String> tagMap = new HashMap<String, String>();
        tagMap.put("k1", "v1");
        Map<String,Double> metricsMap = new HashMap<String, Double>();
        hMetrics.setTags(tagMap);
        metricsMap.put("m1", (double)(0.11));
        hMetrics.setMetrics(metricsMap);
        try {
            thriftClient.Write(hMetrics);
            thriftClient.Flush();
        } catch (TException e) {
            e.printStackTrace();
        }
        System.out.println(thriftClient.toString());
    }
}
