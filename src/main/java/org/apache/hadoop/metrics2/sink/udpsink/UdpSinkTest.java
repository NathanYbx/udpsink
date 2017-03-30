package org.apache.hadoop.metrics2.sink.udpsink;

import alps_Hadoop.demo.HMetrics;
import alps_Hadoop.demo.ThriftClient;
import alps_Hadoop.demo.hmetricsThrift;
import org.apache.curator.RetrySleeper;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.util.ajax.JSON;


import java.sql.Time;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yubangxu on 2016/12/19.
 */
public class UdpSinkTest {

    public static void main(String[] args) throws TException, InterruptedException {


        System.out.println("Hello Thrift");
        ThriftClient thriftClient = ThriftClient.getInstance("10.75.136.105",10020);
//        long timea = 30000 ;
//        Thread.currentThread().sleep(timea);
        for (int i=0; i<1000 ; i++ ){
            HMetrics hMetrics = new HMetrics();
            hMetrics.setName("JvmMetrics");
            Map<String, String> tagMap = new HashMap<String, String>();
            tagMap.put("SessionId","null");
            Map<String, Double> metricsMap = new HashMap<String, Double>();
            for (int j=0;j<100;j++) {
                metricsMap.put("GcTimeMillisPS MarkSweep"+ String.valueOf(j), 0.000000000000000001);
                //metricsMap.put("GcTimeMillisPS MarkSweep"+ String.valueOf(j), 0.0);
            }

            hMetrics.setTags(tagMap);
            hMetrics.setMetrics(metricsMap);
            System.out.println(hMetrics.toString());
            thriftClient.Write(hMetrics);
        }

    }
}
