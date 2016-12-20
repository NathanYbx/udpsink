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
        long timea = 30000 ;
        Thread.currentThread().sleep(timea);
        for (int i=0; i<1000 ; i++ ){
            HMetrics hMetrics = new HMetrics();
            hMetrics.setTime((int) (1300000000));
            hMetrics.setHostname("test");
            hMetrics.setName("testname");
            Map<String, String> tagMap = new HashMap<String, String>();
            tagMap.put("k1", "v1");
            Map<String, Double> metricsMap = new HashMap<String, Double>();
            hMetrics.setTags(tagMap);
            metricsMap.put("m1", (double) (0.11));
            hMetrics.setMetrics(metricsMap);

            thriftClient.Write(hMetrics);
        }

//        TSocket ts = new TSocket("10.75.136.105",10020);
//        TTransport transport = new TFramedTransport(ts);
//        TProtocol protocol = new TBinaryProtocol(transport);
//        hmetricsThrift.Client client = new hmetricsThrift.Client(protocol);
//        transport.open();
//        //hmetricsThrift.AsyncClient client1 = new hmetricsThrift.AsyncClient();
//        for (int i=0;i<1000;i++) {
//
//
//            System.out.println(client);
//            HMetrics hMetrics = new HMetrics();
//            hMetrics.setTime((int) (1300000000));
//            hMetrics.setHostname("test");
//            hMetrics.setName("testname");
//            Map<String, String> tagMap = new HashMap<String, String>();
//            tagMap.put("k1", "v1");
//            Map<String, Double> metricsMap = new HashMap<String, Double>();
//            hMetrics.setTags(tagMap);
//            metricsMap.put("m1", (double) (0.11));
//            hMetrics.setMetrics(metricsMap);
//            try {
//
//                client.put(hMetrics);
//
//                //
//            } catch (TException e) {
//                System.out.println(e.getMessage());
//                e.printStackTrace();
//                transport.close();
//                //break;
//            }
//            //System.out.println(thriftClient.toString());
//        }
        //transport.close();
    }
}
