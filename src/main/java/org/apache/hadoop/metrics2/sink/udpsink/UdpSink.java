package org.apache.hadoop.metrics2.sink.udpsink;

import alps_Hadoop.demo.HMetrics;
import alps_Hadoop.demo.ThriftClient;
import org.apache.avro.data.Json;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.net.DNS;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yubangxu on 2016/12/16.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class UdpSink implements MetricsSink, Closeable {

    private static final Log LOG = LogFactory.getLog(UdpSink.class);
    private static final String FILENAME_KEY = "filename";
    //private PrintStream writer;
    private ThriftClient thriftClient;
    private String hostName ;

    @Override
    public void init(SubsetConfiguration conf) {
        LOG.info("Hello ALPS Thrift MONITOR");
        hostName = "ALPS_DEFAULT_HOST";
        if (conf.getString("slave.host.name") != null) {
            hostName = conf.getString("slave.host.name");
        } else {
            try {
                hostName = DNS.getDefaultHost(
                        conf.getString("dfs.datanode.dns.interface", "default"),
                        conf.getString("dfs.datanode.dns.nameserver", "default"));
            } catch (UnknownHostException uhe) {
                LOG.error(uhe);
                hostName = "UNKNOWN.example.com";
            }
        }
        String filename = conf.getString(FILENAME_KEY);
        try {
//            writer = filename == null ? System.out
//                    : new PrintStream(new FileOutputStream(new File(filename)),
//                    true, "UTF-8");
            String ip = conf.getString("ipaddr");
            int port = conf.getInt("port");
            thriftClient = ThriftClient.getInstance(ip,port);
            LOG.info(thriftClient.toString());
        } catch (Exception e) {
            throw new MetricsException("Error creating "+ filename, e);
        }
    }

    @Override
    public void putMetrics(MetricsRecord record) {

        LOG.info("Hello ALPS MONITOR" + String.valueOf(record.timestamp()));
        LOG.info(thriftClient.toString() + "==================");
//        JSONObject jsonObj = new JSONObject();
//        JSONObject tagObj  = new JSONObject();
//        JSONObject valueObj = new JSONObject();
//        try {
//            jsonObj.put("time", String.valueOf(record.timestamp()));
//            jsonObj.put("name", record.name());
//            jsonObj.put("hostname", hostName);
//            for (MetricsTag tag : record.tags()) {
//                tagObj.put(tag.name(), tag.value());
//            }
//            for (AbstractMetric metric : record.metrics()) {
//                valueObj.put(metric.name(), metric.value());
//            }
//            jsonObj.put("tag", tagObj);
//            jsonObj.put("metrics", valueObj);
//        }catch (Exception e){
//            LOG.error(e.getMessage());
//        }
//        writer.print(jsonObj.toString());
//        writer.println();

        HMetrics hMetrics = new HMetrics();
        hMetrics.setTime((int)(record.timestamp()));
        hMetrics.setHostname(hostName);
        hMetrics.setName(record.name());
        Map<String,String> tagMap = new HashMap<String, String>();
        for (MetricsTag tag : record.tags()) {
            tagMap.put(tag.name(), tag.value());
        }
        Map<String,Double> metricsMap = new HashMap<String, Double>();
        hMetrics.setTags(tagMap);
        for (AbstractMetric metric : record.metrics()) {
            metricsMap.put(metric.name(), metric.value().doubleValue());
        }
        hMetrics.setMetrics(metricsMap);
        try {
            LOG.info(hMetrics.toString());
            if(hMetrics !=null) {
                if (thriftClient != null ) {
                    LOG.info("Hello ALPS MONITOR 8888888888888888");
                    thriftClient.Write(hMetrics);
                }else {
                    LOG.info(thriftClient);
                }
            }else {
                LOG.info("hmetrics is null");
            }
        } catch (TException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void flush() {
//        writer.flush();
//        try {
//            thriftClient.Flush();
//        } catch (TTransportException e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public void close() throws IOException {
        //writer.close();
//        thriftClient.Close();
    }
}
