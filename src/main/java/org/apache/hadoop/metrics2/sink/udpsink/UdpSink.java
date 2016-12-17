package org.apache.hadoop.metrics2.sink.udpsink;

import org.apache.avro.data.Json;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.net.DNS;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.*;
import java.net.*;

/**
 * Created by yubangxu on 2016/12/16.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class UdpSink implements MetricsSink, Closeable {

    private static final Log LOG = LogFactory.getLog(UdpSink.class);
    private SubsetConfiguration conf ;
    private String clusterName ;
    private String udpAddr ;
    private int udpPort ;
    private String hostName ;

    private static final String FILENAME_KEY = "filename";
    private PrintStream writer;

    public void close() throws IOException {
        writer.close();
    }

    public void putMetrics(MetricsRecord metricsRecord) {
//        for (MetricsTag tags : metricsRecord.tags()){
//
//        }

//        JSONObject jsonObj = new JSONObject();
//        JSONObject tagObj  = new JSONObject();
//        JSONObject valueObj = new JSONObject();
//        try {
//            jsonObj.put("time",String.valueOf(metricsRecord.timestamp()));
//            jsonObj.put("name",metricsRecord.name());
//            jsonObj.put("hostname",getHostName());
//            for (MetricsTag tag : metricsRecord.tags()) {
//                tagObj.put(tag.name(),tag.value());
//            }
//            for (AbstractMetric metric : metricsRecord.metrics()) {
//                valueObj.put(metric.name(),metric.value());
//            }
//            jsonObj.put("tag",tagObj);
//            jsonObj.put("metrics",valueObj);
//
//
//            writer.print(jsonObj.toString());
//            //sendUdp(jsonObj.toString());
//        } catch (JSONException e) {
//            e.printStackTrace();
//        }
        writer.print(this.clusterName);
    }

    public void flush() {
        writer.flush();
    }

    public void init(SubsetConfiguration subsetConfiguration) {

        String filename = conf.getString(FILENAME_KEY);
        try {
            writer = (filename == null) ? System.out
                    : new PrintStream(new FileOutputStream(new File(filename)),
                    true, "UTF-8");
        } catch (Exception e) {
            throw new MetricsException("Error creating "+ filename, e);
        }

        LOG.info("udpsink");
        this.conf = subsetConfiguration ;

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
        try {
            parseConfiguration();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void parseConfiguration() {
        this.clusterName = conf.getString("cluster") ;
        this.udpAddr = conf.getString("udpaddr") ;
        this.udpPort = conf.getInt("udpport") ;
    }

    public String getHostName() {
        return this.hostName;
    }

    private void sendUdp(String msg) throws IOException {
        DatagramSocket ds  = new DatagramSocket();
        byte[] buf = msg.getBytes();
        DatagramPacket dp = new DatagramPacket(buf,buf.length, InetAddress.getByName(this.udpAddr),this.udpPort);//10000为定义的端口
        ds.send(dp);
        ds.close();
    }
}
