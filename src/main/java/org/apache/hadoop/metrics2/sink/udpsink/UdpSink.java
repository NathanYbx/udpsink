package org.apache.hadoop.metrics2.sink.udpsink;

import alps_Hadoop.demo.HMetrics;
import alps_Hadoop.demo.ThriftClient;
import alps_Hadoop.demo.hmetricsThrift;
import org.apache.avro.data.Json;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.net.DNS;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
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
    private SubsetConfiguration configuration;
    private String ip ;
    private int port;
    private String hostName ;

    private TTransport transport;
    private hmetricsThrift.Client client;
    private TBinaryProtocol protocol;

    private Map<String,hmetricsThrift.Client> clientPool;

    @Override
    public void init(SubsetConfiguration conf) {
        LOG.info("Hello ALPS Thrift MONITOR");
        hostName = "ALPS_DEFAULT_HOST";
        clientPool = new HashMap<String, hmetricsThrift.Client>();
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
            configuration = conf;
            ip = conf.getString("ipaddr");
            port = conf.getInt("port");

            thriftClient = ThriftClient.getInstance(ip,port);
        } catch (Exception e) {
            throw new MetricsException("Error creating "+ filename, e);
        }
    }

//    private void connection(String ip,int port)   throws TTransportException {
//        transport = new TFramedTransport(new TSocket(ip, port));
//        protocol = new TBinaryProtocol(transport);
//        client = new hmetricsThrift.Client(protocol);
//        transport.open();
//    }

//    private hmetricsThrift.Client getClient(String name) throws TTransportException {
//
//        if (clientPool.containsKey(name)){
//            return clientPool.get(name);
//        }
//        TTransport transport = new TFramedTransport(new TSocket(ip, port));
//        TBinaryProtocol protocol = new TBinaryProtocol(transport);
//        hmetricsThrift.Client client = new hmetricsThrift.Client(protocol);
//        transport.open();
//        clientPool.put(name,client);
//        return client;
//    }

    @Override
    public void putMetrics(MetricsRecord record) {
        HMetrics hMetrics = new HMetrics();
        hMetrics.setTime(record.timestamp());
        hMetrics.setPrefix(configuration.getString("process"));
        hMetrics.setHostname(hostName);
        hMetrics.setName(record.name());
        Map<String, String> tagMap = new HashMap<String, String>();
        for (MetricsTag tag : record.tags()) {
            if (tag.value() != null) {
                tagMap.put(tag.name(), tag.value());
            }else{
                tagMap.put(tag.name(), "");
            }
        }
        Map<String, Double> metricsMap = new HashMap<String, Double>();
        hMetrics.setTags(tagMap);
        for (AbstractMetric metric : record.metrics()) {
            metricsMap.put(metric.name(), metric.value().doubleValue());
        }
        hMetrics.setMetrics(metricsMap);

        try {
            thriftClient.Write(hMetrics);
        } catch (TException e) {
            thriftClient = ThriftClient.getInstance(ip,port);
            LOG.info(e.getMessage() + "TExecption INFO ======");
            e.printStackTrace();
        }
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
}
