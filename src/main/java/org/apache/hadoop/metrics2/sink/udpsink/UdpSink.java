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
//    private ThriftClient thriftClient;
//    private SubsetConfiguration configuration;
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
            //configuration = conf;
            ip = conf.getString("ipaddr");
            port = conf.getInt("port");
            connection(ip,port);
        } catch (Exception e) {
            throw new MetricsException("Error creating "+ filename, e);
        }
    }

    private void connection(String ip,int port)   throws TTransportException {
        LOG.info("Hello connnetction ip ,port" + ip);
        transport = new TFramedTransport(new TSocket(ip, port));
        protocol = new TBinaryProtocol(transport);
        client = new hmetricsThrift.Client(protocol);
        transport.open();
    }

    private hmetricsThrift.Client getClient(String name) throws TTransportException {

        if (clientPool.containsKey(name)){
            return clientPool.get(name);
        }
        TTransport transport = new TFramedTransport(new TSocket(ip, port));
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        hmetricsThrift.Client client = new hmetricsThrift.Client(protocol);
        transport.open();
        clientPool.put(name,client);
        return client;
    }

    @Override
    public void putMetrics(MetricsRecord record) {
        LOG.debug(this.toString());
        LOG.info("Hello ALPS MONITOR" + String.valueOf(record.name()) + record.context());
        HMetrics hMetrics = new HMetrics();
        hMetrics.setTime((int) (record.timestamp()));
        hMetrics.setHostname(hostName);
        hMetrics.setName(record.name());
        Map<String, String> tagMap = new HashMap<String, String>();
        for (MetricsTag tag : record.tags()) {
            tagMap.put(tag.name(), tag.value());
        }
        Map<String, Double> metricsMap = new HashMap<String, Double>();
        hMetrics.setTags(tagMap);
        for (AbstractMetric metric : record.metrics()) {
            metricsMap.put(metric.name(), metric.value().doubleValue());
        }
        hMetrics.setMetrics(metricsMap);
        LOG.info(hMetrics.toString());

        try {
            LOG.info(getClient(record.name()));
            getClient(record.name()).put(hMetrics);
        } catch (TException e) {
            e.printStackTrace();
        }
//        try {
//
//
//        } catch (TException e) {
//            LOG.error(hMetrics.toString() + "ERROR");
//            LOG.error(e.getMessage() + "Error msg");
//
////            try {
////                connection(ip, port);
////            } catch (TTransportException e1) {
////                e1.printStackTrace();
////            }
//            //e.printStackTrace();
//        }
        LOG.info("Hello ALPS MONITOR  End ====");

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
    public void close()


    {
        //writer.close();
//        thriftClient.Close();
    }
}
