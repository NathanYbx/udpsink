package alps_Hadoop.demo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.sink.udpsink.UdpSink;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.omg.PortableServer.THREAD_POLICY_ID;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yubangxu on 2016/12/19.
 */
public class ThriftClient {
    private static final Log LOG = LogFactory.getLog(ThriftClient.class);
    private TTransport transport;
    private hmetricsThrift.Client client;
    private TBinaryProtocol protocol;
    private static String ip = "localhost";
    private static int port = 10020;

    public static class ThriftClientHandle {
        private static ThriftClient instance = new ThriftClient(ip,port);
    }

    private ThriftClient(String ip, int port){
        try {
            System.out.println("Hello transport");
           // new TFramedTransport.Factory(transport)
            //TCompactProtocol protocol = new TCompactProtocol(transport);

            transport = new TFramedTransport(new TSocket(ip, port));
            protocol = new TBinaryProtocol(transport);
            client = new hmetricsThrift.Client(protocol);
            transport.open();



//            Map<String, String> param = new HashMap<String, String>();
//            param.put("name", "qinerg");
//            param.put("passwd", "123456");
//
//            for (int i = 0; i < 1000; i++) {
//                System.out.println(client.funCall(System.currentTimeMillis() , "login", param));
//            }

//            transport.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void Write(HMetrics hMetrics) throws TException {
        if (client != null) {
            System.out.println(client);
            client.put(hMetrics);
        }else{
            LOG.info("Client is Null");
        }
    }

    public void Flush() throws TTransportException {
        //transport.flush();
    }

    public void Close() {
        //transport.close();
    }

    public static ThriftClient getInstance(String sip,int sport) {
        //其实这里写的非常不好
        ip = sip ;
        port = sport;
//        if (ThriftClientHandle.instance.transport.isOpen()) {
//            ThriftClientHandle.instance = new ThriftClient(ip,port);
//        }
        return ThriftClientHandle.instance;
    }
}
