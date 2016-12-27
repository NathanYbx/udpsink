

###安装方法
    > 将bin目录下的 
        【1】libthrift-0.9.3.jar
        【2】hadoop-metrics-udpsink.jar
      放置在hadoop 安装目录下的/share/hadoop/common/lib 中
###配置说明
    在hadoop 安装目录下的 etc/hadoop中编辑 hadoop-metrics2.properties
    在其中增加
    
    *.sink.udpsink.class=org.apache.hadoop.metrics2.sink.udpsink.UdpSink
    
    nodemanager.sink.udpsink.process=nodemanager
    nodemanager.sink.udpsink.ipaddr=10.75.136.105
    nodemanager.sink.udpsink.port=10020
    
    datanode.sink.udpsink.process=datanode
    datanode.sink.udpsink.ipaddr=10.75.136.105
    datanode.sink.udpsink.port=10020
    
    *.period=10
    
#其中ip 与 端口为 ALPS_Monitor的RPC端口

  