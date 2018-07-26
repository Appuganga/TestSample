package com.producer.kafka
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.rest.client.Client
import org.apache.hadoop.hbase.rest.client.Cluster
import org.apache.log4j._

object HbaseUtil {

  @transient private var connection: org.apache.hadoop.hbase.client.Connection = null

  @transient private var client: Client = null;
  val logger = Logger.getLogger(HbaseUtil.getClass)
  def insertIntoHbase(tableName: String, put: Put) = {
    val connection = getHbaseConnection();
    val table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)))
    //logger.info("Got table Object.." + table.getName)
    table.put(put)
    //logger.info("Completed inserting Record into Hbase..");
    table.close()
  }

  def getHbaseConnection(): org.apache.hadoop.hbase.client.Connection = {
    if (connection == null || connection.isClosed()) {
      synchronized {
        //logger.info("creating Hbase Connection...")
        val conf: Configuration = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", PropertyHandler.getValue("zookeeper.quorum"));
        /*        var hbaseSiteXml = PropertyHandler.getValue("hbase.site")
        if (hbaseSiteXml != null & (!hbaseSiteXml.trim().isEmpty())) {
          conf.addResource(hbaseSiteXml)
        }
*/ connection = ConnectionFactory.createConnection(conf)
      }
    }
    connection
  }

  /**
   * closing alive connection
   */
  def closeHbaseConnection() = {
    if (connection != null && connection.isClosed().!=(true)) {
      //logger.info("closing Hbase Connection...")
      connection.close()
      connection = null
    }
  }

  def getHbaseRestCOnnection(): Client = {

    if (client == null) {
      logger.info("creating new rest connection..")
      val host = PropertyHandler.getValue("hbase.rest.host");
      val port = PropertyHandler.getValue("hbase.rest.port");
      var cluster = new Cluster();
      cluster.add(host, port.toInt)
      client = new Client(cluster);
    }else{
      logger.info("using old rest connection..")
    }
    client
  }
}