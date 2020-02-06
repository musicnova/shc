
import io.github.basovyuriy.migration.Util
import io.github.basovyuriy.migration.HbaseUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{functions => F}

val strOut = "/tables/data"
val args = sc.getConf.get ("spark.yarn.appMasterEnv.migrationParams")
val lines = Util.loadConfig (Util.takeArgElem (args, "config"))
val params = Util.parseParams (lines)
val hbaseConfiguration = HBaseConfiguration.create ()
hbaseConfiguration.addResource (new Path ("/etc/hbase/conf/hbase-site.xml"))
hbaseConfiguration.set ("hbase.client.scanner.timeout.period", "18000000")

val connectionData = HbaseUtil.open (hbaseConfiguration)
val qualifiersData = List ( ("main", "personal_id"), ("ident", "phone"))
val cursorsData = HbaseUtil.scan (connectionData, "data", 1, qualifiersData)

val sq = new SQLContext (sc)
HbaseUtil.fetch (sc, sq, cursorsData, 10000, qualifiersData, hbaseConfiguration).zipWithIndex.foreach {
  case (dfData, i) => {
    val part = Util.formatPart (params, Util.takeArgElem (args, "datalake"), i)
    print ("dsData_"+part)
    dfData.show (truncate=false)

    val dfDataNew = dfData.selectExpr(
      "main___personal_id AS s_id"
    , "ident___phone as s_phone"
    , "rowkey as s_rowkey").filter(
    F.col("s_id").isNotNull).filter(
    F.col("s_phone").isNotNull)
    print("dfDataNew_"+part)
    dfDataNew.show (truncate=false)

    dfRes = dfDataNew
    print("dfRes_" + part)
    dfRes.write.mode ("overwrite").option ("compression"
      , "snappy").parquet (Util.formatStagingPath (params, Util.takeArgElem (args, "datalake")) + strOut + "/part=" +part)
}
}
connectionData.close ()
sc.stop ()
