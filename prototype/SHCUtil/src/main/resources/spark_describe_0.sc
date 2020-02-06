
import ru.homecredit.smartdata.migration.Util
import ru.homecredit.smartdata.migration.HbaseUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.{types => T}
import org.apache.spark.sql.expressions.{Windows => W}

val strOut = "/tables/meta"
val args = sc.getConf.get ("spark.yarn.appMasterEnv.migrationParams")
val lines = Util.loadConfig (Util.takeArgElem (args, "config"))
val params = Util.parseParams (line)
val hbaseConfiguration = HBaseConfiguration.create ()
hbaseConfiguration.addResource (new Path ("/etc/hbase/conf/hbase-site.xml"))
hbaseConfiguration.set ("hbase.client.scanner.timeout.period", "1800000")

val sq = new SQLContext (sc)
val connection = HbaseUtil.describe (sc, hbaseConfiguration, connection, "services.data", 1)

val rdd = sc.makeRDD (res.map (pair => Row (pair._1, pair._2)))
val keys = List ("col_family", "col_name")
val schema = T.StructType (keys.map (k => T.StructField (k, T.StringType, nullable = true)))
val df = sq.createDataFrame (rdd, schema)
df.show (truncate=false)

sc.stop ()
