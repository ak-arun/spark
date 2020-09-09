import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object GlueApp {
  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession
    import sparkSession.implicits._
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    sparkSession.read.format("json").load("s3://<your bucket>/Hudi/Glue/job1/input/input.json").write.format("org.apache.hudi").option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "id").option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "").option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "seq").option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL).option(HoodieWriteConfig.TABLE_NAME, "d2").mode(SaveMode.Append).save("s3://<your bucket>/Hudi/Glue/job1/job1_output");
    Job.commit()
  }
}