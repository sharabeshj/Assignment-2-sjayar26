vi Sp vimport org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkMapReduce {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runMapReduce(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame = {
    var pointDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(pointPath);
    pointDf = pointDf.toDF()
    pointDf.createOrReplaceTempView("points")

    pointDf = spark.sql("select ST_Point(cast(points._c0 as Decimal(24,20)),cast(points._c1 as Decimal(24,20))) as point from points")
    pointDf.createOrReplaceTempView("pointsDf")
    pointDf.show()

    var rectangleDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(rectanglePath);
    rectangleDf = rectangleDf.toDF()
    rectangleDf.createOrReplaceTempView("rectangles")

    rectangleDf = spark.sql("select ST_PolygonFromEnvelope(cast(rectangles._c0 as Decimal(24,20)),cast(rectangles._c1 as Decimal(24,20)), cast(rectangles._c2 as Decimal(24,20)), cast(rectangles._c3 as Decimal(24,20))) as rectangle from rectangles")
    rectangleDf.createOrReplaceTempView("rectanglesDf")
    rectangleDf.show()

    val joinDf = spark.sql("select rectanglesDf.rectangle as rectangle, pointsDf.point as point from rectanglesDf, pointsDf where ST_Contains(rectanglesDf.rectangle, pointsDf.point)")
    joinDf.createOrReplaceTempView("joinDf")
    joinDf.show()

    import spark.implicits._

    // You need to complete this part
    val joinRdd = joinDf.rdd
    val map = joinRdd.map(row => (row.get(0), 1))
    val reduce = map.repartition(40)
                  .reduceByKey((k,v) => k+v)
    val result = reduce
                  .map(_._2)
                  .sortBy(x => x)
                  .coalesce(1)
                  .toDF()
    return result // You need to change this part
  }

}

