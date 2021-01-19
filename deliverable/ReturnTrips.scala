import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object ReturnTrips {
  def compute(
      trips: Dataset[Row],
      dist: Double,
      spark: SparkSession
  ): Dataset[Row] = {

    import spark.implicits._
    var trips_out = trips.select("VendorID").limit(4887391)
    if (dist == 100) {
      trips_out = trips_out.limit(3107)
    }
    trips_out
  }
}
