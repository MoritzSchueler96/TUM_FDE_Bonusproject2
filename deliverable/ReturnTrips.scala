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
    val diff_time = 28800
    val diff_dist = dist * 9 / 1000000 // 360° = 2*pi*radius

    val trips_filtered = trips
      .select(
        "pickup_longitude",
        "pickup_latitude",
        "dropoff_longitude",
        "dropoff_latitude",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime"
      )
      .withColumn(
        "tpep_pickup_datetime",
        unix_timestamp($"tpep_pickup_datetime")
      )
      .withColumn(
        "tpep_dropoff_datetime",
        unix_timestamp($"tpep_dropoff_datetime")
      )

    val trips_final = trips_filtered
      .as(alias = "a")
      .withColumn(
        "dropoff_time_bucket",
        floor($"a.tpep_dropoff_datetime" / diff_time)
      )
      .withColumn(
        "pickup_lat_bucket",
        explode(
          array(
            floor($"a.pickup_latitude" / diff_dist) - 1,
            floor($"a.pickup_latitude" / diff_dist),
            floor($"a.pickup_latitude" / diff_dist) + 1
          )
        )
      )
      /*.withColumn(
        "pickup_lon_bucket",
        explode(
          array(
            floor($"a.pickup_longitude" / diff_dist) - 1,
            floor($"a.pickup_longitude" / diff_dist),
            floor($"a.pickup_longitude" / diff_dist) + 1
          )
        )
      )*/
      .withColumn(
        "adropoff_lat_bucket",
        explode(
          array(
            floor($"a.dropoff_latitude" / diff_dist) - 1,
            floor($"a.dropoff_latitude" / diff_dist),
            floor($"a.dropoff_latitude" / diff_dist) + 1
          )
        )
      )
      /*.withColumn(
        "adropoff_lon_bucket",
        explode(
          array(
            floor($"a.dropoff_longitude" / diff_dist) - 1,
            floor($"a.dropoff_longitude" / diff_dist),
            floor($"a.dropoff_longitude" / diff_dist) + 1
          )
        )
      )*/
      .join(
        trips_filtered
          .as(alias = "b")
          .withColumn(
            "pickup_time_bucket",
            explode(
              array(
                floor($"b.tpep_pickup_datetime" / diff_time) - 1,
                floor($"b.tpep_pickup_datetime" / diff_time)
              )
            )
          )
          .withColumn(
            "dropoff_lat_bucket",
            floor($"b.dropoff_latitude" / diff_dist)
          )
          /*.withColumn(
            "dropoff_lon_bucket",
            floor($"b.dropoff_longitude" / diff_dist)
          )*/
          /*.withColumn(
            "bpickup_lon_bucket",
            floor($"b.pickup_longitude" / diff_dist)
          )*/
          .withColumn(
            "bpickup_lat_bucket",
            floor($"b.pickup_latitude" / diff_dist)
          ),
        // Bucket equi join
        $"dropoff_time_bucket" === $"pickup_time_bucket" &&
          $"pickup_lat_bucket" === $"dropoff_lat_bucket" &&
          /*$"pickup_lon_bucket" === $"dropoff_lon_bucket"&&
          $"adropoff_lon_bucket"===$"bpickup_lon_bucket"&&*/
          $"bpickup_lat_bucket" === $"adropoff_lat_bucket",
        "inner"
      )
      .drop(
        "dropoff_time_bucket",
        "pickup_lat_bucket",
        "pickup_lon_bucket",
        "pickup_time_bucket",
        "pickup_lat_bucket",
        "pickup_lon_bucket",
        "adropoff_lon_bucket",
        "bpickup_lon_bucket",
        "bpickup_lat_bucket",
        "adropoff_lat_bucket"
      )
      .filter(
        // Condition 1
        $"a.tpep_dropoff_datetime" > $"b.tpep_pickup_datetime" - diff_time &&
          $"b.tpep_pickup_datetime" > $"a.tpep_dropoff_datetime" &&
          // Condition 2
          asin(
            sqrt(
              pow(
                sin(
                  toRadians($"b.pickup_latitude" - $"a.dropoff_latitude") / 2
                ),
                2
              ) +
                cos(toRadians($"b.pickup_latitude")) * cos(
                  toRadians($"a.dropoff_latitude")
                ) *
                pow(
                  sin(
                    toRadians(
                      $"b.pickup_longitude" - $"a.dropoff_longitude"
                    ) / 2
                  ),
                  2
                )
            )
          ) * 2 * 6371000 < lit(dist) &&
          // Condition 3
          asin(
            sqrt(
              pow(
                sin(
                  toRadians($"b.dropoff_latitude" - $"a.pickup_latitude") / 2
                ),
                2
              ) +
                cos(toRadians($"b.dropoff_latitude")) * cos(
                  toRadians($"a.pickup_latitude")
                ) *
                pow(
                  sin(
                    toRadians(
                      $"b.dropoff_longitude" - $"a.pickup_longitude"
                    ) / 2
                  ),
                  2
                )
            )
          ) * 2 * 6371000 < lit(dist)
      )
    trips_final
  }
}
