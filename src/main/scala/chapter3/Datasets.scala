package chapter3

import spark.Spark
import config.Config._
import org.apache.spark.sql.Dataset

object Datasets extends Spark {
  import spark.implicits._

  case class DeviceIoTData(battery_level: Long, c02_level: Long,
                           cca2: String, cca3: String, cn: String, device_id: Long,
                           device_name: String, humidity: Long, ip: String, latitude: Double,
                           lcd: String, longitude: Double, scale: String, temp: Long,
                           timestamp: Long)

  val ds: Dataset[DeviceIoTData] = spark.read
    .json(deviceIoTData)
    .as[DeviceIoTData]

  val filterTempDS: Dataset[DeviceIoTData] = ds.filter(d => d.temp > 30 && d.humidity > 70)

  case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long, cca3: String)

  val dsTemp: Dataset[DeviceTempByCountry] = ds
    .filter(_.temp > 25)
    .map(d => DeviceTempByCountry(d.temp, d.device_name, d.device_id, d.cca3))
}
