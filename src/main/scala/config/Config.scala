package config

import com.typesafe.config._

object Config {
  val config: Config = ConfigFactory.load()

  val mnmDataset: String = config.getString("dataset.mnm-dataset")

  val jsonDataset: String = config.getString("dataset.json-dataset")

  val sfFireDataset: String = config.getString("dataset.fire-calls-dataset")

  val deviceIoTData: String = config.getString("dataset.device-iot-dataset")
}
