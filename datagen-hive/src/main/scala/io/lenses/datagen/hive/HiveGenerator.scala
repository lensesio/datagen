package io.lenses.datagen.hive

import com.sksamuel.rxhive.evolution.NoopSchemaEvolver
import com.sksamuel.rxhive.formats.ParquetFormat
import com.sksamuel.rxhive.partitioners.DynamicPartitioner
import com.sksamuel.rxhive.resolver.LenientStructResolver
import com.sksamuel.rxhive.{CreateTableConfig, DatabaseName, Float64Type, HiveUtils, HiveWriter, Int32Type, PartitionKey, PartitionPlan, RxHiveFileNamer, StagingFileManager, StringType, Struct, StructField, StructType, TableName, WriteMode}
import com.typesafe.scalalogging.StrictLogging
import io.lenses.data.generator.stocks.StockGenerator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.{HiveMetaStoreClient, TableType}
import scopt.OptionParser

object HiveGenerator extends App with StrictLogging {

  case class HiveDataGenConfig(table: String, database: String, metastoreUris: String, defaultFS: String, count: Int, partitions: List[String])

  logger.info(
    """
      |
      |  _
      | | |    ___ _ __  ___  ___  ___
      | | |   / _ \ '_ \/ __|/ _ \/ __|
      | | |__|  __/ | | \__ \  __/\__ \
      | |_____\___|_|_|_|___/\___||___/                         _
      | |  _ \  __ _| |_ __ _   / ___| ___ _ __   ___ _ __ __ _| |_ ___  _ __
      | | | | |/ _` | __/ _` | | |  _ / _ \ '_ \ / _ \ '__/ _` | __/ _ \| '__|
      | | |_| | (_| | || (_| | | |_| |  __/ | | |  __/ | | (_| | || (_) | |
      | |____/ \__,_|\__\__,_|  \____|\___|_| |_|\___|_|  \__,_|\__\___/|_|
      |
    """.stripMargin)

  val parser: OptionParser[HiveDataGenConfig] = new scopt.OptionParser[HiveDataGenConfig]("generator") {
    head("generator")

    opt[String]("metastore-uris").optional().action { case (uris, conf) =>
      conf.copy(metastoreUris = uris)
    }

    opt[String]("default-fs").optional().action { case (defaultFs, a) =>
      a.copy(defaultFS = defaultFs)
    }

    opt[Int]("count").optional().action { case (count, a) =>
      a.copy(count = count)
    }

    opt[String]("database").optional().action { case (database, a) =>
      a.copy(database = database)
    }

    opt[String]("table").optional().action { case (table, a) =>
      a.copy(table = table)
    }

    opt[String]("partitions").optional().action { case (partitions, a) =>
      a.copy(partitions = partitions.split(',').map(_.trim).toList)
    }
  }

  parser.parse(args, HiveDataGenConfig("", "", "", "", 1000, Nil)).foreach { config =>
    require(config.defaultFS.nonEmpty)
    require(config.metastoreUris.nonEmpty)
    require(config.table.nonEmpty)
    require(config.database.nonEmpty)

    implicit val hiveConf: HiveConf = new HiveConf()
    hiveConf.set("hive.metastore", "thrift")
    hiveConf.set("hive.metastore.uris", config.metastoreUris)

    implicit val client: HiveMetaStoreClient = new HiveMetaStoreClient(hiveConf)

    implicit val conf: Configuration = new Configuration()
    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    conf.set("fs.defaultFS", config.defaultFS)

    implicit val fs: FileSystem = FileSystem.get(conf)

    val schema = new StructType(
      new StructField("category", StringType.INSTANCE),
      new StructField("etf", StringType.INSTANCE),
      new StructField("symbol", StringType.INSTANCE),
      new StructField("name", StringType.INSTANCE),
      new StructField("bid", Float64Type.INSTANCE),
      new StructField("ask", Float64Type.INSTANCE),
      new StructField("lot-size", Int32Type.INSTANCE)
    )

    new HiveUtils(client, fs).dropTable(new DatabaseName(config.database), new TableName(config.table), true)

    import scala.collection.JavaConverters._
    val plan = if (config.partitions.isEmpty) null else new PartitionPlan(config.partitions.map(new PartitionKey(_)).asJava)

    val createConfig = new CreateTableConfig(schema, plan, TableType.MANAGED_TABLE, ParquetFormat.INSTANCE, null)
    val writer = new HiveWriter(new DatabaseName(config.database), new TableName(config.table), WriteMode.Create, DynamicPartitioner.INSTANCE, new StagingFileManager(RxHiveFileNamer.INSTANCE), NoopSchemaEvolver.INSTANCE, LenientStructResolver.INSTANCE, createConfig, client, fs)

    for (k <- 1 to config.count) {
      val tick = StockGenerator.generateTick
      val struct = new Struct(schema, tick.category, java.lang.Boolean.valueOf(tick.etf), tick.symbol, tick.name, java.lang.Double.valueOf(tick.bid), java.lang.Double.valueOf(tick.ask), java.lang.Integer.valueOf(tick.lotSize))
      writer.write(struct)
      if (k % 1000 == 0)
        println(s"Completed $k messsages")
    }

    writer.close()
  }
}
