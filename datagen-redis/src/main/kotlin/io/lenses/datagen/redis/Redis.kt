package io.lenses.datagen.redis

import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.mainBody
import redis.clients.jedis.Jedis
import redis.clients.jedis.StreamEntryID

class RedisConfig(parser: ArgParser) {

  val host by parser.storing(
      "--host",
      help = "Hostname of the redis server")

  val port by parser.storing(
      "--port",
      help = "Port of the redis server") { toInt() }

  val key by parser.storing(
      "--key",
      help = "Key name used to insert streaming data")

  val count by parser.storing(
      "--count",
      help = "Number of sample records to insert") { toInt() }
}

fun main(args: Array<String>): Unit = mainBody {

  println(
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
    """)

  ArgParser(args).parseInto(::RedisConfig).run {
    println("Generating $count records")
    val jedis = Jedis(host, port)
    repeat(count) {
      jedis.xadd(key, StreamEntryID.NEW_ENTRY, Tick.gen().toMap())
    }
    jedis.close()
  }

}


