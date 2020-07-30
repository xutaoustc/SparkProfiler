import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader}

object Test {
  def main(args:Array[String]):Unit={
    val loader = new CacheLoader[String,String](){
      override def load(k: String): String = k.toUpperCase()
    }
    val cache = CacheBuilder.newBuilder()
      .expireAfterAccess(1,TimeUnit.DAYS)
      .build(loader);

    val cc = cache.get("ss")
    println(cc)


  }
}
