package org.apache.spark

/**
 * original implementation based on Josh Suereth's blog
 * http://jsuereth.com/2010/07/13/monkey-patching-scala.html
 */


trait ClosableResource[R] {
  def close(r: R): Unit
}
object ClosableResource {

  implicit def genericResourceTrait[A <: { def close(): Unit }] = new ClosableResource[A] {
    override def close(r: A) = r.close()
    override def toString = "ClosableResource[{def close() : Unit }]"
  }

  implicit def jioResourceTrait[A <: java.io.Closeable] = new ClosableResource[A] {
    override def close(r: A) = r.close()
    override def toString = "ClosableResource[java.io.Closeable]"
  }
}

object ResourceUtils {

  def withResource[A: ClosableResource, B](resource: => A)(f: A => B) = {
    val r = resource
    try {
      f(r)
    } finally {
      implicitly[ClosableResource[A]].close(r)
    }
  }

}
