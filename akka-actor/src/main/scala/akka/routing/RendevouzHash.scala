/**
 * Copyright (C) 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.routing

import scala.collection.immutable
import scala.reflect.ClassTag

/**
 * Rendevouz Hashing implementation.
 *
 * A good explanation of Rendevouz Hashing:
 * https://en.wikipedia.org/wiki/Rendezvous_hashing
 *
 */
class RendevouzHash[T: ClassTag] private(nodes: immutable.Map[Int, T]) {

  import RendevouzHash._

  /**
   * Adds a node to the node list.
   * Note that the instance is immutable and this
   * operation returns a new instance.
   */
  def :+(node: T): RendevouzHash[T] = new RendevouzHash(nodes ++ immutable.Map( node.hashCode() -> node ))

  /**
   * Java API: Adds a node to node list.
   * Note that the instance is immutable and this
   * operation returns a new instance.
   */
  def add(node: T): RendevouzHash[T] = this :+ node

  /**
   * Removes a node from the node list.
   * Note that the instance is immutable and this
   * operation returns a new instance.
   */
  def :-(node: T): RendevouzHash[T] = new RendevouzHash(nodes - node.hashCode())

  /**
   * Java API: Removes a node from the node list.
   * Note that the instance is immutable and this
   * operation returns a new instance.
   */
  def remove(node: T): RendevouzHash[T] = this :- node

  /**
   * Get the node responsible for the data key.
   * Can only be used if nodes exists in the node list,
   * otherwise throws `IllegalStateException`
   */
  def nodeFor(key: Array[Byte]): T = {
    if (isEmpty) throw new IllegalStateException("Can't get node for [%s] from an empty node list" format key)

    var highScore = -1
    var winner = 0

    for ( node <- nodes ) {
      val score = hashFor(Array[Byte](node.hashCode().toByte) ++ key)
      if (score > highScore) {
        highScore = score
        winner = node.hashCode()
      }
      else if (score == highScore) {
        winner = math.max(node.hashCode(), winner)
      }
    }
    nodes(winner)
  }

  /**
   * Get the node responsible for the data key.
   * Can only be used if nodes exists in the node list,
   * otherwise throws `IllegalStateException`
   */
  def nodeFor(key: String): T = {
    if (isEmpty) throw new IllegalStateException("Can't get node for [%s] from an empty node list" format key)

    var highScore = -1
    var winner = 0

    for ( node <- nodes ) {
      val score = hashFor(node.hashCode().toString ++ key)
      if (score > highScore) {
        highScore = score
        winner = node.hashCode()
      }
      else if (score == highScore) {
        winner = math.max(node.hashCode(), winner)
      }
    }
    nodes(winner)
  }

  /**
   * Is the node ring empty, i.e. no nodes added or all removed.
   */
  def isEmpty: Boolean = nodes.isEmpty

}

object RendevouzHash {

  def apply[T: ClassTag](nodes: Iterable[T]): RendevouzHash[T] = {
    new RendevouzHash(
      immutable.Map.empty[Int, T] ++
        (for {
          node ← nodes
        } yield (node.hashCode(), node)))
  }

  /**
   * Java API: Factory method to create a ConsistentHash
   */
  def create[T](nodes: java.lang.Iterable[T]): RendevouzHash[T] = {
    import scala.collection.JavaConverters._
    apply(nodes.asScala)(ClassTag(classOf[Any].asInstanceOf[Class[T]]))
  }

  private def hashFor(bytes: Array[Byte]): Int = MurmurHash.arrayHash(bytes)

  private def hashFor(string: String): Int = MurmurHash.stringHash(string)
}
