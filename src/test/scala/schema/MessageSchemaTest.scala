package schema

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for MessageSchema.
 */
class MessageSchemaTest extends AnyFunSuite {
  test("Message case class should store message string") {
    val msg = MessageSchema.Message("test message")
    assert(msg.message == "test message")
  }
} 