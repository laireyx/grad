package event

import TypedProperties
import kotlin.random.Random

class Message(messageType: String, private val eventName: String) {
    private val configs = TypedProperties("messages/$messageType")

    private val messageName: String = configs["message.name"]

    private val minimumLength: Int = configs["message.length.min"]
    private val maximumLength: Int = configs["message.length.max"]

    val body: String
        get() = "[$eventName/$messageName]".padEnd(Random.nextInt(minimumLength, maximumLength), '@')
}