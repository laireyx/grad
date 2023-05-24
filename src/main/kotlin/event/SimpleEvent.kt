package event

import utils.PropertyHolder
import kotlin.random.Random

class SimpleEvent(eventType: String) : PropertyHolder("events", eventType) {
    private val eventName: String = configs["event.name"]

    private val minimumMessageNumber: Int = configs["message.number.min"]
    private val maximumMessageNumber: Int = configs["message.number.max"]

    private val messageTypes: Array<String> = configs["message.type[]"]

    fun fireMessages() = Array(Random.nextInt(minimumMessageNumber, maximumMessageNumber)) {
        val messageType = messageTypes.random()
        Message(messageType, eventName)
    }
}