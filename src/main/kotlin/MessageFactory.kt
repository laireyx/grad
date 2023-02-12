import java.util.*

class MessageFactory(
    private val minLen: Int,
    private val maxLen: Int
    ) {

    fun generate(tag: String, batchSize: Int) = Array(batchSize) {
        "[$tag]".padEnd(Random().nextInt(minLen, maxLen + 1), '@')
    }
}