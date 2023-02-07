import java.util.*

class MessageFactory(
    private val batchSize: Int,
    private val minLen: Int,
    private val maxLen: Int
    ) {

    fun generate(tag: String) = Array(batchSize) {
        "[$tag]".padEnd(Random().nextInt(minLen, maxLen + 1), '@')
    }
}