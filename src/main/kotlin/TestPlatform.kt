class TestPlatform(
    private val producerNum: Int,
    private val consumerNum: Int,
    private val topicNum: Int,
    private val partitionNum: Int,
    private val produceSpeedMs: Int,
    private val consumeSpeedMs: Int,
    private val messageLengthMin: Int,
    private val messageLengthMax: Int,
) {
    private val consumers: Array<SimpleConsumer> = Array(consumerNum) { SimpleConsumer() }
    private val producers: Array<SimpleProducer> = Array(producerNum) { SimpleProducer(it) }

    init {
        // TODO: initialize something

    }
    public fun test() {
        // TODO: test something

    }
}