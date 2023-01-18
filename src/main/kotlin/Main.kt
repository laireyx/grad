import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch

suspend fun main() {
    val consumerJob = CoroutineScope(Dispatchers.Default).launch { SimpleConsumer().testConsumer() }
    val producerJob = CoroutineScope(Dispatchers.Default).launch { SimpleProducer().testSimpleProducer() }

    consumerJob.join()
    producerJob.join()
}
