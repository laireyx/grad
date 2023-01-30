import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

suspend fun main() = runBlocking {
    launch { SimpleConsumer().testConsumer() }
    repeat(5) {
        launch { SimpleProducer(it).testSimpleProducer() }
    }
}
