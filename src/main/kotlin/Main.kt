import kotlinx.coroutines.runBlocking
import test.TestPlatform

suspend fun main() = runBlocking {
    TestPlatform().test()
}
