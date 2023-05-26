package test

import event.SimpleEvent
import org.slf4j.LoggerFactory
import utils.PropertyHolder

abstract class TestScenario(scenarioType: String) : PropertyHolder("scenarios", scenarioType) {
    protected val logger = LoggerFactory.getLogger(this::class.java)

    protected val scenarioName: String = configs["scenario.name"]

    abstract val isEnd: Boolean

    abstract suspend fun nextEvents(): Array<SimpleEvent>
}