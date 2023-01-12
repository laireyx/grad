
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*

class SimpleProducer {

    // logger 선언
    private val logger = LoggerFactory.getLogger(this::class.java)

    // 저장하고자하는 토픽의 이름을 지정
    private val TOPIC_NAME = "test"

    // 카프카 브로커가 올라가있는 host의 이름과 port 번호를 기입
    private val BOOTSTRAP_SERVER = "localhost:9092"

    fun testSimpleProducer() {
        val configs = Properties()
        configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVER
        // 직렬화/역직력화 정책을 카프카 라이브러리의 StringSerializer로 선택한다
        configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name

        // properties를 kafkaProducer의 파라미터로 전달하여 producer 인스턴스를 생성한다.
        val producer = KafkaProducer<String, String>(configs)

        val messageValue = "testMessage"
        // 메시지 키가 선언되지 않은 상태로 메시지 값만 할당하여 record를 생성하였다.
        // ProducerRecord의 제네릭 2개는 각각 key/value의 타입을 의미한다
        val record = ProducerRecord<String, String>(TOPIC_NAME, messageValue)
        // procuder.send()는 즉각적으로 전송하는게 아니라 배치 타입으로 전송을한다.
        producer.send(record)

        logger.info("$record")

        // flush() 메소드를 통해서 프로듀서 내부 버퍼가 가지고있던 모든 레코드 배치를 브로커로 전달한다
        producer.flush()
        // producer instance의 모든 리소스를 안전하게 종료시킨다
        producer.close()
    }
}
