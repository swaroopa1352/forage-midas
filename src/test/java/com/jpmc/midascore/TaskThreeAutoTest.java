package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(properties = {"spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"})
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskThreeAutoTest {

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private UserRepository userRepository;

    @Test
    void task_three_auto_verifier() throws Exception {
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }

        // wait for listener + JPA updates by polling the repository until expected value or timeout
        final int expectedFloor = 627;
        final long timeoutMs = 10_000L; // total timeout
        final long pollIntervalMs = 200L;
        long start = System.currentTimeMillis();
        UserRecord waldorf = null;
        int floored = Integer.MIN_VALUE;
        while (System.currentTimeMillis() - start < timeoutMs) {
            waldorf = userRepository.findById(5);
            if (waldorf != null) {
                floored = (int) Math.floor(waldorf.getBalance());
                if (floored == expectedFloor) {
                    break;
                }
            }
            Thread.sleep(pollIntervalMs);
        }

        if (waldorf == null) {
            throw new AssertionError("waldorf user (id=5) not found after waiting");
        }

    // debug: (removed) previously printed waldorf balance for troubleshooting
        // verify floor value
        assertEquals(expectedFloor, floored, "waldorf floor balance should be " + expectedFloor);
    }
}
