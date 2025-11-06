package com.jpmc.midascore;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskThreeTests {
    static final Logger logger = LoggerFactory.getLogger(TaskThreeTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private UserRepository userRepository;

    @Test
    void task_three_verifier() throws InterruptedException {
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        // Compute expected balances by replaying the input files in-memory (do not hard-code expected values)
        String[] userLines = fileLoader.loadStrings("/test_data/lkjhgfdsa.hjkl");
        Map<Long, BigDecimal> expectedBalances = new HashMap<>();
        for (int i = 0; i < userLines.length; i++) {
            String[] parts = userLines[i].split(", ");
            BigDecimal bal = new BigDecimal(parts[1]);
            expectedBalances.put((long) (i + 1), bal);
        }

        for (String txLine : transactionLines) {
            String[] parts = txLine.split(", ");
            long sender = Long.parseLong(parts[0].trim());
            long recipient = Long.parseLong(parts[1].trim());
            BigDecimal amount = new BigDecimal(parts[2].trim());
            if (expectedBalances.containsKey(sender) && expectedBalances.containsKey(recipient)) {
                BigDecimal senderBal = expectedBalances.get(sender);
                if (senderBal.compareTo(amount) >= 0) {
                    expectedBalances.put(sender, senderBal.subtract(amount));
                    expectedBalances.put(recipient, expectedBalances.get(recipient).add(amount));
                }
            }
        }

        final int expectedWaldorfFloor = expectedBalances.get(5L).setScale(0, RoundingMode.DOWN).intValue();

        // wait for listener + JPA updates by polling the repository until expected value or timeout
        final long timeoutMs = 10_000L;
        final long pollIntervalMs = 200L;
        long start = System.currentTimeMillis();
        UserRecord waldorf = null;
        int floored = Integer.MIN_VALUE;
        while (System.currentTimeMillis() - start < timeoutMs) {
            waldorf = userRepository.findById(5);
            if (waldorf != null) {
                floored = (int) Math.floor(waldorf.getBalance());
                if (floored == expectedWaldorfFloor) {
                    break;
                }
            }
            Thread.sleep(pollIntervalMs);
        }

        if (waldorf == null) {
            throw new AssertionError("waldorf user (id=5) not found after waiting");
        }

        logger.info("waldorf final (reported) balance = {}", waldorf.getBalance());
        logger.info("expected waldorf floor = {}", expectedWaldorfFloor);

        assertEquals(expectedWaldorfFloor, floored, "waldorf floor balance should match computed expected");
    }
}
