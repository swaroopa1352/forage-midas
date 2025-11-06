package com.jpmc.midascore.kafka;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.incentive.IncentiveService;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    private final List<Transaction> received = Collections.synchronizedList(new ArrayList<>());
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final IncentiveService incentiveService;

    public TransactionListener(UserRepository userRepository, TransactionRepository transactionRepository,
                               IncentiveService incentiveService) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.incentiveService = incentiveService;
    }

    public List<Transaction> getReceived() {
        return received;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core", containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void listen(Transaction transaction) {
        received.add(transaction);
        
        // Get sender and recipient
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        // Validate transaction
        if (isValidTransaction(transaction, sender, recipient)) {
            // Record transaction and update balances
            recordTransaction(transaction, sender, recipient);
        } else {
            logger.info("Invalid transaction: {}", transaction);
        }
    }

    private boolean isValidTransaction(Transaction transaction, UserRecord sender, UserRecord recipient) {
        return sender != null && 
               recipient != null && 
               sender.getBalance() >= transaction.getAmount();
    }

    private void recordTransaction(Transaction transaction, UserRecord sender, UserRecord recipient) {
        // Debug logging for Waldorf's transactions
        if (sender.getId() == 5 || recipient.getId() == 5) {
            logger.info("WALDORF TRANSACTION - Before: Balance = {}", 
                (sender.getId() == 5 ? sender.getBalance() : recipient.getBalance()));
        }
        
        // Update balances for the base transaction
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        // Ask incentives API whether an additional incentive should be granted for this transaction
        float incentiveFloat = 0f;
        try {
            java.math.BigDecimal incentive = incentiveService.fetchIncentiveFor(transaction);
            if (incentive != null && incentive.compareTo(java.math.BigDecimal.ZERO) > 0) {
                // convert incentive to float to match UserRecord.balance type
                incentiveFloat = incentive.floatValue();
                recipient.setBalance(recipient.getBalance() + incentiveFloat);
                logger.info("Applied incentive {} to recipient {}", incentive, recipient.getName());
            }
        } catch (Exception e) {
            logger.warn("Error while fetching/applying incentive: {}", e.getMessage());
        }

        // Debug logging for Waldorf's new balance
        if (sender.getId() == 5 || recipient.getId() == 5) {
            logger.info("WALDORF TRANSACTION - After: Balance = {}, Amount = {}, Type = {}", 
                (sender.getId() == 5 ? sender.getBalance() : recipient.getBalance()),
                transaction.getAmount(),
                (sender.getId() == 5 ? "SENT" : "RECEIVED"));
        }

        // Save users
        userRepository.save(sender);
        userRepository.save(recipient);

        // Create and save transaction record
    TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveFloat);
        transactionRepository.save(record);

        logger.info("Transaction processed: {} -> {}, amount: {}", 
            sender.getName(), recipient.getName(), transaction.getAmount());
    }
}
