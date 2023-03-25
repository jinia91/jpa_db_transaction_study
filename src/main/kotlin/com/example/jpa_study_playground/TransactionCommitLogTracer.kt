package com.example.jpa_study_playground

import mu.KotlinLogging
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.transaction.support.TransactionSynchronization
import org.springframework.transaction.support.TransactionSynchronizationManager

private val log = KotlinLogging.logger {}

@Component
class TransactionCommitLogTracer {
    fun logTransactionCommit() {
        TransactionSynchronizationManager.registerSynchronization(object : TransactionSynchronization {
            override fun afterCommit() {
                log.info { "Transaction 커밋완료" }
            }
        })
    }

}