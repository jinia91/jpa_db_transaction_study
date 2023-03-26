package com.example.jpa_study_playground

import mu.KotlinLogging
import org.aspectj.lang.JoinPoint
import org.aspectj.lang.annotation.AfterReturning
import org.aspectj.lang.annotation.Aspect
import org.aspectj.lang.annotation.Pointcut
import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionSynchronization
import org.springframework.transaction.support.TransactionSynchronizationManager

private val log = KotlinLogging.logger {}

@Aspect
@Component
class TransactionCommitLogTracer {

    @Pointcut("@annotation(org.springframework.transaction.annotation.Transactional)")
    fun transactionalMethods() {
    }

    @AfterReturning(pointcut = "transactionalMethods()", returning = "result")
    fun logTransactionCommit(joinPoint: JoinPoint, result: Any?) {
        TransactionSynchronizationManager.registerSynchronization(object : TransactionSynchronization {
            override fun afterCommit() {
                log.info { "Transaction 커밋완료" }
            }
        })
    }
}
