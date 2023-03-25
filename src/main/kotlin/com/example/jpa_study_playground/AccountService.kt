package com.example.jpa_study_playground

import mu.KotlinLogging
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Isolation
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import javax.persistence.EntityManager
import kotlin.concurrent.thread

private val log = KotlinLogging.logger {}

@Service
class AccountService(
    private val accountRepository: AccountRepository,
    private val em: EntityManager,
    private val transactionCommitLogTracer: TransactionCommitLogTracer
) {
    @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
    fun updateBalanceWithReadUnCommittedForReplicatingDirtyRead(accountId: Long, newBalance: Long) {
        transactionCommitLogTracer.logTransactionCommit();
        val account: Account = accountRepository.findById(accountId).orElse(null) ?: throw Exception("Account not found")
        account.balance = newBalance
        accountRepository.save(account)
        em.flush()
        Thread.sleep(5000)
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
    fun getBalanceWithReadUnCommittedForReplicatingDirtyRead(accountId: Long): Long? {
        transactionCommitLogTracer.logTransactionCommit();
        return accountRepository.findById(accountId).orElse(null)?.balance
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
    fun updateBalanceWithReadUnCommittedForReplicatingNonRepeatableRead(accountId: Long, newBalance: Long) {
        transactionCommitLogTracer.logTransactionCommit();
        val account: Account = accountRepository.findById(accountId).orElse(null) ?: throw Exception("Account not found")
        account.balance = newBalance
        accountRepository.save(account)
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
    fun getBalanceRepeatableWithReadUnCommittedForNonRepeatableRead(accountId: Long): Long? {
        transactionCommitLogTracer.logTransactionCommit();
        for (i in 1..10) {
            accountRepository.findById(accountId).orElse(null)?.balance
                .also { log.info { it } }
            em.clear()
            Thread.sleep(500)
        }
        return accountRepository.findById(accountId).orElse(null)?.balance
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
    fun createAccount(initialBalance: Long): Account? {
        transactionCommitLogTracer.logTransactionCommit();
        val account = Account(balance = initialBalance)
        account.balance = initialBalance
        return accountRepository.save(account)
    }
}
