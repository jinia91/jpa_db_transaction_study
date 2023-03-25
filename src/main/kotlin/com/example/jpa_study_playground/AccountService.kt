package com.example.jpa_study_playground

import mu.KotlinLogging
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Isolation
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import javax.persistence.EntityManager

private val log = KotlinLogging.logger {}

@Service
class AccountService(
    private val accountRepository: AccountRepository,
    private val em: EntityManager,
    private val transactionCommitLogTracer: TransactionCommitLogTracer
) {
    @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
    fun updateBalanceWithReadUnCommitted(accountId: Long, newBalance: Long) {
        transactionCommitLogTracer.logTransactionCommit();
        val account: Account = accountRepository.findById(accountId).orElse(null) ?: throw Exception("Account not found")
        account.balance = newBalance
        accountRepository.save(account)
        em.flush()
        Thread.sleep(5000)
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
    fun getBalanceWithReadUnCommitted(accountId: Long): Long? {
        transactionCommitLogTracer.logTransactionCommit();
        return accountRepository.findById(accountId).orElse(null)?.balance
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED)
    fun createAccount(initialBalance: Long): Account? {
        transactionCommitLogTracer.logTransactionCommit();
        val account = Account(balance = initialBalance)
        account.balance = initialBalance
        return accountRepository.save(account)
    }
}
