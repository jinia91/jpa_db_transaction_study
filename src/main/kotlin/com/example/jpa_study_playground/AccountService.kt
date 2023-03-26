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
) {

    @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
    fun createAccount(initialBalance: Long): Account? {
        val account = Account(balance = initialBalance)
        account.balance = initialBalance
        return accountRepository.save(account)
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
    fun deleteAccount(accountId: Long) {
        return accountRepository.deleteById(accountId)
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
    fun updateBalanceWithReadUnCommittedForReplicatingDirtyRead(accountId: Long, newBalance: Long) {
        val account: Account = accountRepository.findById(accountId).orElse(null) ?: throw Exception("Account not found")
        account.balance = newBalance
        accountRepository.save(account)
        em.flush()
        Thread.sleep(5000)
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
    fun getBalanceWithReadUnCommittedForReplicatingDirtyRead(accountId: Long): Long? {
        return accountRepository.findById(accountId).orElse(null)?.balance
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
    fun updateBalanceWithReadUnCommittedForReplicatingNonRepeatableRead(accountId: Long, newBalance: Long) {
        val account: Account = accountRepository.findById(accountId).orElse(null) ?: throw Exception("Account not found")
        account.balance = newBalance
        accountRepository.save(account)
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
    fun getBalanceRepeatableWithReadUnCommittedForNonRepeatableRead(accountId: Long): Long? {
        for (i in 1..10) {
            accountRepository.findById(accountId).orElse(null)?.balance
                .also { log.info { it } }
            em.clear()
            Thread.sleep(500)
        }
        return accountRepository.findById(accountId).orElse(null)?.balance
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
    fun testWithReadUnCommittedForReplicatingPhantomRead(accountId: Long) {
        for(i in 1..3) {
            accountRepository.findAll()
                .also { log.info { "$i 번째 조회시 전체 컬렉션 사이즈는 현재 ${it.size}" } }
            em.clear()
            Thread.sleep(1000)
        }
    }


}
