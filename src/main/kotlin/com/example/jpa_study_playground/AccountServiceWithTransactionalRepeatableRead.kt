package com.example.jpa_study_playground

import mu.KotlinLogging
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Isolation
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import javax.persistence.EntityManager

private val log = KotlinLogging.logger {}

@Service
@Transactional(isolation = Isolation.REPEATABLE_READ, propagation = Propagation.REQUIRES_NEW)
class AccountServiceWithTransactionalRepeatableRead(
    private val accountRepository: AccountRepository,
    private val em: EntityManager,
) {

    fun createAccount(initialBalance: Long): Account? {
        val account = Account(balance = initialBalance)
        account.balance = initialBalance
        return accountRepository.save(account)
    }

    fun deleteAccount(accountId: Long) {
        return accountRepository.deleteById(accountId)
    }

    fun updateBalanceForReplicatingDirtyRead(accountId: Long, newBalance: Long) {
        val account: Account = accountRepository.findById(accountId).orElse(null) ?: throw Exception("Account not found")
        account.balance = newBalance
        accountRepository.save(account)
        em.flush()
        Thread.sleep(5000)
    }

    fun updateBalanceForTestingSharedLock(accountId: Long, newBalance: Long) : Account {
        val account: Account = accountRepository.findById(accountId).orElse(null) ?: throw Exception("Account not found")
        account.balance = newBalance
        return accountRepository.save(account)
    }

    fun updateBalanceForTestingSharedLockTakingLongTime(accountId: Long, newBalance: Long) {
        val account: Account = accountRepository.findById(accountId).orElse(null) ?: throw Exception("Account not found")
        account.balance = newBalance
        accountRepository.save(account)
        em.flush()
        Thread.sleep(7000)
    }

    fun getBalanceForTestingSharedLockWithoutLag(accountId: Long) : Long? {
        val tmp = accountRepository.findById(accountId).orElse(null)?.balance
        log.info { tmp }
        return tmp
    }

    fun getBalanceForTestingSharedLockWithLock(accountId: Long) : Long? {
        val tmp = accountRepository.findByIdWithLock(accountId)?.balance
        log.info { tmp }
        return tmp
    }

    fun getBalanceWithSharedLockAndLag(accountId: Long): Long? {
        val balance = accountRepository.findByIdWithLock(accountId)?.balance
        em.flush()
        Thread.sleep(7000)
        return balance
    }


    fun getBalanceForReplicatingDirtyRead(accountId: Long): Long? {
        return accountRepository.findById(accountId).orElse(null)?.balance
    }


    fun updateBalanceForReplicatingNonRepeatableRead(accountId: Long, newBalance: Long) {
        val account: Account = accountRepository.findById(accountId).orElse(null) ?: throw Exception("Account not found")
        account.balance = newBalance
        accountRepository.save(account)
    }

    fun getBalanceRepeatableForNonRepeatableRead(accountId: Long): Long? {
        for (i in 1..10) {
            accountRepository.findById(accountId).orElse(null)?.balance
                .also { log.info { it } }
            em.clear()
            Thread.sleep(500)
        }
        return accountRepository.findById(accountId).orElse(null)?.balance
    }

    fun findAllAndLogForReplicatingPhantomRead(balance: Long) {
        for(i in 1..10) {
            accountRepository.findAllByBalance(balance)
                .also { log.info { "$i 번째 조회시 전체 컬렉션 사이즈는 현재 ${it.size}" } }
            em.clear()
            Thread.sleep(1000)
        }
    }
}