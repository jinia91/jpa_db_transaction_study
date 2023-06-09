package com.example.jpa_study_playground

import mu.KotlinLogging
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import javax.persistence.EntityManager


private val log = KotlinLogging.logger {}

@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ReadUnCommittedTransactionTests {

    @Autowired
    lateinit var accountServiceWithTransactionalReadUnCommitted: AccountServiceWithTransactionalReadUnCommitted

    @Autowired
    lateinit var accountRepository: AccountRepository

    @Autowired
    lateinit var em : EntityManager

    @AfterEach
    fun tearDown() {
        accountRepository.deleteAll()
    }

    @Test
    fun `READ_UNCOMMITTED에서 Dirty Read Test`() {
        log.info { "메인 스레드 확인" }
        val initialBalance = 1000L
        val updatedBalance = 2000L

        val account = accountServiceWithTransactionalReadUnCommitted.createAccount(initialBalance)
        val accountId = account!!.id!!
        em.clear()

        val updateBalanceExecutor = Executors.newSingleThreadExecutor()
        val updateBalanceJob = CompletableFuture.runAsync( {
            log.info { "스레드 A : 잔액 변경" }
            accountServiceWithTransactionalReadUnCommitted.updateBalanceForReplicatingDirtyRead(accountId, updatedBalance)
        }, updateBalanceExecutor)

        Thread.sleep(3000)

        val getBalanceExecutor = Executors.newSingleThreadExecutor()
        val getBalanceJob = CompletableFuture.supplyAsync({
            log.info { "스레드 B : 잔액 조회" }
            accountServiceWithTransactionalReadUnCommitted.getBalanceForReplicatingDirtyRead(accountId)
        }, getBalanceExecutor)

        val returnedBalance = getBalanceJob.get()
        log.info { "스레드 B : 잔액 받아옴" }

        Assertions.assertThat(updatedBalance).isEqualTo(returnedBalance)
            .also { log.info {"updatedBalance($updatedBalance) == returnedBalance($returnedBalance) 스레드 A가 커밋전임에도 B의 조회에서 반영됨 Dirty Read 발생!"} }

        updateBalanceJob.get()
        log.info { "스레드 A : 트랜잭션 커밋 일어남" }

    }

    @Test
    fun `READ_UNCOMMITTED에서 Non-Repeatable Read Test`() {
        val initialBalance = 1000L
        val updatedBalance = 2000L

        val account = accountServiceWithTransactionalReadUnCommitted.createAccount(initialBalance)
        val accountId = account!!.id!!
        em.clear()

        val getBalanceExecutor = Executors.newSingleThreadExecutor()
        val getBalanceJob = CompletableFuture.supplyAsync({
            log.info("스레드 A : 잔액 반복 조회 0.5초마다 총 10회")
            accountServiceWithTransactionalReadUnCommitted.getBalanceRepeatableForNonRepeatableRead(accountId)
        }, getBalanceExecutor)

        Thread.sleep(1000)

        val updateBalanceExecutor = Executors.newSingleThreadExecutor()
        val updateBalanceJob = CompletableFuture.runAsync({
            log.info("스레드 B : 잔액 변경")
            accountServiceWithTransactionalReadUnCommitted.updateBalanceForReplicatingNonRepeatableRead(accountId, updatedBalance)
        }, updateBalanceExecutor)
        updateBalanceJob.get()
        log.info { "스레드 B : 트랜잭션 커밋 일어남" }

        getBalanceJob.get()
    }

    @Test
    fun `READ_UNCOMMITTED에서 Phantom Read Test`() {
        val initialBalance = 1000L
        val additionalAccountBalance = 2000L

        accountServiceWithTransactionalReadUnCommitted.createAccount(initialBalance)
        accountServiceWithTransactionalReadUnCommitted.createAccount(initialBalance)
        em.clear()

        val getAccountsExecutor = Executors.newSingleThreadExecutor()
        val getAccountsJob = CompletableFuture.supplyAsync({
            log.info("스레드 A : 계좌들 조회하고 로깅")
            accountServiceWithTransactionalReadUnCommitted.findAllAndLogForReplicatingPhantomRead(initialBalance)
        }, getAccountsExecutor)

        Thread.sleep(1000)

        val addAccountExecutor = Executors.newSingleThreadExecutor()
        val addAccountJob = CompletableFuture.runAsync({
            log.info("스레드 B : 새 계좌 생성")
            val temp = accountServiceWithTransactionalReadUnCommitted.createAccount(additionalAccountBalance)

            Thread.sleep(1000)

            log.info("스레드 B : 새 계좌 삭제")
            accountServiceWithTransactionalReadUnCommitted.deleteAccount(temp!!.id!!)
        }, addAccountExecutor)

        addAccountJob.get()
        log.info("스레드 B : 새 계좌 생성 완료")

        getAccountsJob.get()
    }
}