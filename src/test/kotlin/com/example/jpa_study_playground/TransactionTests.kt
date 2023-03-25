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
class TransactionTests {

    @Autowired
    lateinit var accountService: AccountService

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

        val account = accountService.createAccount(initialBalance)
        val accountId = account!!.id!!
        em.clear()

        val updateBalanceExecutor = Executors.newSingleThreadExecutor()
        val updateBalanceJob = CompletableFuture.runAsync( {
            log.info { "스레드 A : 잔액 변경" }
            accountService.updateBalanceWithReadUnCommittedForReplicatingDirtyRead(accountId, updatedBalance)
        }, updateBalanceExecutor)

        Thread.sleep(3000)

        val getBalanceExecutor = Executors.newSingleThreadExecutor()
        val getBalanceJob = CompletableFuture.supplyAsync({
            log.info { "스레드 B : 잔액 조회" }
            accountService.getBalanceWithReadUnCommittedForReplicatingDirtyRead(accountId)
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

        val account = accountService.createAccount(initialBalance)
        val accountId = account!!.id!!
        em.clear()

        val getBalanceExecutor = Executors.newSingleThreadExecutor()
        val getBalanceJob = CompletableFuture.supplyAsync({
            log.info("스레드 A : 잔액 반복 조회 0.5초마다 총 10회")
            accountService.getBalanceRepeatableWithReadUnCommittedForNonRepeatableRead(accountId)
        }, getBalanceExecutor)

        Thread.sleep(1000)

        val updateBalanceExecutor = Executors.newSingleThreadExecutor()
        val updateBalanceJob = CompletableFuture.runAsync({
            log.info("스레드 B : 잔액 변경")
            accountService.updateBalanceWithReadUnCommittedForReplicatingNonRepeatableRead(accountId, updatedBalance)
        }, updateBalanceExecutor)
        updateBalanceJob.get()
        log.info { "스레드 B : 트랜잭션 커밋 일어남" }

        getBalanceJob.get()
    }
}