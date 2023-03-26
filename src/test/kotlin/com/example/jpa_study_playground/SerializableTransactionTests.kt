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
class SerializableTransactionTests {

    @Autowired
    lateinit var sut: AccountServiceWithTransactionalSerialize

    @Autowired
    lateinit var accountRepository: AccountRepository

    @Autowired
    lateinit var em : EntityManager

    @AfterEach
    fun tearDown() {
        accountRepository.deleteAll()
    }

    @Test
    fun `SEREALIZABLE 에서 Dirty Read Test`() {
        log.info { "메인 스레드 확인" }
        val initialBalance = 1000L
        val updatedBalance = 2000L

        val account = sut.createAccount(initialBalance)
        val accountId = account!!.id!!
        em.clear()

        val updateBalanceExecutor = Executors.newSingleThreadExecutor()
        val updateBalanceJob = CompletableFuture.runAsync({
            log.info { "스레드 A : 잔액 변경" }
            sut.updateBalanceForReplicatingDirtyRead(
                accountId,
                updatedBalance
            )
        }, updateBalanceExecutor)

        Thread.sleep(3000)

        val getBalanceExecutor = Executors.newSingleThreadExecutor()
        val getBalanceJob = CompletableFuture.supplyAsync({
            log.info { "스레드 B : 잔액 조회" }
            sut.getBalanceForReplicatingDirtyRead(accountId)
        }, getBalanceExecutor)

        val returnedBalance = getBalanceJob.get()
        log.info { "스레드 B : 잔액 받아옴" }

        Assertions.assertThat(updatedBalance).isEqualTo(returnedBalance)
            .also { log.info {"updatedBalance($updatedBalance) == returnedBalance($returnedBalance)  무조건 순차적으로 진행됨"} }

        updateBalanceJob.get()
        log.info { "스레드 A : 트랜잭션 커밋 일어남" }

    }

    @Test
    fun `SEREALIZABLE에서 Non-Repeatable Read Test`() {
        val initialBalance = 1000L
        val updatedBalance = 2000L

        val account = sut.createAccount(initialBalance)
        val accountId = account!!.id!!
        em.clear()

        val getBalanceExecutor = Executors.newSingleThreadExecutor()
        val getBalanceJob = CompletableFuture.supplyAsync({
            log.info("스레드 A : 잔액 반복 조회 0.5초마다 총 10회")
            sut.getBalanceRepeatableForNonRepeatableRead(accountId)
        }, getBalanceExecutor)

        Thread.sleep(1000)

        val updateBalanceExecutor = Executors.newSingleThreadExecutor()
        val updateBalanceJob = CompletableFuture.runAsync({
            log.info("스레드 B : 잔액 변경")
            sut.updateBalanceForReplicatingNonRepeatableRead(
                accountId,
                updatedBalance
            )
        }, updateBalanceExecutor)
        updateBalanceJob.get()
        log.info { "스레드 B : 트랜잭션 커밋 일어남" }

        getBalanceJob.get()

        /**
         * SEREALIZABLE transaction 일경우 조회 트랜잭션동안 업데이트 불가능
         */
    }

    @Test
    fun `SEREALIZABLE 에서 update 중 read시 test`() {
        log.info { "메인 스레드 확인" }
        val initialBalance = 1000L
        val updatedBalance = 2000L

        val account = sut.createAccount(initialBalance)
        val accountId = account!!.id!!
        em.clear()

        val updateBalanceExecutor = Executors.newSingleThreadExecutor()
        val updateBalanceJob = CompletableFuture.runAsync({
            log.info { "스레드 A : 잔액 변경" }
            sut.updateBalanceForTestingSharedLockTakingLongTime(accountId, updatedBalance)
        }, updateBalanceExecutor)

        Thread.sleep(1000)

        val getBalanceExecutor = Executors.newSingleThreadExecutor()
        val getBalanceJob = CompletableFuture.supplyAsync({
            log.info { "스레드 B : 잔액 조회" }
            sut.getBalanceForTestingSharedLockWithoutLag(accountId)
        }, getBalanceExecutor)

        updateBalanceJob.get()
        val returnedBalance = getBalanceJob.get()

        Assertions.assertThat(updatedBalance).isEqualTo(returnedBalance)
            .also { log.info {"updatedBalance($updatedBalance) != returnedBalance($returnedBalance) 무조건 순차적"} }

    }
}