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
class ReadCommittedTransactionTests {

    @Autowired
    lateinit var accountServiceWithTransactionalReadCommitted: AccountServiceWithTransactionalReadCommitted

    @Autowired
    lateinit var accountRepository: AccountRepository

    @Autowired
    lateinit var em : EntityManager

    @AfterEach
    fun tearDown() {
        accountRepository.deleteAll()
    }

    @Test
    fun `READ_COMMITTED에서 Dirty Read Test`() {
        log.info { "메인 스레드 확인" }
        val initialBalance = 1000L
        val updatedBalance = 2000L

        val account = accountServiceWithTransactionalReadCommitted.createAccount(initialBalance)
        val accountId = account!!.id!!
        em.clear()

        val updateBalanceExecutor = Executors.newSingleThreadExecutor()
        val updateBalanceJob = CompletableFuture.runAsync( {
            log.info { "스레드 A : 잔액 변경" }
            accountServiceWithTransactionalReadCommitted.updateBalanceForReplicatingDirtyRead(accountId, updatedBalance)
        }, updateBalanceExecutor)

        Thread.sleep(3000)

        val getBalanceExecutor = Executors.newSingleThreadExecutor()
        val getBalanceJob = CompletableFuture.supplyAsync({
            log.info { "스레드 B : 잔액 조회" }
            accountServiceWithTransactionalReadCommitted.getBalanceForReplicatingDirtyRead(accountId)
        }, getBalanceExecutor)

        val returnedBalance = getBalanceJob.get()
        log.info { "스레드 B : 잔액 받아옴" }

        Assertions.assertThat(updatedBalance).isNotEqualTo(returnedBalance)
            .also { log.info {"updatedBalance($updatedBalance) != returnedBalance($returnedBalance)  Dirty Read 방지, 커밋된 스냅샷만 읽을수 있음"} }

        updateBalanceJob.get()
        log.info { "스레드 A : 트랜잭션 커밋 일어남" }

    }


    @Test
    fun `READ_COMMITTED에서 select_pessimisic_read시 s-lock 사용 확인 test`() {
        log.info { "메인 스레드 확인" }
        val initialBalance = 1000L
        val updatedBalance = 2000L

        val account = accountServiceWithTransactionalReadCommitted.createAccount(initialBalance)
        val accountId = account!!.id!!
        em.clear()

        val getBalanceExecutor = Executors.newSingleThreadExecutor()
        val getBalanceJob = CompletableFuture.supplyAsync({
            log.info { "스레드 A : 잔액 조회" }
            accountServiceWithTransactionalReadCommitted.getBalanceWithSharedLockForTestingSharedLock(accountId)
        }, getBalanceExecutor)

        Thread.sleep(2000)

        val updateBalanceExecutor = Executors.newSingleThreadExecutor()
        val updateBalanceJob = CompletableFuture.runAsync( {
            log.info { "스레드 B : 잔액 변경" }
            accountServiceWithTransactionalReadCommitted.updateBalanceForTestingSharedLock(accountId, updatedBalance)
        }, updateBalanceExecutor)

        updateBalanceJob.get()
        log.info { "스레드 B : 트랜잭션 커밋 먼저 일어날 수 없음 이 위에 스레드 A 트랜잭션 커밋이 존재해야함" }
        val returnedBalance = getBalanceJob.get()

        Assertions.assertThat(updatedBalance).isNotEqualTo(returnedBalance)
            .also { log.info {"updatedBalance($updatedBalance) != returnedBalance($returnedBalance)  Dirty Read 방지, 커밋된 스냅샷만 읽을수 있음"} }

        /**
         *
         * 동작이 신기한점
         *
         * - 1. 스레드 A의 트랜잭션은 for update 이므로 shared Lock을 사용
         * - 2. 스레드 B가 먼저 끝나는 트랜잭션이나, lock이 걸려있으므로 그리고 B 트핸잭션 역시 shared Lock 을 사용하는 update 쿼리이므로, A 트랜잭션 커밋될때까지 대기 후 커밋
         * - 3. hibernate SQL Log가 커밋 이전 시점에 미리 날라가는점은 알아 둬야 할점인듯
         *
         */
    }

    @Test
    fun `READ_COMMITTED에서 update 중 read시 동시성 이슈 확인 - dirty read 를 안하면서 생기는 현상 test`() {
        log.info { "메인 스레드 확인" }
        val initialBalance = 1000L
        val updatedBalance = 2000L

        val account = accountServiceWithTransactionalReadCommitted.createAccount(initialBalance)
        val accountId = account!!.id!!
        em.clear()

        val updateBalanceExecutor = Executors.newSingleThreadExecutor()
        val updateBalanceJob = CompletableFuture.runAsync( {
            log.info { "스레드 A : 잔액 변경" }
            accountServiceWithTransactionalReadCommitted.updateBalanceForTestingSharedLockTakingLongTime(accountId, updatedBalance)
        }, updateBalanceExecutor)

        Thread.sleep(1000)

        val getBalanceExecutor = Executors.newSingleThreadExecutor()
        val getBalanceJob = CompletableFuture.supplyAsync({
            log.info { "스레드 B : 잔액 조회" }
            accountServiceWithTransactionalReadCommitted.getBalanceForTestingSharedLockWithoutLag(accountId)
        }, getBalanceExecutor)

        updateBalanceJob.get()
        val returnedBalance = getBalanceJob.get()

        Assertions.assertThat(updatedBalance).isNotEqualTo(returnedBalance)
            .also { log.info {"updatedBalance($updatedBalance) != returnedBalance($returnedBalance) 스레드 A가 update동안 모종의 사유로 트랜잭션 커밋이 지연될경우 그 사이에 해당 row를 조회하는 스레드 B는 이전 형상을 가져감"} }

        /**
         *  위의 테스트와 마찬가지로 update 쿼리가 날라가므로 shared lock 인 트랜잭션이지만,
         *  read 트랜잭션 자체는 문제 없이 선커밋 가능
         *  일반적으로 jpa 기본설정에서 만날수 있는 동시성 이슈
         *
         *  이걸 더티 리드로 풀어낼수도 있긴할텐데...그보단 락을 강제하는게 더 맞지않나? 성능과 데이터 일관성의 트레이드 오프 문제일듯
         */
    }

    @Test
    fun `READ_COMMITTED에서 update 중 read시 동시성 이슈 sharedlock select으로 해결 test`() {
        log.info { "메인 스레드 확인" }
        val initialBalance = 1000L
        val updatedBalance = 2000L

        val account = accountServiceWithTransactionalReadCommitted.createAccount(initialBalance)
        val accountId = account!!.id!!
        em.clear()

        val updateBalanceExecutor = Executors.newSingleThreadExecutor()
        val updateBalanceJob = CompletableFuture.runAsync( {
            log.info { "스레드 A : 잔액 변경" }
            accountServiceWithTransactionalReadCommitted.updateBalanceForTestingSharedLockTakingLongTime(accountId, updatedBalance)
        }, updateBalanceExecutor)

        Thread.sleep(1000)

        val getBalanceExecutor = Executors.newSingleThreadExecutor()
        val getBalanceJob = CompletableFuture.supplyAsync({
            log.info { "스레드 B : 잔액 조회" }
            accountServiceWithTransactionalReadCommitted.getBalanceForTestingSharedLockWithLock(accountId)
        }, getBalanceExecutor)

        updateBalanceJob.get()
        val returnedBalance = getBalanceJob.get()

        Assertions.assertThat(updatedBalance).isEqualTo(returnedBalance)
            .also { log.info {"updatedBalance($updatedBalance) != returnedBalance($returnedBalance) 스레드 A가 update동안 모종의 사유로 트랜잭션 커밋이 지연될경우, B의 조회쿼리가 sharedLock을 사용하는 경우 A의 락이 끝날때까지 기다린후 동작하여 A이후 형상을 가져옴 "} }
    }

    @Test
    fun `READ_COMMITTED에서 Non-Repeatable Read Test`() {
        val initialBalance = 1000L
        val updatedBalance = 2000L

        val account = accountServiceWithTransactionalReadCommitted.createAccount(initialBalance)
        val accountId = account!!.id!!
        em.clear()

        val getBalanceExecutor = Executors.newSingleThreadExecutor()
        val getBalanceJob = CompletableFuture.supplyAsync({
            log.info("스레드 A : 잔액 반복 조회 0.5초마다 총 10회")
            accountServiceWithTransactionalReadCommitted.getBalanceRepeatableForNonRepeatableRead(accountId)
        }, getBalanceExecutor)

        Thread.sleep(1000)

        val updateBalanceExecutor = Executors.newSingleThreadExecutor()
        val updateBalanceJob = CompletableFuture.runAsync({
            log.info("스레드 B : 잔액 변경")
            accountServiceWithTransactionalReadCommitted.updateBalanceForReplicatingNonRepeatableRead(accountId, updatedBalance)
        }, updateBalanceExecutor)
        updateBalanceJob.get()
        log.info { "스레드 B : 트랜잭션 커밋 일어남" }

        getBalanceJob.get()
    }

    @Test
    fun `READ_COMMITTED에서 Phantom Read Test`() {
        val initialBalance = 1000L
        val additionalAccountBalance = 2000L

        accountServiceWithTransactionalReadCommitted.createAccount(initialBalance)
        accountServiceWithTransactionalReadCommitted.createAccount(initialBalance)
        em.clear()

        val getAccountsExecutor = Executors.newSingleThreadExecutor()
        val getAccountsJob = CompletableFuture.supplyAsync({
            log.info("스레드 A : 계좌들 조회하고 로깅")
            accountServiceWithTransactionalReadCommitted.findAllAndLogForReplicatingPhantomRead(initialBalance)
        }, getAccountsExecutor)

        Thread.sleep(1000)

        val addAccountExecutor = Executors.newSingleThreadExecutor()
        val addAccountJob = CompletableFuture.runAsync({
            log.info("스레드 B : 새 계좌 생성")
            val temp = accountServiceWithTransactionalReadCommitted.createAccount(additionalAccountBalance)

            Thread.sleep(1000)

            log.info("스레드 B : 새 계좌 삭제")
            accountServiceWithTransactionalReadCommitted.deleteAccount(temp!!.id!!)
        }, addAccountExecutor)

        addAccountJob.get()
        log.info("스레드 B : 새 계좌 생성 완료")

        getAccountsJob.get()
    }
}