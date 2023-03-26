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
class RepeatableReadTransactionTests {

    @Autowired
    lateinit var sut: AccountServiceWithTransactionalRepeatableRead

    @Autowired
    lateinit var accountRepository: AccountRepository

    @Autowired
    lateinit var em : EntityManager

    @AfterEach
    fun tearDown() {
        accountRepository.deleteAll()
    }

    @Test
    fun `REPEATABLE_READ 에서 Dirty Read Test`() {
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

        Assertions.assertThat(updatedBalance).isNotEqualTo(returnedBalance)
            .also { log.info {"updatedBalance($updatedBalance) != returnedBalance($returnedBalance)  Dirty Read 방지, 커밋된 스냅샷만 읽을수 있음"} }

        updateBalanceJob.get()
        log.info { "스레드 A : 트랜잭션 커밋 일어남" }

    }

    @Test
    fun `REPEATABLE_READ에서 Non-Repeatable Read Test`() {
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
         * REPEATABLE_READ transaction 일경우 최초 조회시 스냅샷을 DB가 떠서 해당 트랜잭션 내에서 반복 조회시엔 해당 스냅샷으로만 반환함으로서 데이터 일관성을 유지
         */
    }


    /**
     * 팬텀 리드 현상을 재현할수가 없음
     *
     * 이론상 전체 조회값에 대한 스냅샷을 뜨진 않을테니 분명 팬텀리드가 생겨야할텐데... DB 자체 옵티마이저의 행위때문인지
     *
     * 팬텀 리드가 일어나질 않음... 팬텀리드는 특정조건에서 스냅샷 한도를 초과하는 상황일때 간헐적으로 발생하는것인가?
     *
      */
//    @Test
//    fun `REPEATABLE_READ에서 Phantom Read Test`() {
//        for(i in 1 .. 700){
//            val randomValue = Random().nextInt(500).toLong()
//            sut.createAccount(randomValue)
//        }
//        for(i in 1 .. 300){
//            sut.createAccount(400)
//        }
//
//        em.clear()
//
//        val getAccountsExecutor = Executors.newSingleThreadExecutor()
//        val getAccountsJob = CompletableFuture.supplyAsync({
//            log.info("스레드 A : 계좌들 조회하고 로깅")
//            sut.findAllAndLogForReplicatingPhantomRead(400)
//        }, getAccountsExecutor)
//
//        Thread.sleep(1000)
//
//        val addAccountExecutor = Executors.newSingleThreadExecutor()
//        val addAccountJob = CompletableFuture.runAsync({
//            log.info("스레드 B : 새 계좌 생성")
//            for(i in 1 .. 10){
//                sut.createAccount(400)
//                Thread.sleep(500)
//            }
//        }, addAccountExecutor)
//
//        addAccountJob.get()
//        log.info("스레드 B : 새 계좌 생성 완료")
//
//        getAccountsJob.get()
//
//        em.clear()
//        accountRepository.findAllByBalance(400)
//            .also { log.info("최종 결과 : ${it.size}") }
//    }

    @Test
    fun `REPEATABLE_READ에서 update 중 read시 동시성 이슈 확인 - dirty read 를 안하면서 생기는 현상 test`() {
        log.info { "메인 스레드 확인" }
        val initialBalance = 1000L
        val updatedBalance = 2000L

        val account = sut.createAccount(initialBalance)
        val accountId = account!!.id!!
        em.clear()

        val updateBalanceExecutor = Executors.newSingleThreadExecutor()
        val updateBalanceJob = CompletableFuture.runAsync( {
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

        Assertions.assertThat(updatedBalance).isNotEqualTo(returnedBalance)
            .also { log.info {"updatedBalance($updatedBalance) != returnedBalance($returnedBalance) 스레드 A가 update동안 모종의 사유로 트랜잭션 커밋이 지연될경우 그 사이에 해당 row를 조회하는 스레드 B는 이전 형상을 가져감"} }

        /**
         *  readCommitted 와 동일하게 먼저 실행된 update 트랜잭션 미종료시, 뒤늦은 select이 이전 형상을 가져오는 동시성 이슈는 발생할 수 있음
         */
    }

    @Test
    fun `REPEATABLE_READ에서 update 중 read시 동시성 이슈 sharedlock select으로 해결 test`() {
        log.info { "메인 스레드 확인" }
        val initialBalance = 1000L
        val updatedBalance = 2000L

        val account = sut.createAccount(initialBalance)
        val accountId = account!!.id!!
        em.clear()

        val updateBalanceExecutor = Executors.newSingleThreadExecutor()
        val updateBalanceJob = CompletableFuture.runAsync( {
            log.info { "스레드 A : 잔액 변경" }
            sut.updateBalanceForTestingSharedLockTakingLongTime(accountId, updatedBalance)
        }, updateBalanceExecutor)

        Thread.sleep(1000)

        val getBalanceExecutor = Executors.newSingleThreadExecutor()
        val getBalanceJob = CompletableFuture.supplyAsync({
            log.info { "스레드 B : 잔액 조회" }
            sut.getBalanceForTestingSharedLockWithLock(accountId)
        }, getBalanceExecutor)

        updateBalanceJob.get()
        val returnedBalance = getBalanceJob.get()

        Assertions.assertThat(updatedBalance).isEqualTo(returnedBalance)
            .also { log.info {"updatedBalance($updatedBalance) == returnedBalance($returnedBalance) 스레드 A가 update동안 모종의 사유로 트랜잭션 커밋이 지연될경우, B의 조회쿼리가 sharedLock을 사용하는 경우 A의 락이 끝날때까지 기다린후 동작하여 A이후 업데이트된 형상을 가져옴 "} }
    }


    @Test
    fun `REPEATABLE_READ에서 read for update중 update를 하면 lock에 의해 sync되는지 test`() {
        log.info { "메인 스레드 확인" }
        val initialBalance = 1000L
        val updatedBalance = 2000L

        val account = sut.createAccount(initialBalance)
        val accountId = account!!.id!!
        em.clear()

        val getBalanceExecutor = Executors.newSingleThreadExecutor()
        val getBalanceJob = CompletableFuture.supplyAsync({
            log.info { "스레드 A : 잔액 조회" }
            sut.getBalanceWithSharedLockAndLag(accountId)
        }, getBalanceExecutor)

        Thread.sleep(1000)

        val updateBalanceExecutor = Executors.newSingleThreadExecutor()
        val updateBalanceJob = CompletableFuture.runAsync( {
            log.info { "스레드 B : 잔액 변경" }
            sut.updateBalanceForTestingSharedLock(accountId, updatedBalance)
        }, updateBalanceExecutor)


        updateBalanceJob.get()
        val returnedBalance = getBalanceJob.get()

        Assertions.assertThat(updatedBalance).isNotEqualTo(returnedBalance)
            .also { log.info {"updatedBalance($updatedBalance) != returnedBalance($returnedBalance) 트랜잭션 A가 shared Lock을 가지고 있고 트랜잭션 B도 shared Lock을 동반한 행위이므로 A이후 B가 동작하여 업데이트됨, 다만 여기에대해서도 sql 쿼리 자체는 flush 여부에 따라 커밋 훨씬 이전에 날라감을 확인 가능"} }
    }
}