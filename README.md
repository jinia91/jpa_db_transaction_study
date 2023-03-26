# jpa_db_transaction_study

> InnoDB mysql5.7 기준으로 작성

DBMS에 따라 정의된 isolation level의 행위에 차이가 존재.

사실상 isolation level은 스펙이며 해당 행위를 만족하기만 하면 실 구현과 동작은 별개로 봐야한다.

## Lock

Lock은 

1. 쿼리 주체에 따라 CUD행위에 대한 Lock인 exclusive Lock, R행위에 대한 Lock인 Shared Lock으로 구분할수 있고

2. 구현 방식에 따라 Record Lock 과 Gap Lock  next-key lock으로 구분

> 주의 : InnoDB에서 모든 Lock은 Select을 막지 않는다! exclusive Lock에 한에 Lock을 동반한 Select을 막을 뿐!

### exclusive Lock, Shared Lock

#### exclusive Lock
- 락을 동반하는 다른 트랜잭션의 접근을 모두 대기시키게만듬

#### s Lock
- ex 락은 대기시키나 같은 s-락의 트랜잭션 접근은 허용
- 애초에 select을 위한 lock이므로 같은 select 행위에 대한 접근은 안막는다보면됨

### Record Lock, Gap Lock, next key lock

todo

### user lock 등 다양한 mysql이 제공하는 lock들

todo


## Transaction Isolation level

### level 1. read uncommitted

#### 요건

- 가장 낮은 수준의 격리수준
- 트랜잭션내에서 커밋전에 행하는 모든 쿼리에 대해 다른 트랜잭션의 간섭이 가능
- 다만 이경우에도 Write(CUD) 행위에 대해서는 Exclusive lock(Write lock)이 걸리므로, CUD 트랜잭션중 CUD 간섭은 불가능
- uncommitted된 레코드도 읽기때문에 Dirty Read, Repeatable Read, Phantom Read가 발생함

#### InnoDB기준
- Read시엔 어떠한 lock을 사용하지않음

### level 2. read committed

#### 요건
- 커밋된 레코드만 읽는 격리 수준
- 트랜잭션 내에서 다른 트랜잭션이 레코드를 커밋하는 경우 간섭이 발생
- Dirty Read가 방지되나  Repeatable Read, Phantom Read는 발생가능

#### InnoDB기준
- Read시엔 어떠한 Lock을 사용하지 않음
- 쿼리시 매번 새로운 커밋 스냅샷을 조회하여 Dirty read 는 방지되지만, 다른 트랜잭션에 의해 커밋된경우 일관된 read는 보장하지 않음

### level 3. Repeatable Read

#### 요건
- 최초 read시 커밋형상을 스냅샷을 undo영역에 뜨고 해당 스냅샷만 트랜잭션이 끝날때까지 바라보게됨
- 다른트랜잭션으로 해당 레코드가 변경되더라도 일관된 read를 보장
- Phantom Read는 발생가능

#### InnoDB기준
- Read시엔 어떠한 Lock도 사용하지 않음
- 해당 코드에서는 아직까지 Phantom Read는 발생을 못시켜봄, 요건을 맞춰 실행했는데 발생안하는이유를 아직 모르겠다...
- undo에 스냅샷을 캐싱해야하므로 많은 Repeatable Read 트랜잭션은 DB에 부하를 가져올 위험이 존재


### level 4. Serializable

#### 요건
- 모든 행위가 순차적으로 이루어짐
- 단순 lock보다 강한행위로, select for update도 slock을 공유하나 serializable은 해당행위조차 lock을 잡게됨


## propagation

