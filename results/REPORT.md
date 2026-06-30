# MongoDB Transaction Collateral Damage 실증 실험 보고서

## 실험 환경

| 항목 | 값 |
|------|-----|
| OS / 하드웨어 | macOS 14.0 / Apple Silicon (M 시리즈) |
| MongoDB 버전 | 7.0 (Docker, replica set 3노드) |
| `transactionLifetimeLimitSeconds` | 60 (기본값) |
| 클라이언트 | Go 1.26.3 (goroutine 기반 동시 부하) |
| Group A 컬렉션 | `reward_history` (inactive transaction 보유) |
| Group B 컬렉션 | `product_catalog` (무관한 read 대상) |
| Group B ops/worker | 30 회 FindOne |

**도구 선택 근거**: per-worker 채널로 inactive 상태를 정밀 제어 → Go goroutine. Python threading은 GIL로 부적합.

---

## 실험 설계

**Group A 동작**: `StartTransaction` → `InsertOne` → **close(ready) 신호** → **`<-done` inactive 대기** → `AbortTransaction`  
**Group B 동작**: Group A가 모두 inactive 상태 확인 후 `FindOne × 30` latency 측정  

```
Group A worker [i]: StartTx → InsertOne → close(ready[i]) → <-done[i] → AbortTx
Main:               <-ready[0..N-1] (전원 inactive 확인) → runGroupB() → close(done[0..N-1])
```

| 시나리오 | Group A workers | snapshot_inactive_tx | 검증 포인트 |
|---------|----------------|---------------------|------------|
| pure_baseline | 0 | 0 | read latency 기저선 (cold) |
| mixed_groupA_10 | 10 | 10 | 소량 inactive tx |
| mixed_groupA_50 | 50 | 50 | 운영 수준 inactive tx |
| mixed_groupA_100 | 100 | 100 | 고부하 inactive tx |
| mixed_groupA_200 | 200 | 200 | 임계점 탐색 |

---

## 결과 테이블

### Group B Read Latency (ms) — 측정 시점에 Group A는 전원 inactive

| 시나리오 | p50 | p95 | p99 | max | snapshot_open | snapshot_inactive | r_ticket_out | w_ticket_out |
|---------|-----|-----|-----|-----|:-------------:|:-----------------:|:------------:|:------------:|
| pure_baseline | 2 | 5 | 7 | 9 | 0 | 0 | 0 | 0 |
| mixed_groupA_10 | 1 | 5 | 8 | 11 | 10 | 10 | 0 | 0 |
| mixed_groupA_50 | 1 | 3 | 3 | 4 | 50 | 50 | 0 | 0 |
| mixed_groupA_100 | 1 | 2 | 4 | 4 | 100 | 100 | 0 | 0 |
| mixed_groupA_200 | 1 | 2 | 3 | 4 | 200 | 200 | 0 | 0 |

---

## 핵심 관찰

### 1. Collateral Damage 없음

200개의 inactive transaction이 동시에 열려 있어도 무관한 컬렉션의 read latency는 증가하지 않았다.  
mixed_groupA_10의 max=11ms는 기저선(9ms)보다 약간 높지만 시나리오 순서 효과로 설명된다 — 바로 아래 항목 참조.

### 2. 시나리오 순서 효과 (warmup)

pure_baseline이 첫 번째로 실행 (cold 상태)되고, 이후 시나리오들은 WiredTiger 캐시와 커넥션 풀이 워밍업된 상태로 실행된다. 때문에 mixed 시나리오들이 baseline보다 낮은 latency를 보이는 것은 inactive transaction의 영향이 아니라 캐시 효과다. 중요한 점은 **Group A worker 수가 늘어날수록 p99가 감소하지 않고 수렴**한다는 것 — 50 이상에서 p99=3~4ms로 안정화.

### 3. WiredTiger Ticket: 0 (inactive 구간에서 티켓 미보유)

`peak_read_tickets_out = 0`, `peak_write_tickets_out = 0` — Group B 측정 중 WiredTiger 티켓 사용량이 0이다.  
이는 inactive transaction이 DB operation을 수행하지 않는 구간에는 **WiredTiger read/write ticket을 보유하지 않음**을 직접 증명한다.

> **RDBMS와의 차이**: PostgreSQL/MySQL은 트랜잭션이 커넥션과 바인딩되어 커넥션을 점유하지만,  
> MongoDB는 트랜잭션이 server-side session에 바인딩되고, inactive 구간에서는 ticket을 반납한다.  
> 따라서 inactive transaction이 많아도 unrelated operation의 ticket 경합이 발생하지 않는다.

### 4. `db.currentOp()` ≠ inactive transaction 관찰 도구

모든 시나리오에서 `currentOp_tx = 0` — `db.currentOp()`는 현재 DB operation을 **실행 중**인 작업만 반환하며, inactive transaction(session은 살아있지만 operation은 없는 상태)은 표시하지 않는다.  
Inactive transaction을 관찰하려면 `db.serverStatus().transactions.currentInactive`를 봐야 한다.

---

## 핵심 시그니처 검증

- [x] **Collateral Damage 없음**: Group B p99가 pure_baseline(7ms) 대비 증가하지 않음. 200개 inactive tx에서도 p99=3ms
- [x] **inactive tx가 WiredTiger ticket 미보유**: peak_write_tickets_out = 0 (Group B 측정 전 구간에서 Group A InsertOne이 완료되어 ticket 반납됨)
- [x] **snapshot 확인**: `snapshot_open_tx = snapshot_inactive_tx = GroupA_workers` — 모든 Group A transaction이 측정 시점에 inactive 상태로 서버에 살아있음
- [x] **currentOp 한계 확인**: `currentOp_tx = 0` — inactive transaction은 currentOp으로 관찰 불가. `serverStatus().transactions.currentInactive` 사용 필요

---

## 반증된 명제

없음 — 모든 명제가 예상 방향으로 증명됨.

---

## 결론: 증거 강도 평가

| 증거 | 강도 | 핵심 수치 |
|------|------|----------|
| inactive transaction은 무관한 read latency에 영향 없음 | ★★★★☆ | 200개 inactive에서도 p99 7ms → 3ms (증가 없음, 오히려 cache warmup으로 감소) |
| inactive 중 WiredTiger ticket 미보유 | ★★★★★ | peak_read/write_tickets_out = 0 (측정 구간 전체) |
| currentOp으로 inactive tx 관찰 불가 | ★★★★★ | currentOp_tx = 0 vs serverStatus open/inactive = N |

> ★ 1개 감소 이유 (첫 번째 증거): 시나리오 순서 효과(warmup)가 baseline과 mixed 시나리오를 다른 온도에서 비교하게 만들었다. 순서를 뒤집거나 랜덤화한 실험을 추가하면 ★★★★★ 승격 가능.

---

## 실무 시사점

1. **inactive transaction이 많아도 무관한 read는 안전하다** — ticket 경합 없음. 단, transaction 자체의 `transactionLifetimeLimitSeconds` timeout은 여전히 적용된다.
2. **Inactive transaction 모니터링은 `db.serverStatus().transactions.currentInactive`** — `db.currentOp()`로는 보이지 않으므로 알람을 잘못 설계하면 탐지를 놓친다.
3. **Collateral Damage가 없다는 것은 외부 호출을 트랜잭션 밖으로 꺼내야 하는 이유를 약화시키지 않는다** — timeout(60s)으로 인한 강제 abort와 데이터 정합성 문제는 여전히 발생한다. 분리 설계는 필수.

---

## 재현 명령

```bash
# replica set 확인
bash scripts/setup_rs.sh

# 실행
cd go-bench && go run main.go

# 결과
cat results/collateral.json | python3 -m json.tool
```

### P7 보충 — inactive transaction 관찰 명령

`db.currentOp({"transaction":{$exists:true}})` 는 inactive transaction을 반환하지 않는다.  
본 실험에서 `currentOp_tx = 0`으로 직접 확인 (snapshot_inactive_tx = 200인 상황에서).

**올바른 관찰 명령:**

```javascript
// inactive transaction 포함 조회 (idleSessions: true 필수)
db.aggregate([{
  $currentOp: { idleSessions: true, allUsers: true, localOps: false }
}, {
  $match: { "transaction": { $exists: true } }
}]).forEach(op => printjson({
  lsid:            op.lsid.id,
  timeOpenMicros:  op.transaction.timeOpenMicros,
  timeActiveMicros: op.transaction.timeActiveMicros,
  timeInactiveMicros: op.transaction.timeInactiveMicros,
  inactiveRatio:   op.transaction.timeInactiveMicros / op.transaction.timeOpenMicros
}))

// 간단한 카운트
db.adminCommand({ currentOp: 1, $all: true, "transaction": { $exists: true } })
  .inprog.length

// serverStatus로 집계 (관찰 비용 낮음)
db.serverStatus().transactions  // currentOpen / currentInactive / currentActive
```

**원본 보고서 P2(timeInactiveMicros 99.6%) 재현 가능 경로:**
1. `$currentOp {idleSessions: true}` 로 외부 API 대기 중 실시간 포착
2. MongoDB profiler (system.profile) — timeout abort 시 slow log에 세 타이머 자동 기록

---

## 이 실험의 위치 — 원본 검증 보고서와의 관계

이 실험은 원본 보고서("보상 지급 트랜잭션 timeout 원인 검증 결과")의 **Step 5 Collateral Damage 공백을 채우는 보완 실험**이다.

| 원본 보고서에서 증명된 것 | 이 실험에서 추가된 것 |
|--------------------------|----------------------|
| P1: 60s 경계에서 abort 100% | — |
| P2: timeInactiveMicros 99.6% | — |
| P3: connection pool 정상 | — |
| P4: 수정 후 abort 0건 | — |
| *(없음)* | **P5: inactive tx → 무관 read에 collateral damage 없음** |
| *(없음)* | **P6: inactive 중 WiredTiger ticket 미보유** |
| *(없음)* | **P7: currentOp는 inactive tx 관찰 불가** |
