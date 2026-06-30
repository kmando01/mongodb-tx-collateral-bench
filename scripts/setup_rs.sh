#!/bin/bash
# 로컬 단일 노드 레플리카셋 초기화 (이미 replica set이 있으면 생략)

set -e

echo "=== MongoDB Replica Set 초기화 ==="
echo "이미 replica set이 구성된 운영 환경에서는 이 스크립트를 실행하지 마세요."
echo ""

# 현재 replica set 상태 확인
RS_STATUS=$(mongosh --quiet --eval "try { rs.status().ok } catch(e) { 0 }" 2>/dev/null)

if [ "$RS_STATUS" = "1" ]; then
    echo "Replica set이 이미 초기화되어 있습니다."
    mongosh --quiet --eval "rs.status().set"
    exit 0
fi

echo "Replica set 초기화 중..."
mongosh --eval 'rs.initiate({_id: "rs0", members: [{_id: 0, host: "localhost:27017"}]})'
echo "대기 중 (PRIMARY 선출 10s)..."
sleep 10
mongosh --eval 'rs.status()'
echo ""
echo "완료. mongoURI: mongodb://localhost:27017/?replicaSet=rs0"
