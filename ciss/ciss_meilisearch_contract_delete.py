import meilisearch
import time

# 관리자 API 키
# 1. Local PC 구동(26.2.27 생성) Admin API Key
# ADMIN_API_KEY = '095edf6d4d86a911ef7a33828f8369a86e962f528c9f1724ebc2729a85dda28b'
# 2. docker 구동(26.3.18 생성) Admin API Key
ADMIN_API_KEY = '66438e50779a8d2792f1f7e6c7ed70539dffb809d9e84fe09a000f1f8b7bbfb2'
client = meilisearch.Client('http://127.0.0.1:7700', ADMIN_API_KEY)
index = client.index('ciss_contracts')

# 삭제할 계약번호 리스트
target_contracts = ['MT25028', 'MT24073-1']

# 배치 삭제 실행
result = index.delete_documents(target_contracts)
print(f"🚀 삭제 요청이 서버에 전달되었습니다. (Task UID: {result.task_uid})")

# 사용자 대기 또는 시간 지연
print("\n[알림] Meilisearch가 내부적으로 삭제를 완료할 때까지 잠시 기다립니다.")
# 방법 A: 2초간 정지
# time.sleep(2) 
# 방법 B: 엔터 키 입력 대기 (수동 확인용)
input("계속하려면 Enter 키를 누르세요...")

# 삭제 확인 로직
print("🔍 삭제 여부 최종 확인 중...")
for contract_no in target_contracts:
    try:
        # 0.1초 정도의 아주 짧은 간격을 두며 하나씩 확인
        time.sleep(0.1)
        index.get_document(contract_no)
        print(f"⚠️ {contract_no}: 아직 데이터가 존재합니다. (삭제 중일 수 있음)")
    except:
        print(f"✨ {contract_no}: 삭제 완료 확인!")