import meilisearch

# 1. Local PC 구동(26.2.27 생성) Search API Key
# SEARCH_API_KEY = '4aafffff0449b873a16ce046195dc4281e33a54668497cb36a06bcf56dfc5e4a'
# 2. docker 구동(26.3.18 생성) Search API Key
SEARCH_API_KEY = 'a3ddc02d006b1677c5f973f95d174ffd64486c1e9ae6ece629f078285611b6b2'
# 1. Meilisearch 연결 설정
client = meilisearch.Client('http://127.0.0.1:7700', SEARCH_API_KEY)

# 인덱스명을 오늘 생성한 'ciss_contracts'로
index = client.index('ciss_contracts')

def run_interactive_search():
    print("\n" + "="*60)
    print("🔎 CISS (Contract Integrated Search System) 통합 검색 엔진")
    print("   (종료하려면 'exit' 또는 'q'를 입력하세요)")
    print("="*60)

    while True:
        keyword = input("\n👉 검색할 계약 내용/단어를 입력하세요: ").strip()

        if keyword.lower() in ['exit', 'q', 'quit', '종료']:
            print("👋 검색 테스트를 종료합니다.")
            break

        if not keyword:
            print("⚠️ 검색어를 입력해 주세요.")
            continue

        # 검색 옵션 최적화
        search_options = {
            'attributesToHighlight': ['content_text'],
            'attributesToCrop': ['content_text'],
            'cropLength': 100,      # 문맥 가독성을 위해 조금 더 길게(100자) 설정
            'limit': 10             # 상위 10개 계약번호 노출
        }

        try:
            results = index.search(keyword, search_options)
        except Exception as e:
            print(f"❌ 검색 중 오류 발생 (인덱스 존재 여부 확인): {e}")
            continue

        # 결과 출력
        if not results['hits']:
            print(f"❌ '{keyword}' 단어가 포함된 계약서를 찾지 못했습니다.")
        else:
            print(f"✅ 총 {len(results['hits'])}건의 관련 계약을 찾았습니다.")
            print("-" * 60)

            for i, hit in enumerate(results['hits']):
                # 하이라이트 표시 및 필드명 변경 반영
                # Meilisearch는 강조할 단어를 <em>으로 감쌈
                raw_content = hit['_formatted']['content_text']
                highlighted_content = raw_content.replace('<em>', '\033[91m').replace('</em>', '\033[0m') # 터미널에서 빨간색 강조
                
                # 파일 리스트가 있다면 가독성 있게 출력
                file_info = ", ".join(hit.get('file_list', ['정보 없음']))

                print(f"[{i+1}] 계약번호: {hit['contract_no']}") # contract_id -> contract_no
                print(f"    포함 파일: {file_info}")
                print(f"    검색문맥: ...{highlighted_content}...")
                print("-" * 60)

if __name__ == "__main__":
    try:
        run_interactive_search()
    except Exception as e:
        print(f"❌ 시스템 오류: {e}")