import os
import time
import shutil
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ==========================================
# [설정 부분] 
# ==========================================
DOWNLOAD_DIR = os.path.join(os.path.expanduser("~"), "Downloads")
TARGET_BASE_DIR = "D:/Dev/CISS/Contract_3"
LIST_URL = "https://tsline.daouoffice.com/gw/app/works/applet/49539/home/search"
# ==========================================

def run_auto_scan_mode():
    if not os.path.exists(TARGET_BASE_DIR): os.makedirs(TARGET_BASE_DIR)
    
    chrome_options = Options()
    chrome_options.add_experimental_option("detach", True)
    chrome_options.add_argument('force-device-scale-factor=0.75')
    chrome_options.add_argument('high-dpi-support=0.75')
    prefs = {
        "profile.default_content_settings.popups": 0,
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "profile.default_content_setting_values.automatic_downloads": 1
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.get(LIST_URL)
    driver.maximize_window() # 창 최대화 추가
    time.sleep(2) # 로그인 및 페이지 로딩 대기 시간 확보

    # 로그인 포커스 문제 해결 (아이디 칸 클릭)
    try:
        wait = WebDriverWait(driver, 7)
        user_id_field = wait.until(EC.element_to_be_clickable((By.ID, "username")))
        user_id_field.click() # 아이디 칸을 명시적으로 클릭하여 포커스 고정
        print(" ✅ 아이디 칸 포커스 완료")
    except: pass

    try:
        # 본문 아무곳이나 클릭하여 포커스 이동
        driver.find_element(By.TAG_NAME, "body").click()
        time.sleep(0.5)
        
        actions = ActionChains(driver)
        # Ctrl + '-' 를 3번 누름 (각 10%~15%씩 감소하여 약 75% 도달)
        for _ in range(3):
            actions.key_down(Keys.CONTROL).send_keys('-').key_up(Keys.CONTROL).perform()
            time.sleep(0.2)
        print("   🔍 화면 배율 축소 시도 완료 (Ctrl + -)")
    except Exception as e:
        print(f"   ⚠️ 단축키 축소 실패, JS 방식 시도: {e}")
        driver.execute_script("document.body.style.zoom='75%'")

    print("\n" + "="*70)
    print("🚀 다우오피스 Works 자동 수집 v6.0 (v5.7 기반 + 파일명 대조)")
    print("-" * 70)
    print("  1. 다우오피스 로그인을 완료하고 수집할 페이지로 이동하세요.")
    print("  2. 화면 사이즈는 자동으로 75% 축소 설정되었습니다.")
    print("  3. [Enter]: 분석 시작 / [q]: 종료")
    print("-" * 70)

    while True:
        user_input = input("\n👉 [Enter]: 현재 페이지 분석 / [q]: 종료 >> ")
        if user_input.lower() == 'q': break

        print("[*] 리스트 분석 중...")
        target_set = set()
        # 번호 수집 시에도 충분히 스크롤하여 누락 방지
        for _ in range(3):
            all_tds = driver.find_elements(By.TAG_NAME, "td")
            for td in all_tds:
                txt = td.text.strip()
                if txt.isdigit() and 0 < int(txt) < 3000:
                    target_set.add(int(txt))
            driver.execute_script("window.scrollBy(0, 1000);")
            time.sleep(1)
        
        target_list = sorted(list(target_set))
        driver.execute_script("window.scrollTo(0, 0);") 
        
        if not target_list:
            print("❌ 번호를 찾지 못했습니다."); continue

        total_count = len(target_list)
        print(f"✅ 분석 완료: {target_list[0]}번 ~ {target_list[-1]}번 (총 {total_count}건)")

        # [통계 변수 세분화]
        stats = {"success": 0, "already_done": 0, "no_contract_no": 0, "no_file": 0, "error": 0}
        CURRENT_PAGE_URL = driver.current_url

        for idx, current_no in enumerate(target_list, 1):
            target_str = str(current_no)
            print(f"\n[{idx}/{total_count}] --- [No. {target_str} 시작] ---")

            if "applet/49539" not in driver.current_url:
                driver.get(CURRENT_PAGE_URL); time.sleep(4)

            success_flag = False
            for attempt in range(2):
                try:
                    wait = WebDriverWait(driver, 12)
                    # 행 밀림 방지를 위해 td 태그로 더 명확히 지정
                    no_cell = wait.until(EC.presence_of_element_located((By.XPATH, f"//*[text()='{target_str}']")))

                    # 화면 중앙 정렬 및 안정화
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", no_cell)
                    time.sleep(1.5)

                    print(f"    🚀 상세 진입 시도 (No. {target_str})")
                    driver.execute_script("arguments[0].click();", no_cell)

                    # 진입 확인 대기
                    try:
                        WebDriverWait(driver, 5).until(lambda d: "doc" in d.current_url)
                        print(f"    ✅ 상세 진입 성공")
                        success_flag = True
                        break 
                    except:
                        # 클릭 반응 없을 시 v5.7의 ENTER 전송 로직
                        print(f"    ⚠️ 클릭 반응 없음, 강제 포커스 후 ENTER 시도")
                        actions = ActionChains(driver)
                        actions.move_to_element(no_cell).click().send_keys(Keys.ENTER).perform()
                        
                        time.sleep(2)
                        if "doc" in driver.current_url:
                            print(f"    ✅ ENTER로 진입 성공")
                            success_flag = True
                            break
                except Exception as e: 
                    print(f"    ⚠️ 진입 시도 중 오류 발생, 새로고침 후 재시도")
                    driver.refresh()
                    time.sleep(5)

            if not success_flag:
                print(f"    ❌ {target_str}번 최종 진입 실패"); stats["error"] += 1; continue

            try:
                time.sleep(2.5)
                driver.switch_to.default_content()
                iframes = driver.find_elements(By.TAG_NAME, "iframe")
                combined_source = driver.page_source
                for f in iframes:
                    try: 
                        driver.switch_to.frame(f) 
                        combined_source += driver.page_source
                        driver.switch_to.default_content()
                    except: driver.switch_to.default_content()
                
                match = re.search(r'\b(HT|MT|LT|ST|CT)\d+(?:-[0-9]+)?\b', combined_source, re.IGNORECASE)
                
                if match:
                    contract_no = match.group().upper()
                    print(f"    ✨ 인식: {contract_no}")
                    dest_path = os.path.join(TARGET_BASE_DIR, contract_no)
                    
                    # 내 컴퓨터에 이미 있는 파일 목록 미리 확인
                    # 1. 타겟 폴더 내 파일 목록 미리 읽기
                    existing_files = os.listdir(dest_path) if os.path.exists(dest_path) else []

                    before_files = set(os.listdir(DOWNLOAD_DIR))
                    total_clicked = 0
                    
                    for f in [None] + iframes:
                        try:
                            driver.switch_to.default_content()
                            if f: driver.switch_to.frame(f)
                            
                            btns = driver.find_elements(By.XPATH, "//a[contains(@class, 'btn_file_download')] | //span[text()='다운로드']/..")
                            if btns:
                                btns.sort(key=lambda x: x.location['y'])
                                top_y = btns[0].location['y']
                                for btn in btns:
                                    if abs(btn.location['y'] - top_y) < 150:
                                        try:
                                           # [1] 전체 텍스트 가져오기
                                            full_row_text = btn.find_element(By.XPATH, "./../..").text
                                            raw_line = full_row_text.split('\n')[0]
                                            
                                            # [2] 핀포인트 정제: "파일명"은 최대한 보존
                                            # (숫자.숫자KB/MB/GB) 형태의 괄호만 찾아서 제거
                                            # 예: "파일명 (1.2MB) 다운로드" -> "파일명 다운로드"
                                            cleaned_name = re.sub(r'\(\d+\.?\d*\s?(KB|MB|GB|B)\)', '', raw_line, flags=re.IGNORECASE)
                                            
                                            # [3] 버튼명만 제거 (파일명에 '다운로드'가 들어갈 일은 거의 없으므로)
                                            raw_web_name = cleaned_name.replace("AI요약", "").replace("미리보기", "").replace("다운로드", "").strip()
                                            
                                        except:
                                            raw_web_name = btn.get_attribute("title") or btn.text

                                        # [4] 대조용 키워드 (공백만 제거하여 비교)
                                        # 말줄임표(...)는 웹 표시 한계이므로 제거하고 비교
                                        clean_web = raw_web_name.replace("...", "").replace(" ", "").strip()
                                        
                                        # print(f"\n      🔍 [원본 유지 대조]")
                                        # print(f"         - 웹 추출명: '{raw_web_name}'")
                                        # print(f"         - 비교 키워드: '{clean_web}'")

                                        is_exist = False
                                        matched_local_name = ""

                                        # [5] 로컬 파일과 비교 (로컬 파일명은 절대 정제하지 않음)
                                        if clean_web:
                                            for local_f in existing_files:
                                                clean_local = local_f.replace(" ", "").strip()
                                                
                                                # 웹 파일명(clean_web)이 로컬 파일명에 포함되어 있는지 확인
                                                if clean_web in clean_local:
                                                    is_exist = True
                                                    matched_local_name = local_f
                                                    break
                                        
                                        if is_exist:
                                            print(f"      ✅ [PASS] '{matched_local_name}' 일치함")
                                            continue
                                        
                                        print(f"      🚀 [NEW] 다운로드 실행")
                                        driver.execute_script("arguments[0].click();", btn)
                                        total_clicked += 1; time.sleep(1.2)

                                if total_clicked > 0: break 
                        except: continue

                    if total_clicked > 0:
                        os.makedirs(dest_path, exist_ok=True)
                        for _ in range(25):
                            time.sleep(1)
                            new_files = list(set(os.listdir(DOWNLOAD_DIR)) - before_files)
                            for fname in new_files:
                                if not fname.endswith(('.crdownload', '.tmp')):
                                    time.sleep(0.5) # 파일 쓰기 완료 대기 시간 소폭 상향
                                    src = os.path.join(DOWNLOAD_DIR, fname)
                                    dst = os.path.join(dest_path, fname)
                                    # 중복 이동 방지 (이미 있으면 삭제 후 이동)
                                    if os.path.exists(dst): os.remove(dst)
                                    shutil.move(src, dst)
                                    print(f"       📥 완료: {fname}")
                                    before_files.add(fname)
                            if len(new_files) >= total_clicked: break
                        stats["success"] += 1
                    else:
                        # 클릭한 게 하나도 없으면 중복이거나 진짜 파일이 없는 것
                        if any(existing_files):
                            print(f"    ✅ 모든 파일이 이미 존재함"); stats["already_done"] += 1
                        else:
                            print(f"    ⚠️ 다운로드할 파일 없음"); stats["no_file"] += 1

                    # print(f"\n[테스트] {target_str}번 처리가 완료되었습니다.")
                    # input("👉 로그를 확인하신 후 [Enter]를 누르면 다음 번호로 진행합니다... (중단하려면 Ctrl+C)")

                else:
                    print(f"    ❌ 계약번호 미발견"); stats["no_contract_no"] += 1

                driver.get(CURRENT_PAGE_URL); time.sleep(3.5)
            except Exception as e:
                print(f"    ❌ 오류 발생: {str(e)[:50]}")
                stats["error"] += 1
                driver.get(CURRENT_PAGE_URL); time.sleep(4)

        # [통계 출력]
        print("\n" + "="*70)
        print(f"📊 최종 결과 보고")
        print(f"  - 전체 처리 대상 : {total_count}건")
        print(f"  - 수집 완료      : {stats['success']}건")
        print(f"  - 기존 중복      : {stats['already_done']}건 (파일 존재함)")
        print(f"  - 계약번호 미발견: {stats['no_contract_no']}건")
        print(f"  - 계약서 없음    : {stats['no_file']}건 (다운로드 버튼 없음)")
        print(f"  - 시스템 오류    : {stats['error']}건")
        print("="*70)
        
    driver.quit()

if __name__ == "__main__":
    run_auto_scan_mode()