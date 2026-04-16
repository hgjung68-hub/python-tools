import pyautogui
import time
import subprocess
import pygetwindow as gw
import sys
import os
import ctypes
from datetime import datetime

# ─────────────────────────────────────────────
# 1. 환경 설정 (DPI 보정 및 관리자 권한)
# ─────────────────────────────────────────────

try:
    # 125% 배율에서도 실제 1920x1080 좌표를 사용하도록 설정
    ctypes.windll.shcore.SetProcessDpiAwareness(1)
except Exception:
    ctypes.windll.user32.SetProcessDPIAware()

def set_input_block(block=True):
    """관리자 권한 실행 시 마우스/키보드 입력 차단"""
    return ctypes.windll.user32.BlockInput(block)

pyautogui.PAUSE = 1.0 
pyautogui.FAILSAFE = True 

def run_ktt_full_automation():
    exe_path = r"C:\Program Files (x86)\KTT_FPReader\CSNET20_HDS_DeviceControlKT.exe"
    screen_width, screen_height = pyautogui.size()

    # 좌표 비율 데이터 (천헤림 차장 PC & 모니터 좌표)
    RATIO_POINTS = {
        'ADMIN_CHK' : (0.5563, 0.4389),
        'LOGIN_OK'  : (0.5745, 0.4676),
        'READER_4F' : (0.3333, 0.3954),
        'READER_8F' : (0.3333, 0.4111),
        'SAVE_START': (0.5802, 0.5046),
        'CLOSE_BTN' : (0.6505, 0.6537),
        'EXIT_X'    : (0.7531, 0.1315)
    }

    def click_ratio(point_name):
        rx, ry = RATIO_POINTS[point_name]
        tx = round(screen_width * rx)
        ty = round(screen_height * ry)
        pyautogui.moveTo(tx, ty, duration=0.5)
        pyautogui.click()

    try:
        # A. 시작 안내 (입력 차단 전)
        pyautogui.alert(
            text=f"지문인식기 자동화를 시작합니다.\n\n"
                 f"해상도: {screen_width}x{screen_height}\n"
                 "확인을 누르면 약 1분 30초간 마우스/키보드가 차단됩니다.",
            title="TiSS 자동화 알림"
        )

        # B. 입력 차단 및 초기화
        set_input_block(True)
        print("[*] 입력 차단 활성화 및 기존 프로세스 정리...")
        subprocess.run(['taskkill', '/F', '/IM', 'CSNET20_HDS_DeviceControlKT.exe', '/T'], capture_output=True)
        time.sleep(2)

        # C. 프로그램 구동
        print(f"[*] 프로그램 구동: {exe_path}")
        subprocess.Popen(exe_path)
        
        # D. 창 탐색 로직 (대기 시간 강화)
        target_window = None
        search_keywords = ["관리자 인증", "관리자", "인증", "관리자 아이디", "관리자 비밀번호", "관리자 메뉴", "확 인"]
        
        for i in range(15): 
            all_titles = gw.getAllTitles()
            found = [t for t in all_titles if any(k.upper() in t.upper() for k in search_keywords if t.strip())]
            
            if found:
                print(f"[*] 창 발견: '{found[0]}'")
                target_window = gw.getWindowsWithTitle(found[0])[0]
                break
            time.sleep(1)

        if target_window:
            time.sleep(2)
            try:
                if target_window.isMinimized:
                    target_window.restore()
                target_window.activate()
                # 스크린샷의 로그인창은 작으므로 maximize()가 안 먹힐 수 있음
                # 대신 창이 활성화된 것만 확인하고 진행
                print("[*] 창 활성화 완료")
                time.sleep(2)
            except Exception as e:
                print(f"[*] 창 활성화 시도 중: {e}")
        else:
            set_input_block(False)
            # 디버깅을 위해 현재 시스템의 모든 창 제목을 출력
            print("\n[!] 현재 감지된 모든 창 목록:")
            for title in [t for t in gw.getAllTitles() if t.strip()]:
                print(f"    - {title}")
            pyautogui.alert(text="로그인 창('관리자 인증')을 찾지 못했습니다.", title="오류")
            return

        # 6. 로그인 및 수집 진행
        click_ratio('ADMIN_CHK')
        click_ratio('LOGIN_OK')
        
        print("[*] 메인 화면 대기 중...")
        time.sleep(8)

        click_ratio('READER_4F')
        pyautogui.keyDown('ctrl')
        click_ratio('READER_8F')
        pyautogui.keyUp('ctrl')
        time.sleep(1)

        click_ratio('SAVE_START')
        print("[*] DB 저장 중 (60초)...")
        time.sleep(60)

        # 8. 종료 처리
        click_ratio('CLOSE_BTN')
        time.sleep(3)
        click_ratio('EXIT_X')

    except Exception as e:
        set_input_block(False) # 에러 시에도 차단 해제 우선
        print(f"[!] 에러: {e}")
        pyautogui.alert(text=f"작업 중 에러가 발생했습니다:\n{e}", title="에러 알림")

    finally:
        set_input_block(False)
        pyautogui.alert(text="지문인식기 수집 작업이 모두 완료되었습니다.", title="완료")

if __name__ == "__main__":
    run_ktt_full_automation()