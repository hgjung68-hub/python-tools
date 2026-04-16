import pyautogui
import keyboard  # 설치 필요: pip install keyboard
import ctypes
from datetime import datetime

# DPI 인식 설정 (실제 픽셀 좌표를 얻기 위함)
try:
    ctypes.windll.shcore.SetProcessDpiAwareness(1)
except Exception:
    ctypes.windll.user32.SetProcessDPIAware()

def get_ratio_coordinates():
    # 현재 모니터의 실제 해상도 가져오기
    screen_width, screen_height = pyautogui.size()
    
    print("==================================================")
    print(f" 현재 시스템 해상도: {screen_width} x {screen_height}")
    print("==================================================")
    print(" [사용 방법]")
    print(" 1. 좌표를 따고 싶은 버튼 위에 마우스를 올리세요.")
    print(" 2. 키보드의 'S' 키를 누르면 비율 좌표가 추출됩니다.")
    print(" 3. 종료하려면 'ESC' 키를 누르세요.")
    print("==================================================")

    while True:
        # ESC 누르면 종료
        if keyboard.is_pressed('esc'):
            print("\n[*] 좌표 추출을 종료합니다.")
            break

        # S 누르면 현재 좌표 출력
        if keyboard.is_pressed('s'):
            x, y = pyautogui.position()
            
            # 비율 계산 (소수점 4자리까지)
            rx = round(x / screen_width, 4)
            ry = round(y / screen_height, 4)
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 추출 성공!")
            print(f" -> 물리 좌표: ({x}, {y})")
            print(f" -> 비율 좌표: ({rx}, {ry})")
            print(f" -> 코드용: '이름': ({rx}, {ry}),")
            print("-" * 30)
            
            # 키 중복 입력 방지
            while keyboard.is_pressed('s'):
                pass

if __name__ == "__main__":
    try:
        get_ratio_coordinates()
    except KeyboardInterrupt:
        print("\n중단됨")