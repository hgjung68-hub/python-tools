import os
import struct
from PIL import Image

def create_and_verify_final(input_png, output_ico):
    if not os.path.exists(input_png):
        print(f"[오류] 파일 없음: {input_png}")
        return

    try:
        # 1. 정사각형 보정 및 별도 저장
        raw_img = Image.open(input_png).convert('RGBA')
        width, height = raw_img.size
        square_size = max(width, height)
        
        # 투명 배경의 정사각형 생성
        square_img = Image.new('RGBA', (square_size, square_size), (0, 0, 0, 0))
        square_img.paste(raw_img, ((square_size - width) // 2, (square_size - height) // 2))
        
        # --- 추가된 부분: 보정 이미지 저장 ---
        name, ext = os.path.splitext(input_png)
        v1_filename = f"{name}-v1.png"
        square_img.save(v1_filename, "PNG")
        print(f"[1/3] 정사각형 보정 완료 및 저장: {v1_filename} ({square_size}x{square_size})")
        # ----------------------------------

        # 2. 멀티 사이즈 ICO 저장
        target_sizes = [(16, 16), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)]
        print(f"[2/3] 멀티 사이즈 ICO 저장 중...")
        square_img.save(output_ico, format='ICO', sizes=target_sizes)
        
        file_size = os.path.getsize(output_ico)
        print(f"[3/3] 저장 완료: {output_ico} ({file_size} bytes)")
        print("-" * 50)

        # 3. 바이너리 정밀 검증
        print("[검증 시작: 바이너리 분석]")
        with open(output_ico, "rb") as f:
            f.seek(4)
            count = struct.unpack("<H", f.read(2))[0]
            print(f"  - ICO 헤더에 기록된 실제 프레임 수: {count}개")
            
            for i in range(count):
                entry = f.read(16)
                if len(entry) < 16: break
                w, h = entry[0], entry[1]
                w = 256 if w == 0 else w
                h = 256 if h == 0 else h
                print(f"    - 프레임 {i+1}: {w}x{h}")

    except Exception as e:
        print(f"[오류 발생] {e}")

if __name__ == "__main__":
    # 사용하시는 원본 파일명
    create_and_verify_final("TS_ActiveScale_icon.png", "TS_ActiveScale.ico")