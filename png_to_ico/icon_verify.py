import struct

def verify_ico_detail(file_path):
    print(f"[{file_path}] 바이너리 정밀 분석 시작...")
    
    with open(file_path, "rb") as f:
        # 1. 헤더 읽기 (Reserved 2bytes, Type 2bytes, Count 2bytes)
        header = f.read(6)
        if len(header) < 6:
            print("파일이 너무 작습니다.")
            return

        reserved, resource_type, count = struct.unpack("<HHH", header)
        print(f"헤더 정보: 타입={resource_type}(1:ICO), 포함된 아이콘 수={count}개")
        print("-" * 50)

        # 2. 각 아이콘 디렉토리 엔트리 읽기 (개당 16바이트)
        for i in range(count):
            # Width, Height, ColorCount, Reserved, Planes, BitCount, BytesInRes, ImageOffset
            entry = f.read(16)
            data = struct.unpack("<BBBBHHII", entry)
            
            width = data[0] if data[0] > 0 else 256  # 0은 보통 256px을 의미함
            height = data[1] if data[1] > 0 else 256
            bit_count = data[5]
            image_size = data[6]
            
            print(f"아이콘 #{i+1}: 규격 {width}x{height} | {bit_count}bit | 데이터 크기: {image_size} bytes")

if __name__ == "__main__":
    verify_ico_detail("TS_ActiveScale.ico")