import os

def restore_folder_names(base_dirs):
    """
    Ok-로 시작하는 폴더명에서 'Ok-'를 제거하여 원래의 계약번호로 복구합니다.
    """
    print("="*60)
    print("📂 CISS 폴더명 복구 도구 (Ok- 제거)")
    print("="*60)

    for base_dir in base_dirs:
        # 해당 경로가 실제로 존재하는지 확인
        if not os.path.exists(base_dir):
            print(f"⚠️  경로를 찾을 수 없음: {base_dir}")
            continue

        print(f"\n📁 작업 중인 상위 폴더: {base_dir}")
        count = 0

        # 폴더 내의 모든 항목을 확인
        for folder_name in os.listdir(base_dir):
            folder_path = os.path.join(base_dir, folder_name)

            # 폴더이면서 'Ok-'로 시작하는 경우
            if os.path.isdir(folder_path) and folder_name.startswith("Ok-"):
                # 새 이름 생성 (Ok- 이후의 문자열만 취함)
                new_name = folder_name.replace("Ok-", "", 1)
                new_path = os.path.join(base_dir, new_name)

                try:
                    # 폴더명 변경
                    os.rename(folder_path, new_path)
                    print(f"   ✅ [변경] {folder_name}  ->  {new_name}")
                    count += 1
                except Exception as e:
                    print(f"   ❌ [실패] {folder_name}: {e}")

        print(f"   ✨ {base_dir} 작업 완료: 총 {count}건 복구됨")

if __name__ == "__main__":
    # 작업할 대상 상위 폴더 리스트 (실제 경로에 맞춰 수정하세요)
    target_dirs = [
        "D:/CISS/Contract_1-(1-22)",
        "D:/CISS/Contract_2-(23-40)",
        "D:/CISS/Contract_3-(41-75)"
    ]

    restore_folder_names(target_dirs)
    print("\n" + "="*60)
    print("🎉 모든 복구 작업이 종료되었습니다.")