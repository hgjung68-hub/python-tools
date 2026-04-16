# ----------------------------------------------------------------------------------------------------------------------------
# Quantum ActiveScale Object Operations Tool (qas-objecttops)
# 최초 작성자 및 작성일 : 티에스라이시스템(주) 정현길, 2026.01.09
# 수정 : 2026.01.09, v1.0.0 - 기존 activescale_migrator와 activescale_explorer 통합 버전 출시
#       2026.01.10, v1.1.7 - 기능 안정화
#       2026.01.12, v1.3.2 - 기능 추가(데이터 전송후 target 조회 등) 및 개선(화면 좌/우 분할 50:50 등)
#                   v1.3.4 - Cloud 소스 목록에서도 멀티파일 선택 및 마우스 우클릭 선택으로 데이터 이전 가능
#                   v1.3.5 - 소스 선택이후 전송 예약시간 설정 기능 추가(설정이 없을 경우 즉시 전송 실행)
#       2026.01.13, v1.3.8 - 화면 UI 재정렬 및 개선
#                   v1.3.9 - 좌측 cloud Source 파일을 우측 Target으로 드래그 앤 드롭 기능 추가
#                   v1.4.0 - 전송 중단(cancel) 추가 및 안정화
#       2026.01.14, v1.4.8 - 파일 사이즈 표시를 Windows 탐색기 스타일로 (KB, 0byte = 0, 이상은 올림하여 정수 표현)
#                   v1.4.9 - source 및 target 목록 최대치를 0으로 입력할 경우 API 최대치 1,000으로 동작
#                   v1.5.0 - source가 공개 모드일 경우 access key, secret key가 없어도 처리되도록 수정
#       2026.01.15, v1.6.5 - 멀티 스레드 전송, Stream Copy 기능
#                            (Local -> QAS : Multipart Upload, Cloud -> QAS : 스토리지 서버 내부 복제)
#                   v1.9.9 - 멀티 스레드 전송, Stream Copy 기능 적용이후 Local 파일/폴더 전송 성공 버전
#                   v2.0.5 - 안정화 버전(Cloud S3 to ActiveScale, Treeview 로그 초기화, Source 마우스 우클릭 전송 등)
#       2026.01.16  v2.0.6 - 하단 푸터에 Help 버튼 추가
#       2026.01.21  v2.0.7 - 트리뷰 목록창 초기화 부분 보완(소스/타겟 환경 필드 수정이 있을 경우에 한하여 소스/타겟 해당부분만 초기화)
#       2026.01.22  v2.0.8 - 로컬 파일 또는 폴더 선택시 treeview창에 파일 목록 표시
#       2026.01.23  v2.0.9 - Cloud 데이터 전송시 동기화(Sync)모드, 전송 무결성 검증(Checksum) 기능 추가
#                            예외 처리 강화(error handling), 암호화 키 관리 개선 - 대외 배포시 적용, 대용량 목록 로딩 성능(Pagination)
#                            UI 반응성 개선(Debounce 로직), Endpoint URL 설정값 유효성 검사 추가(http:// or https:// 누락 시 자동 보정)
#       2026.01.24  v2.1.1 - 전송 최적화 설정 추가(멀티파트 임계값, 조각 크기, 병렬 워커 개수)
#       2026.01.25  v2.1.2 - 전송 최적화 설정 부분을 별도 팝업으로 분리
#       2026.01.27  v2.1.4 - UI 개선 및 Traget Default 설정 불러오기 개선, 운영 설정파일 저장/불러오기 암호화 강화
#       2026.01.28  v2.1.5 - Local / Cloud Put/Multipart 전송 정상화, 기타 오류 수정 안정화 버전
#       2026.01.30  v2.1.7 - Local 폴더/파일 선택 전송 시 Windows Defender 백신 검사하여 비감염일 경우 전송, 감염파일 있을 경우 경고, 전송작업 취소
#       2026.02.02  v2.1.8 - 백신 엔진 선택 가능 기능 추가(WIndows Defender, V3, 알약, 하우리), 
#                            Target 목록 파일 실행 시 임시 다운로드 폴더 변경(Windows 임시폴더 -> Windows 로그인 사용자 다운로드 폴더)
#                   v2.1.9 - 트리뷰 목록 [Ctrl + C], [Ctrl + A], 마우스 우클릭 [파일명 클립보드 복사] 기능 추가
#       2026.02.05  v2.2.0 - 기존에 로그가 3,000줄이 넘는 시점에 300줄씩 삭제하는 것을, 삭제되는 로그를 별도 로그 파일에 누적 저장 처리
# ----------------------------------------------------------------------------------------------------------------------------
# 함수 정의 및 배치순서
# 1. 클래스 초기화 및 속성 설정: __init__, _auto_load_sample_file, UI 호출
# 2. 백엔드 로직(S3 & 데이터 처리): create_s3_client, get_transfer_config, normalize_key
# 3. 보안 및 암호화: _get_fernet_key_fixed, _get_fernet_key, _validate_password, _get_device_id
# 4. 보안 검사(백신 스캔 로직): scan_defender_path, get_av_engine_path, check_av_environment, run_av_scan
# 5. 로깅 및 상태관리: log, _flush_logs, clear_log
# 6. UI 구성 및 메인 인터페이스: setup_ui, open_optimization_settings, center_sash, format_size_windows_kb, get_readable_size
# 7. 이벤트 핸들러 및 사용자 액션: on_source_change, on_entry_change, on_tree_click, open_help_pdf, browse_folder/file, start_transfer_thread 등
# ----------------------------------------------------------------------------------------------------------------------------

import os
import json  # 설정 데이터를 문자열(JSON)로 변환해 저장할 때 필수
import traceback 
import tkinter as tk
from tkinter import messagebox, ttk, scrolledtext, filedialog, simpledialog
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError, ConnectTimeoutError
from boto3.s3.transfer import TransferConfig
from concurrent.futures import ThreadPoolExecutor
import urllib3
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
import requests
import base64
import uuid
import tempfile
import subprocess
import math
import re
import io
import hashlib
import concurrent.futures  # 멀티스레딩을 위한 라이브러리 추가
import ctypes
from tkcalendar import Calendar
from pathlib import Path

# --- 암호화 관련 핵심 정의 ---
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend

# SSL 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class CancelCallback:
    def __init__(self, app_instance):
        self.app_instance = app_instance

    def __call__(self, bytes_amount):
        # 전송 청크가 넘어올 때마다 호출됨
        if getattr(self.app_instance, 'cancel_requested', False):
            # 이 예외가 발생하면 boto3의 upload_fileobj/download_fileobj가 즉시 중단됨
            raise Exception("User Cancel Requested")

class ActiveScaleObjectOperations:
    # ----------------------------------------------------
    # 1. 클래스 초기화 및 속성 설정
    # ----------------------------------------------------
    def __init__(self, root):
        self.root = root
        self.root.title("Quantum ActiveScale Object Storage Operations Tool (v2)")
        self.root.geometry("1500x780+0+0")   # 형식: 너비x높이+x좌표+y좌표
        self.root.resizable(True, True)      # 필요시 창 크기 조절 가능하게 허용
        
        self.CURSOR_HAND = "hand2"
        self.cloud_defaults = {
            "AWS S3": "https://s3.amazonaws.com",
            "NCP (Naver Cloud)": "https://kr.object.ncloudstorage.com",
            "KT Cloud": "https://s3.ktcloud.com",
            "Azure Blob": "https://<account>.blob.core.windows.net",
            "GCS (Google Cloud)": "https://storage.googleapis.com",
            "Kakao Cloud": "https://objectstore.kr-central-1.kakaocloud.com",
            "Samsung SDS (SCP)": "https://objectstorage.kr-1.samsungcloud.com",
            "NHN Cloud": "https://kr1-api-object-storage.nhncloudcod.com"
        }
        
        # 시스템 전용 고정 키 (샘플 파일 해독용)
        self._INTERNAL_FIXED_PW = "tslinedefaultconfig1234!"
        # 초기 값은 빈 딕셔너리로 설정
        self.default_config = {
            "Endpoint URL:": "",
            "Access Key:": "",
            "Secret Key:": "",
            "Target Bucket:": ""
        }
        # 프로그램 기동 시 자동으로 샘플 파일 로드 시도
        self._auto_load_sample_file()
        
        # 화면 제어용 변수 정의 (GUI 위젯과 연결)
        # 동기화 및 검증 모드 변수 정의
        self.use_av_scan = tk.BooleanVar(value=False)      # 기본값: 백신검사 미사용
        self.av_engine_var = tk.StringVar(value="Windows Defender")  # 기본값: Windows Defender 백신
        self.sync_mode_var = tk.BooleanVar(value=True)     # 기본값: 동기화 사용
        self.verify_mode_var = tk.BooleanVar(value=True)   # 기본값: 무결성 검증 사용
        # Entry 컨트롤을 위한 StringVar/IntVar 설정 (초기값 지정)
        self.threshold_var = tk.StringVar(value="100")   # Threshold 100MB
        self.chunk_var = tk.StringVar(value="20")        # Chunk Size 20MB
        self.worker_var = tk.StringVar(value="5")        # 병렬 워커 5개
        
        # 변경 비교를 위한 마지막 저장 상태 스냅샷 (last_config)
        # GUI 변수의 현재 값을 읽어와서 저장
        self.last_config = {
            "av": self.use_av_scan.get(),
            "av_engine": self.av_engine_var.get(),
            "sync": self.sync_mode_var.get(),
            "verify": self.verify_mode_var.get(),
            "threshold": self.threshold_var.get(),
            "chunk": self.chunk_var.get(),
            "worker": self.worker_var.get()
        }

        # 로그 제어를 위한 변수 설정
        self.log_queue = deque()        # 로그 메시지를 임시 보관하는 큐
        self.is_flushing = False        # UI 업데이트 예약 중복 방지 플래그
        self.log_update_ms = 150        # UI 갱신 주기 (150ms = 초당 약 7회)

        self.transfer_running = False   # 현재 전송이 진행 중인지 여부
        self.cancel_requested = False   # 사용자가 중단 버튼을 눌렀는지 여부

        # Windows Defender 실행 파일 경로 동적 로드
        self.defender_exe = self.scan_defender_path()

        self.setup_ui(self.root)
        self.on_source_change(None)
        self.apply_initial_config()     # 초기화 시 타겟 기본값 세팅

    # 기동 시 자동 실행되는 함수
    def _auto_load_sample_file(self):
        sample_path = "default_config.enc"
        if os.path.exists(sample_path):
            try:
                # 비밀번호 입력 없이 내부 키로 즉시 해독
                fernet = self._get_fernet_key_fixed()
                with open(sample_path, "rb") as f:
                    encrypted_data = f.read()
                    
                if not encrypted_data:
                    return  # 파일이 비어있으면 종료

                decrypted_data = fernet.decrypt(encrypted_data)
                
                # bytes를 문자열로 변환 후 JSON 파싱
                loaded_data = json.loads(decrypted_data.decode('utf-8'))
                
                # 불러온 데이터로 기본값 업데이트
                if isinstance(loaded_data, dict):
                    self.default_config.update(loaded_data)
                    
            except Exception as e:
                print(f"시스템: ActiveScale default 설정 로드 중 오류 발생: {e}")
                # default 설정 로드 실패 시에도 프로그램은 계속 진행됨
                pass
            
    # ----------------------------------------------------
    # 2. 백앤드 로직 (S3 & 데이터 처리)
    # ----------------------------------------------------
    def create_s3_client(self, info):
        """
        NCP 및 S3 호환 스토리지를 위한 클라이언트 생성 (인증 정보 처리 강화)
        """
        self.status_msg.config(text="⏳ S3 클라이언트 생성 및 인증 중...", fg="darkblue", font=('맑은 고딕', 9))
        
        url = info.get("Endpoint URL:", "").strip()
        ak = info.get("Access Key:", "").strip()
        sk = info.get("Secret Key:", "").strip()
        bucket = info.get("Target Bucket:", info.get("Source Bucket/Path:", "")).split('/')[0]

        # URL 유효성 기본 검사
        if not url.startswith(('http://', 'https://')):
            self.log("❌ 오류: Endpoint URL은 http:// 또는 https://로 시작해야 합니다.", force_now=True)
            self.root.after(0, lambda: self.status_msg.config(text="❌ URL 오류", fg="red"))
            self.root.after(5000, lambda: self.status_msg.config(text="준비 완료", fg="black"))
            return None
        
        # 공통 설정
        s3_config = Config(
            s3={'addressing_style': 'path'},  # ActiveScale/NCP 권장
            signature_version='s3v4',
            retries={'max_attempts': 3},   # 재시도 횟수
            connect_timeout=5,             # 연결 타임아웃 설정
            read_timeout=5                 # 읽기 시도
        )

        try:
            # AK/SK가 존재할 때만 인증 모드로 생성
            if ak and sk:
               # 인증 모드 클라이언트 생성
                client = boto3.client(
                    's3',
                    aws_access_key_id=ak,
                    aws_secret_access_key=sk,
                    endpoint_url=url,
                    config=s3_config,
                    verify=False       # SSL 인증서 무시 설정
                )
                # 실제 연결성 및 권한 검증 (HeadBucket 등 가벼운 호출)
                # client.list_buckets() 호출은 권한에 따라 거부될 수 있으므로 주의 필요
                self.log(f"🔄 {url} 연결 확인 중...", force_now=True)
                
            else:
                # 인증 정보가 부족할 때만 익명 모드로 시도
                self.log("🔓 인증 키 부족: Public(Anonymous) 모드 시도", force_now=True)
                client = boto3.client(
                    's3', 
                    endpoint_url=url, 
                    config=Config(signature_version=UNSIGNED), 
                    verify=False
                )

            # 로그가 화면에 즉시 나타나도록 강제 업데이트
            self.root.update_idletasks()

            # 실제 권한 검증 로직 (사전 검증)
            if bucket:
                # head_bucket은 실제 데이터를 받지 않고 권한만 체크하므로 매우 가벼움
                client.head_bucket(Bucket=bucket)
                self.log(f"✅ 버킷 연결 확인 성공: {bucket}", force_now=True)

            return client
        
        except (ConnectTimeoutError, EndpointConnectionError):
            msg = f"❌ 타겟 서버 접속 실패: {url} (네트워크 상태/URL 확인 필요)"
            self.log(msg, force_now=True)
            self.root.after(0, lambda: self.status_msg.config(text="❌ 접속 실패", fg="red"))
            self.root.after(5000, lambda: self.status_msg.config(text="준비 완료", fg="black"))
            raise Exception(msg) 
        except ClientError as e:
            err_code = e.response.get('Error', {}).get('Code', 'Unknown')
            msg = f"❌ S3 권한/버킷 오류: {err_code} (키 또는 버킷명 확인)"
            self.log(msg, force_now=True)
            self.root.after(0, lambda: self.status_msg.config(text="❌ 권한 오류", fg="red"))
            self.root.after(5000, lambda: self.status_msg.config(text="준비 완료", fg="black"))
            raise Exception(msg)
        except Exception as e:
            msg = f"❌ 연결 중 알 수 없는 오류: {str(e)}"
            self.log(msg, force_now=True)
            self.root.after(0, lambda: self.status_msg.config(text="❌ 오류 발생", fg="red"))
            self.root.after(5000, lambda: self.status_msg.config(text="준비 완료", fg="black"))
            raise Exception(msg)
            
    def get_transfer_config(self):
        """
        GUI 입력값을 읽어 boto3 전송 설정을 생성합니다.
        """
        try:
            # GUI Entry에서 값 읽기
            threshold = int(self.threshold_var.get()) * 1024 * 1024
            chunk_size = int(self.chunk_var.get()) * 1024 * 1024

        except Exception:
            # 에러 발생 시 기본값 반환(100MB, 20MB)
            threshold, chunk_size = 104857600, 20971520

        return TransferConfig(
            multipart_threshold=threshold,
            multipart_chunksize=chunk_size,
            max_concurrency=2,    # 파일 내 조각 병렬 전송(Concurrency)은 2로 제한
            use_threads=True      # 내부 스레드 사용 허용 (성능 최적화)
        )
    
    # --- Key 정규화 (Bucket 중복 제거) ---
    def normalize_key(self, bucket, key):
        """
        S3 HeadObject / CopyObject용 Key 정규화
        - Key에 Bucket명이 포함돼 있으면 제거
        """
        if not key:
            return key

        key = key.lstrip('/')

        if bucket and key.startswith(bucket + '/'):
            return key[len(bucket) + 1:]

        return key

    # ----------------------------------------------------
    # 3. 보안 및 암호화
    # ----------------------------------------------------
    # [내부 자동 로드용] 별도의 암호 입력 없이 시스템 키로 동작
    def _get_fernet_key_fixed(self):
        # 내부 고정 암호를 사용하므로 인자(password)가 필요 없음
        salt = b'tsline_salt_fixed' 
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(self._INTERNAL_FIXED_PW.encode()))
        return Fernet(key)
    
    # [운영용] 강화된 비밀번호 기반 방식 (사용자 설정 저장/불러오기용)
    def _get_fernet_key(self, password):
        # 기기 ID를 Salt에 섞지 않고 고유하지만 고정된 서비스 Salt 사용
        # 이렇게 해야 기기를 옮겨도 비번만으로 해독이 가능함
        salt = b'tsline_secure_service_fixed_salt' 
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=200000,  # 연산 횟수를 늘려 보안성 강화
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        return Fernet(key)
    
    # 기기 식별자 추출 (로그 기록 또는 참고용)
    def _get_device_id(self):
        """메인보드 UUID 추출 (실패 시 MAC 주소로 대체)"""
        try:
            # Windows 기준 메인보드 고유 ID
            cmd = 'wmi csproduct get uuid'
            uuid_val = subprocess.check_output(cmd, shell=True).decode().split('\n')[1].strip()
            return uuid_val
        except:
            return str(uuid.getnode())
        
    def _validate_password(self, pwd):
        """8자리 이상, 영문, 숫자, 특수문자 조합 검사"""
        if len(pwd) < 8: return False
        if not re.search(r"[a-zA-Z]", pwd): return False
        if not re.search(r"\d", pwd): return False
        special_chars = r"[!@#$%^&*()_+={}\[\]|\\:;\"'<>,.?/~`-]"
        if not re.search(special_chars, pwd): return False
        return True
    
    # ----------------------------------------------------
    # 4. 보안검사 (백신 스캔 로직)
    # ----------------------------------------------------
    # Windows Defender 백신 CLI경로
    def scan_defender_path(self):
        # Windows Defendr 백신 일반적인 경로들 확인
        paths = [
            r"C:\Program Files\Windows Defender\MpCmdRun.exe",
            r"C:\ProgramData\Microsoft\Windows Defender\Platform\*\MpCmdRun.exe"
        ]
        import glob
        found_paths = []
        for p in paths:
            found_paths.extend(glob.glob(p))

        if found_paths:
            # 버전 숫자가 포함된 경로가 있을 수 있으므로 정렬 후 가장 마지막 것 반환
            return sorted(found_paths)[-1]
                
        return r"C:\Program Files\Windows Defender\MpCmdRun.exe" # 기본값
    
    def get_av_engine_path(self):
        """설정된 백신 엔진에 따라 실행 파일 경로 반환"""
        engine = self.av_engine_var.get()

        if engine == "Windows Defender":
            return self.scan_defender_path()
            
        elif engine == "AhnLab V3":
            # 기업용 V3 및 개인용 V3의 일반적인 경로들
            v3_paths = [
                r"C:\Program Files\AhnLab\V3Plus\V3Scan.exe",
                r"C:\Program Files (x86)\AhnLab\V3Plus\V3Scan.exe",
                r"C:\Program Files\AhnLab\V3IS80\V3Scan.exe"
            ]
            for p in v3_paths:
                if os.path.exists(p): return p
            return ""   # 빈 문자열 반환하여 상위 함수에서 체크하게 함

        elif engine == "Alyac":
            alyac_path = r"C:\Program Files\ESTsoft\Alyac\AYScan.exe"
            return alyac_path if os.path.exists(alyac_path) else alyac_path
        
        elif engine == "ViRobot (Hauri)":
            # 하우리 콘솔 스캐너의 일반적인 경로
            hauri_paths = [
                r"C:\Program Files\Hauri\ViRobot\vrun.exe",
                r"C:\Program Files (x86)\Hauri\ViRobot\vrun.exe"
            ]
            for p in hauri_paths:
                if os.path.exists(p): return p
            return ""   # 빈 문자열 반환하여 상위 함수에서 체크하게 함

        return "" # 엔진 없음

    def check_av_environment(self):
        """백신 검사 실행 전 환경 체크 (관리자 권한 및 충돌 방지)"""
        if not self.use_av_scan.get():
            return True
        
        # 현재 선택된 엔진 이름 가져오기
        selected_engine = self.av_engine_var.get()

        # 관리자 권한 체크
        if not ctypes.windll.shell32.IsUserAnAdmin():
            messagebox.showwarning("관리자 권한 필요", 
                                   "백신 검사 기능을 사용하려면 프로그램을 '관리자 권한'으로\n"
                                   "실행해야 합니다. 백신 검사 사용 기능 OFF로 변경됩니다.", 
                                   parent=self.root)
            self.use_av_scan.set(False)
            self.av_engine_var.set("Windows Defender")
            self.last_config.update({"av": False, "av_engine": "Windows Defender"})
            return False

        # 엔진별 맞춤형 안내 메시지
        if selected_engine == "Windows Defender":
            msg = (f"🛡️ {selected_engine} 검사를 활성화합니다.\n\n"
                   "타사 백신(V3, 알약 등)이 실시간 감시 중일 경우\n"
                   "속도 저하나 중복 검사 오류가 발생할 수 있습니다.\n"
                   "계속하시겠습니까?\n"
                   "(아니오: 백신 검사 사용 기능 OFF로 변경됩니다.)")
        else:
            msg = (f"🛡️ {selected_engine} 검사를 활성화합니다.\n\n"
                   f"해당 백신 프로그램이 정상 설치되어 있어야 하며,\n"
                   "실행 중인 다른 보안 솔루션과 충돌이 없는지 확인해 주세요.\n"
                   "계속하시겠습니까?\n"
                   "(아니오: 백신 검사 사용 기능 OFF로 변경됩니다.)")

        if not messagebox.askyesno("백신 환경 확인", msg, parent=self.root):
            # 사용자가 거부 시 백신 체크박스를 해제
            self.use_av_scan.set(False)
            self.av_engine_var.set("Windows Defender")
            self.last_config.update({"av": False, "av_engine": "Windows Defender"})
            return False
        
        # 실제 실행 파일 존재 여부 확인
        engine_path = self.get_av_engine_path() # 앞서 만든 경로 탐색 함수
        if not os.path.exists(engine_path):
            messagebox.showerror(
                "백신 엔진 미검출", 
                f"선택하신 {selected_engine} 실행 파일을 찾을 수 없습니다.\n경로를 확인하거나 다른 백신 엔진을 선택해 주세요.",
                parent=self.root
            )
            self.use_av_scan.set(False)
            self.av_engine_var.set("Windows Defender")
            self.last_config.update({"av": False, "av_engine": "Windows Defender"})
            return False
    
        return True

    def run_av_scan(self, path):
        """전송 전 파일/폴더 스캔 (다중 엔진 지원)"""

        # 설정된 엔진 종류 및 실행 파일 경로 가져오기
        engine = self.av_engine_var.get()
        engine_path = self.get_av_engine_path()  # 정의해둔 경로 탐색 함수 호출

        cmd = []
        clean_code = 0     # 정상 종료(위협 없음) 코드
        infected_code = 1  # 위협 탐지 코드 (기본값)

        # 엔진별 실행 인자(Arguments) 및 리턴 코드 설정
        if engine == "Windows Defender":
            # Defender는 위협 탐지 시 리턴 코드가 2
            cmd = [engine_path, "-Scan", "-ScanType", "3", "-File", path]
            infected_code = 2

        elif engine == "AhnLab V3":
            # V3Scan.exe [경로] /S(하위포함) /A(모든파일) /I(감염시 알림만)
            cmd = [engine_path, path, "/S", "/A", "/I"]
            infected_code = 1

        elif engine == "Alyac":
            # AYScan.exe /F [경로]
            cmd = [engine_path, "/F", path]
            infected_code = 1

        elif engine == "ViRobot (Hauri)":
            # vrun.exe [경로] -s(하위포함) -a(모든파일) -e(치료하지 않고 검사만)
            cmd = [engine_path, path, "-s", "-a", "-e"]
            clean_code = 0
            # 하우리는 발견 시 보통 리턴 코드 1 또는 발견 개수를 반환합니다.
            infected_code = 1

        # 엔진 파일 존재 여부 최종 확인
        if not engine_path or not os.path.exists(engine_path):
            self.root.after(0, lambda: self.log(f"⚠ [Vaccine] {engine} 엔진을 찾을 수 없어 검사 생략: {os.path.basename(path)}", force_now=True))
            return True

        # 스캔 실행
        try:
            # 경로 정규화 (역슬래시 등 처리)
            path = os.path.normpath(path)
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                creationflags=subprocess.CREATE_NO_WINDOW,
                timeout=600  # 10분
            )

            # 엔진별 결과 판독 로직 분기
            ret = result.returncode

            if ret == 0:   # 클린
                return True

            # Defender는 2가 탐지, V3/알약/하우리는 1 이상이 탐지
            if engine == "Windows Defender":
                if ret == 2:  # Defender 공식 위협 발견 코드
                    self.root.after(0, lambda: self.log(f"🚨 [Vaccine] 위협 탐지({engine}): {os.path.basename(path)}", force_now=True))
                    return False
                else: # 오류 코드 (1 등)
                    self.root.after(0, lambda: self.log(f"⚠ [Vaccine] {engine} 보고 (Code {ret}). 안전을 위해 중단합니다.", force_now=True))
                    return False
            else:
                if ret >= 1: # V3, 알약, 하우리 탐지
                    self.root.after(0, lambda: self.log(f"🚨 [Vaccine] 위협 탐지({engine}): 발견됨 (Code {ret})", force_now=True))
                    return False

            return True 
            
        except subprocess.TimeoutExpired:
            self.root.after(0, lambda: self.log(f"⏰ [Vaccine] {engine} 검사 시간 초과(10분)", force_now=True))
            return False # 시간 초과 시에도 안전을 위해 False 반환 권장
        except Exception as e:
            self.root.after(0, lambda: self.log(f"❌ [Vaccine] {engine} 예외 발생: {str(e)}", force_now=True))
            return False

    # ----------------------------------------------------
    # 5. 로깅 및 상태 관리
    # ----------------------------------------------------
    def _write_to_temp_log(self, text_to_save):
        """삭제되는 로그를 임시 파일에 누적 저장 및 파일 분할(Rotation)"""
        try:
            # 최초 삭제 시점에 파일명 결정 (프로그램 기동 중 유지)
            if not hasattr(self, 'log_base_name') or self.log_base_name is None:
                current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
                # 기본 이름 저장 (예: backup_log_20260205_1540)
                self.log_base_name = f"backup_log_{current_time}"
                self.log_file_index = 0      # 파일 번호 (0부터 시작)
                self.current_file_lines = 0  # 현재 파일의 라인 수

                # 최초 파일명 설정
                self.temp_log_path = f"{self.log_base_name}.txt"
                # 최초 생성 로그 출력 (무한 루프 방지를 위해 force_now=True 필수)
                self.log(f"시스템: 오래된 로그가 삭제전 파일에 백업됩니다. ({self.temp_log_path})", force_now=True)

            # 파일 분할 기준 체크 (2만 라인마다 새로운 파일 생성)
            # text_to_save가 몇 줄인지 계산
            added_lines = text_to_save.count('\n')

            if self.current_file_lines + added_lines > 20000:
                self.log_file_index += 1
                # 새 파일명: 기본이름_1.txt, 기본이름_2.txt ...
                self.temp_log_path = f"{self.log_base_name}_{self.log_file_index}.txt"
                self.current_file_lines = 0
                self.log(f"시스템: 백업 로그 파일 용량 초과(2만 라인)로 새 파일에 저장합니다. ({self.temp_log_path})", force_now=True)

            # 파일에 누적 기록 (utf-8 권장)
            with open(self.temp_log_path, "a", encoding="utf-8") as f:
                f.write(text_to_save)

            # 현재 라인 수 업데이트
            self.current_file_lines += added_lines

        except Exception as e:
            print(f"Temp Log Write Error: {e}")

    def _flush_logs(self):
        """큐에 쌓인 로그를 배치(Batch)로 UI에 한꺼번에 반영"""
        try:
            # 큐가 비었더라도 상태 해제는 반드시 수행해야 함
            if not hasattr(self, 'log_queue') or not self.log_queue:
                self.is_flushing = False
                return

            # 큐 비우기 및 결합
            combined_logs = "".join(self.log_queue)
            self.log_queue.clear()

            # UI 작업 시작
            self.log_area.configure(state='normal')
            self.log_area.insert(tk.END, combined_logs)

            # 메모리 보호: 3000줄 초과 시 오래된 로그 300줄 삭제
            # 한 줄씩 지우는 것보다 뭉텅이(300줄)로 지우는 것이 빠름
            line_count = int(self.log_area.index('end-1c').split('.')[0])
            if line_count > 3000:
                # 삭제될 범위(1줄부터 300줄까지)의 텍스트를 가져옴
                deleted_text = self.log_area.get('1.0', '301.0')
                # 파일에 저장
                self._write_to_temp_log(deleted_text)
                # UI에서 삭제(상단 300줄)
                self.log_area.delete('1.0', '301.0')

            self.log_area.see(tk.END)
            self.log_area.configure(state='disabled')

        except Exception as e:
            print(f"Log Flush Error: {e}")
        finally:
            self.is_flushing = False  # 작업 완료 후 다시 로그를 받을 수 있는 상태로 변경

    def log(self, msg, force_now=False):
        """
        로그 출력: 쓰로틀링(Throttling) 적용 및 메모리 보호 기능 통합
        로그 출력: force_now=True 시 즉시 출력 및 GUI 갱신
        """
        # 위젯 존재 여부 확인 (기존 안전장치 유지)
        if not hasattr(self, 'log_area') or self.log_area is None:
            return

        try:
            # 메시지 생성
            time_str = datetime.now().strftime('%H:%M:%S')
            full_msg = f"[{time_str}] {msg}\n"

            if force_now:
                # 즉시 UI에 삽입
                self.log_area.config(state="normal")
                self.log_area.insert(tk.END, full_msg)
            
                # 메모리 보호 (너무 많으면 위쪽 삭제)
                current_lines = int(self.log_area.index('end-1c').split('.')[0])
                if current_lines > 3000:
                    # 삭제될 범위(1줄부터 300줄까지)의 텍스트를 가져옴
                    deleted_text = self.log_area.get('1.0', '301.0')
                    # 파일에 저장
                    self._write_to_temp_log(deleted_text)
                    # UI에서 삭제(상단 300줄)
                    self.log_area.delete('1.0', '301.0')
                
                self.log_area.see(tk.END)
                self.log_area.config(state="disabled")
            
                # GUI 강제 갱신 (대기 상태에서도 로그가 보이게 함)
                self.root.update_idletasks()
            else:
                # 쓰로틀링 큐에 저장
                # 백그라운드 스레드에서 안전하게 실행 가능
                if not hasattr(self, 'log_queue'):
                    self.log_queue = deque()
                self.log_queue.append(full_msg)

                # UI 업데이트 예약 (없으면 생성)
                if not getattr(self, 'is_flushing', False):
                    self.is_flushing = True
                    ms = getattr(self, 'log_update_ms', 150)
                    self.root.after(ms, self._flush_logs)

        except Exception as e:
            print(f"Log Input Error: {e}")

    def clear_log(self):
        """로그 화면과 대기열을 모두 비우는 공통 함수"""
        try:
            # 아직 출력되지 않고 대기 중인 로그 큐 비우기
            self.log_queue.clear()
            self.is_flushing = False
            
            # UI 위젯 내용 삭제
            self.log_area.configure(state='normal')
            self.log_area.delete(1.0, tk.END)
            
            # 다시 읽기 전용으로 설정
            self.log_area.configure(state='disabled')
            
            # 화면 즉시 갱신 (사용자 경험 개선)
            self.root.update_idletasks()
            
        except Exception as e:
            # 로깅 시스템 자체의 오류이므로 print 또는 다른 방식으로 알림
            print(f"Clear Log Error: {e}")

    # ----------------------------------------------------
    # 6. UI 구성 및 메인 인터페이스
    # ----------------------------------------------------
    # =================================================================================================================    
    def setup_ui(self, root):
        self.v_paned = tk.PanedWindow(self.root, orient="vertical", sashrelief="raised", sashwidth=4)
        self.v_paned.pack(fill="both", expand=True, padx=10, pady=5)

        self.top_work_area = tk.Frame(self.v_paned)
        # stretch="always"로 설정하여 화면을 키울 때 목록창이 커지게 함
        self.v_paned.add(self.top_work_area, stretch="always", minsize=350)

        self.main_h_paned = tk.PanedWindow(self.top_work_area, orient="horizontal", sashrelief="raised", sashwidth=6)
        self.main_h_paned.pack(fill="both", expand=True)

        # ================= [좌측: SOURCE 영역] ========================================================================
        self.src_container = tk.Frame(self.main_h_paned)
        self.main_h_paned.add(self.src_container)
        
        src_lf = tk.LabelFrame(self.src_container, text=" 1. 원본 데이터 (Source) ", fg="darkgreen", 
                               font=('맑은 고딕', 10, 'bold'), padx=10, pady=5)
        src_lf.pack(fill="both", expand=True, padx=5, pady=5)

        src_input_frame = tk.Frame(src_lf)
        src_input_frame.pack(fill="x", pady=5)
        src_input_frame.columnconfigure(2, weight=1)

        tk.Label(src_input_frame, text="Source Type:", width=15, anchor="e").grid(row=0, column=0, pady=2)

        # 프레임을 먼저 생성 (columnspan=2로 우측 끝까지 확장)
        src_type_inner_frame = tk.Frame(src_input_frame)
        src_type_inner_frame.grid(row=0, column=1, columnspan=2, sticky="ew", padx=0)
        # 콤보박스 배치
        self.src_type_var = tk.StringVar(value="Local File System")
        self.src_combo = ttk.Combobox(src_type_inner_frame, textvariable=self.src_type_var, 
                                     values=["Local File System"] + list(self.cloud_defaults.keys()), 
                                     state="readonly", width=46)
        self.src_combo.pack(side="left", padx=5)

        self.btn_src_folder = tk.Button(src_type_inner_frame, text="📂폴더", 
                                        command=self.browse_folder, width=5, cursor=self.CURSOR_HAND)
        self.btn_src_folder.pack(side="left", padx=(7, 2))
        
        self.btn_src_file = tk.Button(src_type_inner_frame, text="📄파일", 
                                      command=self.browse_file, width=5, cursor=self.CURSOR_HAND)
        self.btn_src_file.pack(side="left", padx=2)

        # Source Type 옆 우측 끝에 전송 최적화 설정 버튼 배치
        self.btn_opt_settings = tk.Button(src_type_inner_frame, text="⚙️ 전송 설정", 
                                          command=self.open_optimization_settings,
                                          fg="#546e7a",                # 실버/그레이 톤
                                          font=('맑은 고딕', 9, 'bold'),  # 굵게 처리
                                          bg="#f8f9fa",                 
                                          cursor=self.CURSOR_HAND,
                                          relief="raised",               # 입체감 있는 버튼 형태
                                          borderwidth=1,                 # 테두리 두께
                                          highlightthickness=1, 
                                          highlightbackground="#cfd8dc",
                                          width=12)
        self.btn_opt_settings.pack(side="right", padx=10)

        # 마우스 호버 시에만 살짝 푸른 빛
        self.btn_opt_settings.bind("<Enter>", lambda e: self.btn_opt_settings.config(bg="#e3f2fd"))
        self.btn_opt_settings.bind("<Leave>", lambda e: self.btn_opt_settings.config(bg="#f8f9fa"))

        # 소스type이 변경될 경우 소스 트리뷰 목록, 로그 clear
        # self.src_combo.bind("<<ComboboxSelected>>", lambda e: [self.on_source_change(e), self.clear_source_treeview()])
        self.src_combo.bind("<<ComboboxSelected>>", lambda e: [
            self.on_source_change(e), 
            self.clear_source_treeview(),
            self.clear_log()   # 소스 타입 변경시에만 전체 로그 초기화
        ])

        self.src_entries = {}
        src_fields = [("Endpoint URL:", 1), ("Access Key:", 2), ("Secret Key:", 3), ("Source Bucket/Path:", 4)]
        for txt, r in src_fields:
            tk.Label(src_input_frame, text=txt, width=15, anchor="e").grid(row=r, column=0, pady=2)
            ent = tk.Entry(src_input_frame, width=48)
            if "Secret" in txt: ent.config(show="*")
            ent.grid(row=r, column=1, padx=5, sticky="w")
            self.src_entries[txt] = ent
        
        self.src_ep_widget = self.src_entries["Endpoint URL:"]
        self.path_entry_widget = self.src_entries["Source Bucket/Path:"]

        src_btn_sub = tk.Frame(src_input_frame)
        src_btn_sub.grid(row=1, column=2, rowspan=4, padx=10, sticky="nw")
        self.btn_src_load = tk.Button(src_btn_sub, text="🔒정보 불러오기", 
                                      command=lambda: self.load_config("source"), 
                                      width=12, 
                                      bg="#e1f5fe", 
                                      fg="#01579b", 
                                      cursor=self.CURSOR_HAND)
        self.btn_src_load.pack(pady=2)
        self.btn_src_save = tk.Button(src_btn_sub, text="💾정보 저장하기", 
                                      command=lambda: self.save_config("source"), 
                                      width=12, 
                                      bg="#e8f5e9", 
                                      fg="#1b5e20",
                                      cursor=self.CURSOR_HAND)
        self.btn_src_save.pack(pady=2)

        src_path_btns = tk.Frame(src_input_frame)
        src_path_btns.grid(row=4, column=2, rowspan=4, padx=10, sticky="ew")

        # 전송 중단 버튼 추가
        spacer = tk.Label(src_path_btns)
        spacer.pack(side="left", expand=True, fill="x")
        self.stop_btn = tk.Button(src_path_btns, text="⏹ 전송 중단", 
                                  command=self.cancel_transfer, 
                                  width=12, 
                                  cursor="arrow", 
                                  bg="#f0f0f0",   # 비활성 배경
                                  fg="#a0a0a0",   # 비활성 글자색
                                  font=('맑은 고딕', 9, 'bold'), 
                                  state="disabled", 
                                  relief="raised",    # 비활성 시엔 평평하게 
                                  borderwidth=1)
        self.stop_btn.pack(side="right", padx=(5, 0), pady=2)
        
        # [필터/최대개수/목록조회 복원]
        src_ctrl = tk.Frame(src_lf)
        src_ctrl.pack(fill="x", pady=5)
        tk.Label(src_ctrl, text="필터:").pack(side="left", padx=2)
        self.src_filter = tk.Entry(src_ctrl, width=12); self.src_filter.pack(side="left", padx=2)
        tk.Label(src_ctrl, text="최대:").pack(side="left", padx=2)
        self.src_max = tk.Entry(src_ctrl, width=5); self.src_max.insert(0, "100"); self.src_max.pack(side="left", padx=2)

        tk.Button(src_ctrl, text="🔍조회", 
                  command=lambda: self.check_bucket_files("source"), 
                  bg="#e1f5fe", 
                  cursor=self.CURSOR_HAND).pack(side="left", padx=5)
        
        # 예약 전송 영역 (LabelFrame으로 감싸서 구분)
        # "예약전송" 글자 바로 옆에 날짜부터 순서대로 배치
        tk.Label(src_ctrl, text="   예약전송:", font=('맑은 고딕', 9, 'bold')).pack(side="left", padx=2)

        # 날짜 입력 (부모를 src_ctrl로 변경)
        self.sched_date = tk.Entry(src_ctrl, width=9)
        self.sched_date.insert(0, datetime.now().strftime("%Y%m%d"))
        self.sched_date.pack(side="left", padx=1)

        # 달력 버튼
        self.btn_cal = tk.Button(src_ctrl, text="📅", 
                                 command=self.open_calendar, 
                                 relief="flat", 
                                 cursor=self.CURSOR_HAND)
        self.btn_cal.pack(side="left", padx=1)

        # 오전/오후
        self.sched_ampm = ttk.Combobox(src_ctrl, values=["AM", "PM"], width=4, state="readonly")
        self.sched_ampm.set(datetime.now().strftime("%p"))
        self.sched_ampm.pack(side="left", padx=1)

        # 시/분
        self.sched_hour = ttk.Combobox(src_ctrl, values=[str(i) for i in range(1, 13)], width=3, state="readonly")
        self.sched_hour.set(datetime.now().strftime("%I").lstrip('0'))
        self.sched_hour.pack(side="left", padx=1)

        self.sched_min = ttk.Combobox(src_ctrl, values=[f"{i:02d}" for i in range(60)], width=3, state="readonly")
        self.sched_min.set(datetime.now().strftime("%M"))
        self.sched_min.pack(side="left", padx=1)

        self.start_btn = tk.Button(src_ctrl, text="데이터 전송 ▶▶", 
                                   command=self.start_transfer_thread, 
                                   bg="#005a9e", 
                                   fg="white", 
                                   font=('맑은 고딕', 9, 'bold'), 
                                   cursor=self.CURSOR_HAND)
        self.start_btn.pack(side="right")

        style = ttk.Style()
        style.theme_use('clam')
        # 헤더(Heading) 스타일: 연한 하늘색 배경, 굵은 글씨, 회색 테두리
        style.configure("Treeview.Heading",
                        background="#D9EAF7",     # 연한 하늘색
                        foreground="#333333",     # 진한 회색 글자
                        font=('맑은 고딕', 9, 'bold'),
                        relief="flat")

        # 데이터 행(Row) 스타일: 행 높이 조절
        style.configure("Treeview", rowheight=25)

        # 트리뷰 (목록 표시)
        self.src_tree_frame = tk.Frame(src_lf)
        self.src_tree_frame.pack(fill="both", expand=True, padx=2, pady=2)

        # Treeview 생성 (부모 프레임은 self.src_tree_frame)
        self.src_tree = ttk.Treeview(self.src_tree_frame, columns=("Name", "Size", "Date"), 
                                     show="headings", selectmode='extended')

        # Source 트리뷰에 우클릭 바인딩(Windows/Linux: <Button-3>)
        self.src_tree.bind("<Button-3>", self.show_src_context_menu)
        
        # 빈 공간 클릭 시 모든 선택 해제 (좌측)
        self.src_tree.bind("<Button-1>", lambda e: self.on_tree_click(e, "source"))

        # ESC 키 입력 시 선택 해제
        self.src_tree.bind("<Escape>", lambda e: self.src_tree.selection_remove(self.src_tree.selection()))

        # Ctrl+C 단축키 바인딩
        self.src_tree.bind("<Control-c>", lambda e: self.copy_treeview_filename(tree_widget=self.src_tree))
        self.src_tree.bind("<Control-C>", lambda e: self.copy_treeview_filename(tree_widget=self.src_tree))

        # 전체 선택 단축키 (Ctrl+A)
        self.src_tree.bind("<Control-a>", lambda e: self.src_tree.selection_set(self.src_tree.get_children()))
        self.src_tree.bind("<Control-A>", lambda e: self.src_tree.selection_set(self.src_tree.get_children()))

        # 줄무늬(Striped Rows)를 위한 태그 설정
        self.src_tree.tag_configure('oddrow', background='#F2F2F2') # 홀수행 연한 회색
        self.src_tree.tag_configure('evenrow', background='white')    # 짝수행 흰색

        # 컬럼 설정
        self.src_tree.heading("Name", text="경로/파일명"); self.src_tree.column("Name", width=350, anchor="w")  # 왼쪽 정렬
        self.src_tree.heading("Size", text="크기"); self.src_tree.column("Size", width=80, anchor="e")  # 숫자는 오른쪽 정렬
        self.src_tree.heading("Date", text="수정일(KST)"); self.src_tree.column("Date", width=120, anchor="center", stretch=True) # 남는 공간 채우기

        # 스크롤바 생성 및 연결
        src_scrollbar = ttk.Scrollbar(self.src_tree_frame, orient="vertical", command=self.src_tree.yview)
        self.src_tree.configure(yscrollcommand=src_scrollbar.set)
        
        # 배치 (Treeview는 왼쪽, 스크롤바는 오른쪽)
        self.src_tree.pack(side="left", fill="both", expand=True)
        src_scrollbar.pack(side="right", fill="y")

        # ================= [우측] TARGET AREA (Source와 대칭형) =======================================================
        self.tgt_container = tk.Frame(self.main_h_paned)
        self.main_h_paned.add(self.tgt_container)
        
        # Target 영역의 그룹 박스 생성
        tgt_lf = tk.LabelFrame(self.tgt_container, text=" 2. 대상 ActiveScale (Target) ", 
                              fg="blue", font=('맑은 고딕', 10, 'bold'), padx=10, pady=5)
        tgt_lf.pack(fill="both", expand=True, padx=5, pady=5)

        # 입력창을 담을 프레임 (부모를 tgt_lf 설정)
        tgt_input_frame = tk.Frame(tgt_lf)
        tgt_input_frame.pack(fill="x", padx=10, pady=(10, 5))

        tk.Label(tgt_input_frame, text="", height=1).grid(row=0, column=0)

        self.tgt_entries = {}
        # 라벨과 입력창을 같은 Row에 배치 (Source와 동일한 스타일)
        tgt_fields = [("Endpoint URL:", 1), ("Access Key:", 2), ("Secret Key:", 3), ("Target Bucket:", 4)]
        
        for txt, r in tgt_fields:
            # 라벨 배치
            tk.Label(tgt_input_frame, text=txt, width=15, anchor="e").grid(row=r, column=0, pady=2)
            ent = tk.Entry(tgt_input_frame, width=48)
            if "Secret" in txt: ent.config(show="*")
            ent.grid(row=r, column=1, padx=5, sticky="w")
            self.tgt_entries[txt] = ent
        
        # 버튼 영역 (로드/저장 버튼을 입력창 우측에 배치)
        tgt_btn_sub = tk.Frame(tgt_input_frame)
        tgt_btn_sub.grid(row=1, column=2, rowspan=4, padx=10, pady=(2, 0), sticky="nw") 

        self.btn_tgt_load = tk.Button(tgt_btn_sub, text="🔒정보 불러오기", 
                                      command=lambda: self.load_config("target"),
                                      state='normal',  # 상태 항상 활성화 명시
                                      width=12, 
                                      bg="#e1f5fe", 
                                      fg="#01579b", 
                                      cursor=self.CURSOR_HAND)
        self.btn_tgt_load.pack(pady=2)

        self.btn_tgt_save = tk.Button(tgt_btn_sub, text="💾정보 저장하기", 
                                      command=lambda: self.save_config("target"), 
                                      state='normal',  # 상태 항상 활성화 명시
                                      width=12, 
                                      bg="#e8f5e9", 
                                      fg="#1b5e20", 
                                      cursor=self.CURSOR_HAND)
        self.btn_tgt_save.pack(pady=2)

        # 컨트롤 바 (필터/조회/다운로드 등)
        tgt_ctrl = tk.Frame(tgt_lf)
        tgt_ctrl.pack(fill="x", pady=(8, 5))
        
        tk.Label(tgt_ctrl, text="필터:").pack(side="left", padx=2)
        self.tgt_filter = tk.Entry(tgt_ctrl, width=12); self.tgt_filter.pack(side="left", padx=2)
        tk.Label(tgt_ctrl, text="최대:").pack(side="left", padx=2)
        self.tgt_max = tk.Entry(tgt_ctrl, width=5); self.tgt_max.insert(0, "100"); self.tgt_max.pack(side="left", padx=2)
        
        BTN_WIDTH = 12
        tk.Button(tgt_ctrl, text="🔍파일 조회", command=self.refresh_list_thread, 
                  bg="#e1f5fe", width=BTN_WIDTH, cursor=self.CURSOR_HAND).pack(side="left", padx=2)
        tk.Button(tgt_ctrl, text="📥선택 다운로드", command=self.download_file, 
                  bg="#f0f4c3", width=BTN_WIDTH, cursor=self.CURSOR_HAND).pack(side="left", padx=2)
        tk.Button(tgt_ctrl, text="⚡선택 실행", command=self.run_file, 
                  bg="#ffecb3", width=BTN_WIDTH, cursor=self.CURSOR_HAND).pack(side="left", padx=2)
        tk.Button(tgt_ctrl, text="🗑선택 삭제", command=self.delete_selected_files, 
                  fg="red", width=BTN_WIDTH, cursor=self.CURSOR_HAND).pack(side="right", padx=2)

        # 우클릭 메뉴(Context Menu) 생성
        self.context_menu = tk.Menu(self.root, tearoff=0)
        self.context_menu.add_command(label="📥 다운로드", command=self.download_file)
        self.context_menu.add_separator()  # 구분선
        self.context_menu.add_command(label="⚡ 실행", command=self.run_file)
        self.context_menu.add_separator()  # 구분선
        self.context_menu.add_command(label="📋 파일명 클립보드 복사", 
                              command=lambda: self.copy_treeview_filename(tree_widget=self.tgt_tree))
        self.context_menu.add_separator()  # 구분선
        self.context_menu.add_command(label="🗑 삭제", command=self.delete_selected_files, foreground="red")

        # 트리뷰 (목록 표시)
        self.tgt_tree_frame = tk.Frame(tgt_lf)
        self.tgt_tree_frame.pack(fill="both", expand=True, padx=2, pady=2)
        
        self.tgt_tree = ttk.Treeview(self.tgt_tree_frame, columns=("Name", "Size", "Date"), show="headings")

        # 타겟 트리뷰에 우클릭 바인딩
        self.tgt_tree.bind("<Button-3>", self.show_context_menu) # Windows

        # 우측 타겟 트리뷰 더블 클릭 이벤트 바인딩
        self.tgt_tree.bind("<Double-1>", self.run_file)

        # 빈 공간 클릭 시 모든 선택 해제 (우측)
        self.tgt_tree.bind("<Button-1>", lambda e: self.on_tree_click(e, "target"))

        # ESC 키 입력 시 선택 해제
        self.tgt_tree.bind("<Escape>", lambda e: self.tgt_tree.selection_remove(self.tgt_tree.selection()))

        # Ctrl+C 단축키 바인딩
        self.tgt_tree.bind("<Control-c>", lambda e: self.copy_treeview_filename(tree_widget=self.tgt_tree))
        self.tgt_tree.bind("<Control-C>", lambda e: self.copy_treeview_filename(tree_widget=self.tgt_tree))

        # 전체 선택 단축키 (Ctrl+A)
        self.tgt_tree.bind("<Control-a>", lambda e: self.tgt_tree.selection_set(self.tgt_tree.get_children()))
        self.tgt_tree.bind("<Control-A>", lambda e: self.tgt_tree.selection_set(self.tgt_tree.get_children()))

        # 줄무늬(Striped Rows)를 위한 태그 설정
        self.tgt_tree.tag_configure('oddrow', background='#F2F2F2')
        self.tgt_tree.tag_configure('evenrow', background='white')

        # 컬럼 설정
        self.tgt_tree.heading("Name", text="파일명"); self.tgt_tree.column("Name", width=350, anchor="w") # 왼쪽 정렬
        self.tgt_tree.heading("Size", text="크기"); self.tgt_tree.column("Size", width=80, anchor="e")    # 숫자는 오른쪽 정렬
        self.tgt_tree.heading("Date", text="수정일(KST)"); self.tgt_tree.column("Date", width=120, anchor="center", stretch=True)
        
        # 스크롤바 생성 및 연결
        tgt_scrollbar = ttk.Scrollbar(self.tgt_tree_frame, orient="vertical", command=self.tgt_tree.yview)
        self.tgt_tree.configure(yscrollcommand=tgt_scrollbar.set)
        
        # 배치 (Treeview는 왼쪽, 스크롤바는 오른쪽)
        self.tgt_tree.pack(side="left", fill="both", expand=True)
        tgt_scrollbar.pack(side="right", fill="y")

        # 하단 로그 영역
        self.bottom_area = tk.Frame(self.v_paned)
        # stretch="never"로 설정하고 초기 높이를 작게 부여
        self.v_paned.add(self.bottom_area, stretch="never", height=100)

        # log_area를 bottom_area에 pack할 때 fill과 expand를 확실히 부여
        self.log_area = scrolledtext.ScrolledText(self.bottom_area, font=('Consolas', 9), bg="white")
        self.log_area.pack(fill="both", expand=True) 

        # UI 생성 직후 로그가 찍히는지 확인
        self.root.after(0, lambda: self.log("🚀 프로그램이 시작되었습니다."))

        # 하단 상태바 (Progress + Info)
        self.status_bar = tk.Frame(self.root, relief="sunken", bd=1)
        self.status_bar.pack(side="top", fill="x") # pack 순서상 아래쪽에 배치됨

        self.progress = ttk.Progressbar(self.status_bar, orient="horizontal", mode="determinate", length=750)
        self.progress.pack(side="left", padx=10, pady=3)
        self.status_msg = tk.Label(self.status_bar, text="준비 완료", anchor="w", font=('맑은 고딕', 9))
        self.status_msg.pack(side="left", fill="x")

        # 로그 저장 버튼 (우측 끝 배치)
        self.save_log_btn = tk.Button(self.status_bar, text="💾 로그 파일로 저장", 
                                      command=self.manual_save_log,
                                      bg="#ffffff",            # 순백색
                                      fg="#333333",            # 진회색
                                      font=('맑은 고딕', 8, 'bold'), 
                                      relief="raised", borderwidth=1, cursor=self.CURSOR_HAND)
        self.save_log_btn.pack(side="right", padx=10)

        # 하단 푸터 (Version / Copyright / Help)
        # 상단의 연한 하늘색/그레이 느낌을 주기 위해 밝은 고스트 화이트/스틸 블루 계열 사용
        footer_bg = "#F0F4F9"  # 윈도우 11 표준 앱 배경에 가까운 연한 하늘색 톤
        footer_text = "#444444" # 텍스트는 진한 그레이로 가독성 확보
        self.footer_frame = tk.Frame(self.root, bg=footer_bg, bd=1, relief="sunken")  # 하단 경계선 부여
        self.footer_frame.pack(side="bottom", fill="x")

        # 버전 정보 ((글자색 흰색)
        tk.Label(self.footer_frame, text="Version 2.2.0(Build: 2026-02-06)", 
                bg=footer_bg, fg=footer_text, 
                font=('맑은 고딕', 8)).pack(side="left", padx=15, pady=5)

        # Help 버튼 (푸터 우측) - 버튼도 시스템 기본 스타일로
        tk.Button(self.footer_frame, text="❓ Help (Manual)", command=self.open_help_pdf,
                  bg="#FFFFFF", 
                  fg="#1A73E8", # 구글/윈도우 표준 블루 글씨
                  activebackground="#E8F0FE",
                  font=('맑은 고딕', 8, 'bold'), 
                  relief="groove", 
                  borderwidth=1, 
                  cursor=self.CURSOR_HAND, padx=8).pack(side="right", padx=10, pady=2)

        # 카피라이트 (글자색 연한 회색)
        tk.Label(self.footer_frame, text="Copyright 2026 TSLINE SYSTEM All rights reserved.", 
                bg=footer_bg, 
                fg="#666666",   
                font=('맑은 고딕', 8)).pack(side="right", padx=10)
        
        # 소스 엔트리 값 변경 감지 (Source 정보 수정 시)
        for label, entry in self.src_entries.items():
            # 초기값 저장 (프로그램 시작 시)
            entry.old_value = entry.get()

            # 포커스가 들어올 때 현재 값을 갱신하여 저장 (비교 기준점)
            entry.bind("<FocusIn>", lambda e: setattr(e.widget, 'old_value', e.widget.get()))

            # 엔트리에서 포커스가 빠질 때(FocusOut)나 엔터를 쳤을 때(Return) 초기화
            entry.bind("<FocusOut>", lambda e: self.on_entry_change(e, "source"))
            entry.bind("<Return>", lambda e: self.on_entry_change(e, "source"))

        # 타겟 엔트리 값 변경 감지 (Target 정보 수정 시)
        for label, entry in self.tgt_entries.items():
            # 초기값 저장 (프로그램 시작 시)
            entry.old_value = entry.get()

            # 포커스가 들어올 때 현재 값을 갱신하여 저장 (비교 기준점)
            entry.bind("<FocusIn>", lambda e: setattr(e.widget, 'old_value', e.widget.get()))

            # 엔트리에서 포커스가 빠질 때(FocusOut)나 엔터를 쳤을 때(Return) 초기화
            entry.bind("<FocusOut>", lambda e: self.on_entry_change(e, "target"))
            entry.bind("<Return>", lambda e: self.on_entry_change(e, "target"))
            
        # 소스 타입(콤보박스) 변경 감지
        # 콤보박스 변수명이 src_type_combo라고 가정할 때
        if hasattr(self, 'src_type_combo'):
            # self.src_type_combo.bind("<<ComboboxSelected>>", lambda e: self.clear_source_treeview())
            self.src_type_combo.bind("<<ComboboxSelected>>", self.on_entry_change(e, "source"))

        # 창 크기가 변할 때(전체화면 포함) 실행될 이벤트 바인딩
        # event.widget == self.root 체크를 통해 메인 창이 변할 때만 작동
        self.root.bind("<Configure>", lambda e: self.center_sash() if e.widget == self.root else None)

        # 초기 실행 시에도 중앙을 잡도록 예약
        self.root.after(300, self.center_sash)
        
    def open_optimization_settings(self):
        """전송 최적화 설정을 위한 별도 팝업 창 (v2.1.8 백신 검증 강화)"""
        settings_win = tk.Toplevel(self.root)
        settings_win.title("⚙️ 전송 최적화 설정")
        settings_win.geometry("420x520")
        settings_win.resizable(False, False)
        settings_win.grab_set()  # 팝업 우선권 설정

        main_frame = tk.Frame(settings_win, padx=25, pady=20)
        main_frame.pack(fill="both", expand=True)

        # --- 상태 표시 라벨 (동적 경고용) ---
        self.av_status_msg = tk.StringVar()
        status_label = tk.Label(main_frame, textvariable=self.av_status_msg, 
                                font=('맑은 고딕', 9, 'bold'), wraplength=340, justify="left")
        
        def update_av_ui_state(*args):
            """백신 체크박스나 엔진 선택이 바뀔 때 실시간으로 상태를 검증"""
            if not self.use_av_scan.get():
                self.av_status_msg.set("")
                return
            
            selected_engine = self.av_engine_var.get()
            # 관리자 권한 확인 (ctypes 필요)
            is_admin = ctypes.windll.shell32.IsUserAnAdmin()
      
            if not is_admin:
                status_label.config(fg="#e74c3c") # Red
                self.av_status_msg.set("⚠️ 권한 경고: 백신 기능을 사용하려면 프로그램을\n'관리자 권한'으로 다시 실행해야 합니다.")
            else:
                engine_path = self.get_av_engine_path()
                if not engine_path or not os.path.exists(engine_path):
                    status_label.config(fg="#e67e22") # Orange
                    self.av_status_msg.set(f"❌ 엔진 미검출: {selected_engine}이 설치되어 있지 않거나\n경로가 올바르지 않습니다.")
                else:
                    status_label.config(fg="#27ae60") # Green
                    self.av_status_msg.set(f"✅ 확인: {selected_engine} 엔진으로 검사 준비 완료")

        # 실시간 감시(Trace) 등록
        trace_id_av = self.use_av_scan.trace_add("write", update_av_ui_state)
        trace_id_engine = self.av_engine_var.trace_add("write", update_av_ui_state)

        # 전송 모드 설정 영역
        tk.Label(main_frame, text="1. 전송 모드 설정", font=('맑은 고딕', 10, 'bold')).pack(anchor="w")
        tk.Checkbutton(main_frame, text="Local 파일 전송 전 백신 검사 사용", variable=self.use_av_scan).pack(anchor="w", padx=10)
        # --- 백신 엔진 선택 영역 ---
        av_select_frame = tk.Frame(main_frame)
        av_select_frame.pack(fill="x", padx=30, pady=(0, 5))
        tk.Label(av_select_frame, text="└ 백신 엔진 선택:", font=('맑은 고딕', 9)).pack(side=tk.LEFT)
        
        # 백신 엔진 선택 콤보박스 (AhnLab V3 등 추가 가능)
        av_engines = ["Windows Defender", "AhnLab V3", "Alyac", "ViRobot (Hauri)"]
        # self.av_engine_var 는 __init__ 에서 미리 정의(예: self.av_engine_var = tk.StringVar(value="Windows Defender"))
        self.av_combo = ttk.Combobox(av_select_frame, textvariable=self.av_engine_var, values=av_engines, state="readonly", width=17)
        self.av_combo.pack(side=tk.LEFT, padx=5)

        # 동적 상태 라벨 배치 (백신 설정 바로 아래)
        status_label.pack(anchor="w", padx=30, pady=(0, 10))
        # -----------------------------
        tk.Checkbutton(main_frame, text="동기화(Sync) 모드 사용", variable=self.sync_mode_var).pack(anchor="w", padx=10)
        tk.Checkbutton(main_frame, text="무결성 검증(Checksum) 사용", variable=self.verify_mode_var).pack(anchor="w", padx=10)
        
        tk.Frame(main_frame, height=1, bg="#dddddd").pack(fill="x", pady=15)

        # 세부 파라미터 설정 영역 (Grid Layout)
        tk.Label(main_frame, text="2. 세부 파라미터 설정", font=('맑은 고딕', 10, 'bold')).pack(anchor="w")
        
        grid_f = tk.Frame(main_frame)
        grid_f.pack(fill="x", pady=10)

        # 레이블과 연결할 변수 리스트
        fields = [
            ("Multipart Threshold (MB):", self.threshold_var),
            ("Multipart Chunk Size (MB):", self.chunk_var),
            ("병렬 워커 수 (2~30):", self.worker_var)
        ]

        for i, (label_text, var) in enumerate(fields):
            tk.Label(grid_f, text=label_text).grid(row=i, column=0, sticky="e", pady=5)
            # textvariable 설정을 통해 __init__의 초기값이 자동으로 표시
            ent = tk.Entry(grid_f, width=12, textvariable=var, justify='center')
            ent.grid(row=i, column=1, sticky="w", padx=15)

        # --- 설정 가이드 레이블 ---
        guide_text = (
            "💡 가이드: 저사양 1~5 / 일반(PC, 1Gbps) 8~10\n" 
            "고성능(고사양 워크스테이션, 10Gbps) 20 이상 권장\n"
            "※ 워커 수가 과도하면 시스템 부하가 증가할 수 있습니다."
        )
        tk.Label(main_frame, text=guide_text, font=('맑은 고딕', 8), 
                 fg="#666666", justify="left", wraplength=320).pack(pady=(0, 10))

        def save_and_close():
            # 중복 실행 방지를 위해 프로토콜 해제
            settings_win.protocol("WM_DELETE_WINDOW", lambda: None)

            # trace 해제 시 에러 방지 (try-except로 감싸는 것이 안전)
            try:
                self.use_av_scan.trace_remove("write", trace_id_av)
                self.use_av_scan.trace_remove("write", trace_id_engine)
            except: pass

            # 현재 화면의 값들
            curr = {
                "av": self.use_av_scan.get(),
                "av_engine": self.av_engine_var.get(),
                "sync": self.sync_mode_var.get(),
                "verify": self.verify_mode_var.get(),
                "threshold": self.threshold_var.get(),
                "chunk": self.chunk_var.get(),
                "worker": self.worker_var.get()
            }

            # 직전 값(last_config)과 하나라도 다르면 로그 출력
            if curr != self.last_config:
                # 백신 정보 문자열 구성 (예: [Vaccine: ON (V3)])
                a_status = f"ON ({curr['av_engine']})" if curr["av"] else "OFF"
                s_str = "ON" if curr["sync"] else "OFF"
                v_str = "ON" if curr["verify"] else "OFF"
                self.log(f"⚙️ 설정 변경됨: [Vaccine:{a_status}, Sync:{s_str}, Verify:{v_str}] "
                         f"Threshold {curr['threshold']}MB / Chunk {curr['chunk']}MB / Workers {curr['worker']}개")
                
                # 비교가 끝났으므로 현재 값을 마지막 상태로 업데이트
                self.last_config = curr.copy()
            else:
                self.log("ℹ️ 설정 변경 사항 없습니다.")
        
            settings_win.destroy()

        # 취소/닫기 시에도 trace 제거를 위해 protocol 연결
        settings_win.protocol("WM_DELETE_WINDOW", save_and_close)

        # 적용 버튼
        tk.Button(main_frame, text="설정 완료", command=save_and_close, 
                  bg="#005a9e", fg="white", font=('맑은 고딕', 9, 'bold'), 
                  cursor=self.CURSOR_HAND, height=2).pack(fill="x", pady=(20, 0))
        
        # 최초 실행 시 상태 업데이트
        update_av_ui_state()

    # 좌, 우 화면 분할을 50:50 중앙 정렬
    def center_sash(self):
        try:
            # 윈도우의 현재 렌더링 상태를 강제로 업데이트하여 정확한 너비를 가져옴
            self.root.update_idletasks()

            # PanedWindow의 현재 실제 렌더링 너비를 가져옴
            total_width = self.main_h_paned.winfo_width()
            
            # 너비가 정상적으로 계산되었을 때만 실행
            if total_width > 100:
                # 정확한 절반 지점 계산 (소수점 버림)
                half_point = int(total_width / 2)
                
                # 0번 구분선(첫 번째 구분선)을 중앙 x좌표로 이동
                #sash_place(index, x, y)
                self.main_h_paned.sash_place(0, half_point, 0)
        except Exception as e:
            # 초기 로딩 시 발생할 수 있는 오류 방지
            pass
    
    def format_size_windows_kb(self, size_bytes):
        """
        Windows 탐색기 스타일 사이즈 표시:
        1. 0 Byte는 0 KB
        2. 0 Byte 초과는 올림하여 정수 KB로 표시
        """
        if size_bytes <= 0:
            return "0 KB"
        
        # 1~1024바이트는 1KB로, 그 이상은 1024로 나눈 뒤 소수점 무조건 올림
        size_kb = math.ceil(size_bytes / 1024)
                           
        return f"{size_kb:,} KB"  # 천 단위 콤마 추가
    
    def get_readable_size(self, size_bytes):
        if size_bytes == 0: return "0 B"
        # 천 단위 콤마 추가한 바이트 수
        comma_size = f"{size_bytes:,} B"
        # 읽기 편한 단위 변환
        units = ("B", "KB", "MB", "GB", "TB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{comma_size} ({s} {units[i]})"
    
    # ----------------------------------------------------
    # 7. 이벤트 핸들러 및 사용자 액션
    # ----------------------------------------------------
    def on_source_change(self, e):
        # 현재 선택된 소스 타입 가져오기
        selected_type = self.src_type_var.get()
        is_local = "Local" in selected_type

        # 모든 입력 필드(src_entries)를 순회하며 초기화
        for k, v in self.src_entries.items():
            # 값을 수정하기 위해 상태를 일시적으로 normal로 변경
            v.config(state="normal")
            v.delete(0, tk.END)
            
            if is_local:
                # [Local일 경우] 
                # 'Source Bucket/Path:'는 로컬 경로 입력용이므로 활성화 유지, 나머지는 비활성화
                if k != "Source Bucket/Path:":
                    v.config(state="disabled")
                else:
                    v.config(state="normal")
            else:
                # [Cloud일 경우] 
                # Endpoint URL 필드에만 초기 선언된 URL 대입
                if k == "Endpoint URL:":
                    default_url = self.cloud_defaults.get(selected_type, "")
                    v.insert(0, default_url)
                
                # Cloud 모드이므로 모든 입력창을 다시 활성화
                v.config(state="normal")

        # 버튼 상태 설정 (로컬일 때 파일/폴더 선택 활성화, 로드/저장 비활성화)
        COLOR_LOAD_ACTIVE = "#E1F5FE"
        COLOR_LOAD_FG = "#01579B"
        COLOR_SAVE_ACTIVE = "#E8F5E9"
        COLOR_SAVE_FG = "#1B5E20"
        COLOR_FILE_FOLDER_ACTIVE = "#f0f0f0"
        COLOR_DISABLED_BG = "#F0F0F0"
        COLOR_DISABLED_FG = "#A0A0A0"
        # 커서 정의
        CURSOR_ACTIVE = self.CURSOR_HAND  # "hand2"
        CURSOR_DISABLED = "arrow"         # 기본 화살표

        if selected_type == "Local File System":
            # --- [Local 모드] 파일/폴더 선택 활성화 ---
            self.btn_src_file.config(state="normal", bg=COLOR_FILE_FOLDER_ACTIVE, fg="black", cursor=CURSOR_ACTIVE)
            self.btn_src_folder.config(state="normal", bg=COLOR_FILE_FOLDER_ACTIVE, fg="black", cursor=CURSOR_ACTIVE)

            # --- 소스(좌측) 버튼 비활성화: 회색 톤으로 변경 ---
            self.btn_src_load.config(state="disabled", bg=COLOR_DISABLED_BG, fg=COLOR_DISABLED_FG, cursor=CURSOR_DISABLED)
            self.btn_src_save.config(state="disabled", bg=COLOR_DISABLED_BG, fg=COLOR_DISABLED_FG, cursor=CURSOR_DISABLED)
        else:
            # --- [Cloud 모드] 파일/폴더 선택 비활성화 ---
            self.btn_src_file.config(state="disabled", bg=COLOR_DISABLED_BG, fg=COLOR_DISABLED_FG, cursor=CURSOR_DISABLED)
            self.btn_src_folder.config(state="disabled", bg=COLOR_DISABLED_BG, fg=COLOR_DISABLED_FG, cursor=CURSOR_DISABLED)

            # --- 소스(좌측) 버튼 활성화: 원래의 생생한 색상 복구 ---
            self.btn_src_load.config(state="normal", bg=COLOR_LOAD_ACTIVE, fg=COLOR_LOAD_FG, cursor=CURSOR_ACTIVE)
            self.btn_src_save.config(state="normal", bg=COLOR_SAVE_ACTIVE, fg=COLOR_SAVE_FG, cursor=CURSOR_ACTIVE)

        # --- 타겟(우측) 버튼은 항상 활성화 색상 유지 ---
        self.btn_tgt_load.config(state="normal", bg=COLOR_LOAD_ACTIVE, fg=COLOR_LOAD_FG, cursor=CURSOR_ACTIVE)
        self.btn_tgt_save.config(state="normal", bg=COLOR_SAVE_ACTIVE, fg=COLOR_SAVE_FG, cursor=CURSOR_ACTIVE)
     
        # 로그 출력
        mode_str = "Local File" if is_local else f"Cloud ({selected_type})"
        self.root.after(0, lambda: self.log(f"⚙️ Source 타입이 {mode_str}로 변경되어 설정이 초기화되었습니다."))   
    
    def on_entry_change(self, event, area_type):
        """
        설정값이 변경될 때 호출되는 핸들러
        데이터 전송 중이면 초기화를 차단하고, 아니면 해당 영역의 트리뷰만 초기화
        area_type: 'source' 또는 'target'
        """
        # 데이터 전송 중인지 확인 (클래스 변수 self.is_transferring 사용)
        if hasattr(self, 'is_transferring') and self.is_transferring:
            # 전송 중일 때는 아무 작업도 하지 않음
            return
        
        widget = event.widget
        # 위젯에 저장된 이전 값 가져오기 (없으면 빈 문자열)
        old_val = getattr(widget, 'old_value', "")
        new_val = widget.get()

        # 값이 실제로 변경되었을 때만 실행
        if old_val != new_val:
            if area_type == "source":
                self.clear_source_treeview()
            elif area_type == "target":
                self.clear_target_treeview()
            
            # 변경된 값을 다시 '이전 값'으로 업데이트
            widget.old_value = new_val

    # 파일이 없는 빈 영역을 클릭하면 모든 선택을 해제함
    def on_tree_click(self, event, area_type):
        # 클릭된 위젯을 가져옴 (src_tree인지 tgt_tree인지 자동 판별)
        widget = event.widget
        # identify_region을 사용하면 클릭한 곳이 'nothing'인지 'heading'인지 더 정확히 알 수 있음
        region = widget.identify_region(event.x, event.y)
        item = widget.identify_row(event.y)

        # 행(row)이 아니거나, 배경(nothing) 영역을 클릭했을 때
        if not item or region == "nothing":
            # 기존 선택 모두 해제
            widget.selection_remove(widget.selection())
            # 포커스 초기화
            widget.focus("")
            
            # 다른 위젯으로 포커스를 옮겨 확실히 해제된 시각 효과 부여
            self.root.focus() 

            if area_type == "source":
                # 소스 전용 추가 로직
                pass
            elif area_type == "target":
                # 타겟 전용 추가 로직
                pass

    # 원본 Local PATH 폴더 선택
    def browse_folder(self):
        self.status_msg.config(text="⏳ Local 폴더 선택 중...", fg="darkblue", font=('맑은 고딕', 9))
        # 폴더 선택창 호출
        p = filedialog.askdirectory()
        if p:
            # self.path_entry_widget 사용
            self.path_entry_widget.delete(0, tk.END)
            self.path_entry_widget.insert(0, p)
            self.root.after(0, lambda: self.log(f"📁 선택된 폴더: {p}"))

            # Treeview 초기화 (내용이 있을 경우에만 실행)
            all_items = self.src_tree.get_children()
            if all_items:
                self.src_tree.delete(*all_items)  # 루프 대신 한 번에 삭제 가능

            try:
                file_list = []
                total_size = 0
                # 모든 하위 폴더 탐색
                for root, dirs, files in os.walk(p):
                    for file_name in files:
                        full_path = os.path.join(root, file_name)
                        total_size += os.path.getsize(full_path) # 파일 사이즈 누적
                        
                        # 경로를 'sub/folder/file.txt' 형태로 통일 (Windows 대응)
                        rel_path = os.path.relpath(full_path, p).replace("\\", "/")
                        
                        stats = os.stat(full_path)
                        size_str = self.format_size_windows_kb(stats.st_size)
                        mtime = datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                        
                        file_list.append((rel_path, size_str, mtime))

                # 리스트를 이름순으로 정렬
                file_list.sort(key=lambda x: x[0])

                # Treeview에 삽입
                for i, item_data in enumerate(file_list):
                    tag = 'evenrow' if i % 2 == 0 else 'oddrow'
                    self.src_tree.insert("", tk.END, values=item_data, tags=(tag,))
                
                size_str = self.get_readable_size(total_size)
                self.root.after(0, lambda: self.log(f"✅ Local 폴더 로드 완료: {p} (총 {len(file_list):,}개 파일 / 총 용량: {size_str})"))
                self.root.after(0, lambda: self.status_msg.config(text="✅ Local 폴더 로드 완료", fg="black"))
                self.root.after(5000, lambda: self.status_msg.config(text="준비 완료", fg="black"))
                
            except Exception as e:
                self.root.after(0, lambda: self.log(f"❌ 폴더 탐색 오류: {str(e)}"))
                self.root.after(0, lambda: self.status_msg.config(text="❌ 오류 발생", fg="red"))
                self.root.after(5000, lambda: self.status_msg.config(text="준비 완료", fg="black"))

    # 원본 Local PATH 파일 선택
    def browse_file(self):
        self.status_msg.config(text="⏳ Local 파일 선택 중...", fg="darkblue", font=('맑은 고딕', 9))
        # filetypes를 지정하여 가독성을 높이고, multiple=True 옵션 추가
        files = filedialog.askopenfilenames(
            title="파일 선택 (Ctrl/Shift키를 눌러 다중 선택 가능)",
            filetypes=(("모든 파일", "*.*"),)
        )
        
        if not files:
            self.status_msg.config(text="준비 완료", fg="black")
            return
        
        # 초기화 및 UI 업데이트
        file_list_str = "; ".join(files)
        self.path_entry_widget.delete(0, tk.END)
        self.path_entry_widget.insert(0, file_list_str)

        # Treeview 기존 항목 삭제
        all_items = self.src_tree.get_children()
        if all_items:
            self.src_tree.delete(*all_items)

        total_size = 0
        file_info_list = []

        try:
            # 파일 정보 추출 (파일만 선택되므로 단순 루프)
            for full_path in files:
                if os.path.exists(full_path):
                    stats = os.stat(full_path)
                    total_size += stats.st_size
                
                    # Treeview용 정보 포맷팅
                    size_str = self.format_size_windows_kb(stats.st_size)
                    mtime = datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                
                    file_info_list.append((full_path, size_str, mtime))

            # Treeview에 데이터 삽입
            for i, item_data in enumerate(file_info_list):
                tag = 'evenrow' if i % 2 == 0 else 'oddrow'
                self.src_tree.insert("", tk.END, values=item_data, tags=(tag,))

            # 로그 및 상태 업데이트
            readable_total_size = self.get_readable_size(total_size)
            log_msg = f"📑 선택 완료 | 파일: {len(files):,}개 | 총 용량: {readable_total_size}"
        
            self.root.after(0, lambda: self.log(log_msg, force_now=True))
            self.root.after(0, lambda: self.status_msg.config(text="✅ Local 파일 선택 완료", fg="black"))
            self.root.after(5000, lambda: self.status_msg.config(text="준비 완료", fg="black"))

        except Exception as e:
            self.root.after(0, lambda: self.log(f"❌ 파일 정보 로드 중 오류: {str(e)}"))
            self.root.after(0, lambda: self.status_msg.config(text="❌ 오류 발생", fg="red"))

    # 버킷 연결정보 저장하기
    def save_config(self, m):
        filename = "source_config.enc" if m == "source" else "target_config.enc"
        target_dict = self.src_entries if m == "source" else self.tgt_entries
        bucket_key = "Source Bucket/Path:" if m == "source" else "Target Bucket:"

        # 입력 필드 데이터 수집
        endpoint = target_dict["Endpoint URL:"].get().strip()
        access_key = target_dict["Access Key:"].get().strip()
        secret_key = target_dict["Secret Key:"].get().strip()
        bucket = target_dict[bucket_key].get().strip()
        
        # 필수값 검사
        if not endpoint or not bucket:
            messagebox.showwarning("입력 확인", "Endpoint URL과 Bucket은 필수 입력 항목입니다.")
            return

        # Target은 Access / Secret 필수
        if m == "target" and (not access_key or not secret_key):
            messagebox.showwarning("입력 확인", "Target은 Access Key와 Secret Key가 반드시 필요합니다.")
            return

        # Source는 Public 허용
        if m == "source" and (not access_key or not secret_key):
            self.root.after(0, lambda: self.log("🔓 Source 설정 저장: Public(Anonymous) 모드로 저장합니다."))

        raw_vals = [endpoint, access_key, secret_key, bucket]

        pwd = simpledialog.askstring(f"암호 설정", 
                                     f"버킷 연결정보를 암호화할 비밀번호를 입력하세요\n(8자 이상 영문/숫자/특수문자 포함):", 
                                     show='*')
        if not pwd: return

        # 보안 규정 검증 (8자 이상, 영문, 숫자, 특수문자 포함)
        # ^(?=.*[A-Za-z]) : 영문자 최소 하나 포함
        # (?=.*\d) : 숫자 최소 하나 포함
        # (?=.*[@$!%*#?&]) : 특수문자 종류에 상관없이 최소 하나 포함
        # 위 조건을 만족하는 8글자 이상 문장
        password_regex = r"^(?=.*[A-Za-z])(?=.*\d)(?=.*[!@#$%^&*()_+={}\[\]|\\:;\"'<>,.?/~`-]).{8,}$"
        
        if not re.match(password_regex, pwd):
            messagebox.showerror("보안 정책 위반", 
                                 "보안을 위해 아래 규칙을 지켜주세요.\n\n"
                                 "1. 8글자 이상 입력\n"
                                 "2. 영문자, 숫자, 특수문자 각각 최소 1개 포함")
            return

        try:
            # 현재 기기 ID를 포함하여 저장 데이터 구성
            current_device = self._get_device_id()
            # 저장 데이터 리스트 구성 (5개 항목)
            raw_vals = [endpoint, access_key, secret_key, bucket, current_device] 
        
            # 기기 변경 대응형 키 생성 (_get_fernet_key)
            fernet = self._get_fernet_key(pwd)
            encrypted_data = fernet.encrypt("|".join(raw_vals).encode())
         
            with open(filename, "wb") as f:
                f.write(encrypted_data)
            
            messagebox.showinfo("성공", f"버킷 연결정보가 {filename}에 저장되었습니다.")
            self.root.after(0, lambda: self.log(f"💾 {m} 설정 파일 저장 완료: {os.path.basename(filename)}"))
        except Exception as e:
            messagebox.showerror("오류", f"저장 중 오류 발생: {e}")

    # 버킷 연결정보 불러오기
    def load_config(self, m):
        filename = "source_config.enc" if m == "source" else "target_config.enc"
        target_dict = self.src_entries if m == "source" else self.tgt_entries
        # Target Bucket과 Source Path의 딕셔너리 키 명칭이 다르므로 매핑 처리
        bucket_key = "Source Bucket/Path:" if m == "source" else "Target Bucket:"

        if not os.path.exists(filename):
            messagebox.showwarning("알림", f"저장된 버킷 연결정보 파일({filename})이 없습니다.")
            return

        pwd = simpledialog.askstring(f"암호 입력", 
                                     f"{m.upper()} 연결정보를 불러오기 위해 비밀번호를 입력하세요:", 
                                     show='*')
        if not pwd: return

        try:
            fernet = self._get_fernet_key(pwd)
            with open(filename, "rb") as f:
                encrypted_content = f.read()
                # 복호화 시도
                decrypted_data = fernet.decrypt(encrypted_content).decode('utf-8')
            
            # 데이터 분해 및 입력창에 삽입
            v = decrypted_data.split("|")

            # 기기 ID 체크 (5번째 항목이 있는지 확인)
            if len(v) >= 5:
                saved_device = v[4]
                current_device = self._get_device_id()
                if saved_device != current_device:
                    # 기기가 달라도 '확인'을 누르면 계속 진행 (사용자 편의)
                    confirm = messagebox.askyesno("기기 변경 감지", 
                        "이 설정 파일은 다른 기기(PC)에서 작성된 것으로 보입니다.\n계속 불러오시겠습니까?")
                    if not confirm: return
            
            else:
                raise ValueError("저장된 데이터 형식이 올바르지 않습니다.")

            target_dict["Endpoint URL:"].delete(0, tk.END); target_dict["Endpoint URL:"].insert(0, v[0])
            target_dict["Access Key:"].delete(0, tk.END); target_dict["Access Key:"].insert(0, v[1])
            target_dict["Secret Key:"].delete(0, tk.END); target_dict["Secret Key:"].insert(0, v[2])
            target_dict[bucket_key].delete(0, tk.END); target_dict[bucket_key].insert(0, v[3])

            # access_key 변수 대신 리스트 v의 인덱스를 직접 참조하거나 변수 선언
            cur_ak = v[1].strip()
            cur_sk = v[2].strip()

            # Public / Private 상태 로그
            if m == "source" and (not cur_ak or not cur_sk):
                self.root.after(0, lambda: self.log("🔓 Source 설정 불러오기: Public(Anonymous) 모드"))
            elif m == "target" and (not cur_ak or not cur_sk):
                self.root.after(0, lambda: self.log("⚠ Target 설정에 Access/Secret Key가 비어 있습니다."))

            # 로그 출력 전 잠시 GUI 프로세스 처리
            # self.root.update()
            # self.clear_treeview()
            self.root.after(0, lambda: self.log(f"✅ {m} 연결설정 불러오기 성공: {os.path.basename(filename)}"))
            messagebox.showinfo("성공", f"{m} 설정을 성공적으로 불러왔습니다.")
            if m == "source":
                self.clear_source_treeview()
            elif m == "target":
                self.clear_target_treeview()
 
        except InvalidToken:
            # 암호가 틀렸을 때 발생하는 전용 에러
            messagebox.showerror("암호 오류", "암호가 일치하지 않습니다. 다시 확인해 주세요.")
        except Exception as e:
            messagebox.showerror("오류", f"설정 파일을 불러오는 중 오류가 발생했습니다:\n{str(e)}")

    def open_calendar(self):
        """달력 팝업창을 띄워 날짜 선택"""
        # 새 창 생성
        top = tk.Toplevel(self.root)
        top.title("날짜 선택")
        # 부모 창 중앙 근처에 배치
        top.geometry(f"+{self.root.winfo_x() + 400}+{self.root.winfo_y() + 200}")
        top.resizable(False, False)
        top.transient(self.root) # 부모 창 위에 항상 위치
        top.grab_set()           # 달력 창이 닫히기 전까지 메인 창 조작 방지

        # 달력 위젯 생성
        cal = Calendar(top, 
                       selectmode='day', 
                       year=datetime.now().year, 
                       month=datetime.now().month, 
                       day=datetime.now().day,
                       date_pattern='yymmdd') # YYYYMMDD 형식을 유도하기 위한 설정
        cal.pack(padx=20, pady=20)

        def set_date():
            # 선택된 날짜 객체 가져오기
            selected_date_obj = cal.selection_get()
            # YYYYMMDD 형식 문자열로 변환
            date_str = selected_date_obj.strftime("%Y%m%d")
            
            # 입력창 초기화 후 삽입
            self.sched_date.delete(0, tk.END)
            self.sched_date.insert(0, date_str)
            
            self.root.after(0, lambda: self.log(f"📅 예약 날짜가 선택되었습니다: {date_str}"))
            top.destroy()

        # 선택 버튼
        btn_sel = tk.Button(top, text="날짜 선택 완료", command=set_date, 
                            bg="#e1f5fe", font=('맑은 고딕', 9, 'bold'))
        btn_sel.pack(pady=10)

    # 좌측 Source 목록 우클릭 선택 기능
    def show_src_context_menu(self, event):
        # 현재 소스 타입 확인
        source_type = self.src_type_var.get()
        is_local = "Local" in source_type
        
        # 클릭한 위치의 아이템 파악
        item = self.src_tree.identify_row(event.y)
        current_selection = self.src_tree.selection()

        # 메뉴 초기화
        self.context_menu = tk.Menu(self.root, tearoff=0)

        # 아이템 위에서 우클릭했을 경우
        if item:
            # 다중 선택 지원: 우클릭한 항목이 선택 영역에 없으면 단일 선택으로 변경
            if item not in current_selection:
                self.src_tree.selection_set(item)
                current_selection = (item,) # 갱신
                                  
            # 객체 목록에서 Key(경로) 리스트 추출
            # 트리뷰 생성 시 text 또는 values[0] 중 어디에 Key를 넣었는지에 따라 선택
            files = []
            for i in current_selection:
                item_data = self.src_tree.item(i)
                # Name 컬럼이 첫 번째 values에 있으므로 우선 확인, 없으면 text 확인
                key = item_data['values'][0] if item_data['values'] else item_data['text']
                if key:
                    files.append(key)

            if files:
                # 클라우드일 때만 '전송' 메뉴 추가
                if not is_local:
                    self.context_menu.add_command(
                        label=f"⚡ 선택한 {len(files)}개 객체 전송", 
                        command=lambda: self.start_transfer_thread(specific_files=files))
                    self.context_menu.add_separator()

                # 파일명 복사 (로컬/클라우드 공통)
                self.context_menu.add_command(
                    label="📋 파일명 클립보드 복사", 
                    command=lambda: self.copy_treeview_filename(specific_files=files))
                self.context_menu.add_separator()

        # 선택된 것이 있을 때 공통 메뉴 (로컬/클라우드 공통), 빈 공간 우클릭 시에도 '선택 해제' 메뉴는 보여줌
        if current_selection:
            # 선택 해제 메뉴
            self.context_menu.add_command(
                label="✕ 모든 선택 해제 (Deselect All)", 
                command=lambda: self.src_tree.selection_remove(self.src_tree.selection()))
            # 메뉴 표시 (선택된 게 있을 때만 메뉴를 팝업)
            self.context_menu.post(event.x_root, event.y_root)

    # 우측 Target 목록 우클릭 선택 기능
    def show_context_menu(self, event):
        """마우스 우클릭 시 해당 항목을 선택하고 메뉴를 표시합니다."""
        item = self.tgt_tree.identify_row(event.y)
        if item:
            if item not in self.tgt_tree.selection():
                self.tgt_tree.selection_set(item)

            # 🔍 우측 전용 메뉴를 여기서 다시 정의하여 덮어쓰기 방지
            # (또는 별도로 생성해둔 tgt_menu를 호출)
            tgt_menu = tk.Menu(self.root, tearoff=0)
            tgt_menu.add_command(label="📥 다운로드", command=self.download_file)
            tgt_menu.add_separator()
            tgt_menu.add_command(label="⚡ 실행", command=self.run_file)
            tgt_menu.add_separator()
            tgt_menu.add_command(label="📋 파일명 클립보드 복사", 
                                 command=lambda: self.copy_treeview_filename(tree_widget=self.tgt_tree))
            tgt_menu.add_separator()
            tgt_menu.add_command(label="🗑 삭제", command=self.delete_selected_files, foreground="red")
            
            tgt_menu.post(event.x_root, event.y_root)

    def clear_treeview(self):
        """조회된 모든 리스트(소스 및 타겟) 초기화"""
        self.clear_source_treeview()
        self.clear_target_treeview()
    
    def clear_source_treeview(self):
        """소스 트리뷰 목록만 초기화"""
        # 이미 목록이 비어 있다면 아무것도 하지 않고 리턴
        items = self.src_tree.get_children()
        if not items:
            return
        
        # 목록이 있을 경우에만 삭제 및 로그 출력
        for item in items:
            self.src_tree.delete(item)
    
        self.status_msg.config(text="소스 설정 변경됨 - 다시 조회 필요", fg="orange")
        self.root.after(0, lambda: self.log("🧹 소스 연결 정보 변경으로 인해 소스 목록이 초기화되었습니다."))
 
    def clear_target_treeview(self):
        """타겟 트리뷰 목록만 초기화"""
        # 이미 목록이 비어 있다면 아무것도 하지 않고 리턴
        items = self.tgt_tree.get_children()
        if not items:
            return
        
        # 목록이 있을 경우에만 삭제 및 로그 출력
        for item in items:
            self.tgt_tree.delete(item)
    
        self.status_msg.config(text="타겟 설정 변경됨 - 다시 조회 필요", fg="orange")
        self.root.after(0, lambda: self.log("🧹 타겟 연결 정보 변경으로 인해 타겟 목록이 초기화되었습니다."))

    def copy_treeview_filename(self, tree_widget=None, specific_files=None):
        """
        트리뷰 항목 복사 (우클릭 메뉴 및 단축키 공용)
        - specific_files: 좌측 메뉴처럼 이미 파일 리스트를 뽑아낸 경우
        - tree_widget: 단축키나 우측 메뉴처럼 위젯에서 직접 추출해야 하는 경우
        """
        file_names = []

        # 특정 파일 리스트가 인자로 넘어온 경우 (우클릭 메뉴에서 호출 시)
        if specific_files:
            file_names = specific_files
    
        # 단축키 등으로 호출되어 특정 리스트가 없는 경우 (트리뷰에서 직접 추출)
        elif tree_widget:
            selection = tree_widget.selection()
            for item_id in selection:
                # 컬럼 ID "Name" 값을 우선시하고 없으면 첫 번째 값 가져오기
                file_name = tree_widget.set(item_id, "Name")
                if not file_name:
                    item_data = tree_widget.item(item_id)
                    file_name = item_data['values'][0] if item_data['values'] else item_data['text']
            
                if file_name:
                    file_names.append(str(file_name))

        if file_names:
            copy_text = "\n".join(file_names)
            self.root.clipboard_clear()
            self.root.clipboard_append(copy_text)
        
            # 피드백 출력
            msg = f"📋 클립보드 복사 완료: {file_names[0]}"
            if len(file_names) > 1:
                msg += f" 외 {len(file_names)-1}건"
        
            self.log(msg)
            self.status_msg.config(text=msg, fg="blue")
            self.root.after(3000, lambda: self.status_msg.config(text="준비 완료", fg="black"))

    def cancel_transfer(self):
        """
        사용자가 중단 버튼을 눌렀을 때 호출
        """
        if not self.transfer_running:
            return

        if messagebox.askyesno("중단 확인", "현재 진행 중인 전송 작업을 중단하시겠습니까?"):
            # 중단 플래그 설정 (process_transfer 스레드에서 이를 체크하여 멈춤)
            self.cancel_requested = True   # 변수명을 클래스 내 체크 변수와 일치시킴
            
            self.log(f"🛑 사용자에 의해 중단 요청됨. 현재 파일 완료 후 정지합니다.", force_now=True)
            
            # 버튼 상태 즉시 변경
            self.start_btn.config(state="normal")
            self.stop_btn.config(
                state="disabled", 
                bg="#f0f0f0", 
                fg="#a0a0a0", 
                relief="raised", 
                cursor="arrow",
                bd=2
            )
            # 만약 스레드가 즉시 종료되지 않더라도 UI는 다시 시작 가능한 상태로 만듦
            # 상태 메시지도 중단 중임을 표시
            self.status_msg.config(text="🛑 중단 중...", fg="red")

    # 버튼 클릭 시 직접 로그 저장 창 열기
    def manual_save_log(self):
        log_content = self.log_area.get("1.0", tk.END).strip()
        if not log_content:
            messagebox.showwarning("알림", "저장할 로그 내용이 없습니다.")
            return

        now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            initialfile=f"save_log_{now_str}.txt",
            title="로그 파일 저장 위치 선택",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        
        if save_path:
            try:
                with open(save_path, "w", encoding="utf-8") as f:
                    f.write(log_content)
                messagebox.showinfo("저장 완료", f"로그가 저장되었습니다:\n{save_path}")
            except Exception as e:
                messagebox.showerror("저장 실패", f"로그 저장 중 오류가 발생했습니다: {e}")
      
    # PDF 도움말 파일을 여는 함수
    def open_help_pdf(self):
        pdf_file = "qas-objectops.pdf"
        if os.path.exists(pdf_file):
            try:
                # Windows 환경에서 기본 PDF 뷰어로 실행
                os.startfile(pdf_file)
                self.root.after(0, lambda: self.log(f"📖 도움말 파일을 열었습니다: {pdf_file}"))
            except Exception as e:
                messagebox.showerror("오류", f"파일을 열 수 없습니다: {str(e)}")
        else:
            messagebox.showwarning("파일 없음", f"도움말 파일({pdf_file})을 찾을 수 없습니다.\n실행 파일과 같은 폴더에 있는지 확인해주세요.")
          
    # =========================================================
    # ----- 원본 데이타 to ActiveScale(전송 예약시간 관리) --------
    # =========================================================
    def start_transfer_thread(self, specific_files=None):
        """
        [시작] 버튼 클릭 시 호출되는 진입점 (메인 스레드)
        네트워크 작업 전 UI를 즉시 고정하여 사용자의 중복 클릭을 방지
        """
        # 백신(Multi-Engine) 사용 시 환경 체크 수행
        if self.use_av_scan.get():
            if not self.check_av_environment():
                return   # 관리자 권한 미달, 백신 미설치 시 중단
            
        if self.transfer_running:
            messagebox.showwarning("경고", "이미 전송이 진행 중입니다.")
            return
        
        # 메인 스레드에서 안전하게 UI 입력값들을 미리 읽어옴
        try:
            target_info = {k: v.get().strip() for k, v in self.tgt_entries.items()}
            sched_date = self.sched_date.get().strip()
            sched_ampm = self.sched_ampm.get()
            sched_hour = self.sched_hour.get()
            sched_min = self.sched_min.get()
            # 소스 타입 정보도 미리 캡처
            src_type = self.src_type_var.get()
        except Exception as e:
            self.log(f"❌ 입력값 읽기 실패: {str(e)}", force_now=True)
            self.transfer_running = False
            return

        # UI 즉시 반영
        self.status_msg.config(text="⏳ 전송 프로세스 시작 중...", fg="blue", font=('맑은 고딕', 9))
        self.is_transferring = True
        self.transfer_running = True
        self.cancel_requested = False

        # 버튼 및 입력창 상태 잠금
        self.start_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self.src_combo.config(state="disabled")
        # 딕셔너리에서 해당 위젯을 찾아 비활성화
        src_path_widget = self.src_entries.get("Source Bucket/Path:")
        if src_path_widget:
            src_path_widget.config(state="disabled")

        # 모든 검증과 대기 로직을 백그라운드로 위임
        # 스레드 시작
        thread = threading.Thread(
            target=self.transfer_full_process_worker, 
            args=(specific_files, target_info, sched_date, sched_ampm, sched_hour, sched_min, src_type), 
            daemon=True
        )
        thread.start()
        # print("DEBUG: Thread started successfully")

    def transfer_full_process_worker(self, specific_files, target_info, sched_date, ampm, hour_str, min_str, src_type):
        """
        백그라운드에서 S3 접속 검증, 예약 시간 계산, 대기 처리를 모두 수향
        여기서 발생하는 지연은 UI를 멈추게 하지 않음
        """
        try:
            # 타겟 서버 접속 사전 검증
            self.root.after(0, lambda: self.log("🔍 전송 시작 전 타겟 서버 접속 확인 중...", force_now=True))

            # 필수 정보 체크 (이미 인자로 받은 target_info 사용)
            if not target_info.get("Endpoint URL:") or not target_info.get("Target Bucket:"):
                self.root.after(0, lambda: messagebox.showwarning("입력 확인", "Target 정보는 필수입니다."))
                self.root.after(0, self.reset_transfer_state)
                return

            # S3 접속 테스트 (스레드 내부에서 안전하게 실행)
            test_client = self.create_s3_client(target_info)
            if not test_client:
                self.root.after(0, self.reset_transfer_state)
                return
            
            # 대소문자 무시 및 공백 제거 후 비교
            is_local = "LOCAL" in str(src_type).upper().strip()
            # 로컬 소스 및 백신 검사 옵션 체크
            is_av_enabled = self.use_av_scan.get()

            if is_local and is_av_enabled:
                # 현재 선택된 엔진명 가져오기
                selected_engine = self.av_engine_var.get()
                self.root.after(0, lambda e=selected_engine: self.log(f"🛡️ {e} 백신 검사 시작...", force_now=True))

                # Entry 위젯에서 현재 경로 문자열 가져오기
                src_path_entry = self.src_entries.get("Source Bucket/Path:")
                current_path_str = src_path_entry.get().strip() if src_path_entry else ""

                if not current_path_str:
                    # Entry가 비어있다면 specific_files나 selected_files 확인
                    if not specific_files:
                        specific_files = getattr(self, 'selected_files', [])
                    
                    if isinstance(specific_files, list):
                        paths_to_check = specific_files
                    else:
                        paths_to_check = [item.strip() for item in str(specific_files).split(';') if item.strip()]
                else:
                    # Entry에 있는 값을 ';' 기준으로 분리하여 리스트화
                    paths_to_check = [item.strip() for item in current_path_str.split(';') if item.strip()]

                # 실제 검사될 총 파일 개수 미리 계산
                total_files_count = 0
                for p in paths_to_check:
                    if self.cancel_requested: 
                        self.root.after(0, self.reset_transfer_state); return
                    
                    if os.path.isfile(p):
                        total_files_count += 1
                    elif os.path.isdir(p):
                        # 폴더인 경우, 사용자에게 집계 중임을 알림
                        self.root.after(0, lambda: self.log("📊 검사 대상 파일 개수 확인 중...", force_now=True))

                        # 폴더 내 모든 파일 개수 합산
                        # 에러 방지 및 중단 체크를 포함한 집계
                        for root, dirs, files in os.walk(p):
                            if self.cancel_requested: break
                            total_files_count += len(files)

                total_items = len(paths_to_check)

                for idx, item in enumerate(paths_to_check, 1):
                    # 경로 정규화 (Windows 경로 포맷 통일)
                    item = os.path.normpath(item)
                    if not os.path.exists(item): continue
                    
                    if self.cancel_requested:
                        self.root.after(0, self.reset_transfer_state); return
                    
                    display_name = os.path.basename(item)
                    # 로그에 선택된 엔진 이름, 검사 폴더/파일 표시
                    self.root.after(0, lambda n=display_name, i=idx, t=total_items, e=selected_engine: 
                                    self.log(f"🔍 [{i}/{t}] {e} 검사 중: {n}", force_now=True))
                    
                    # 다중 엔진을 지원하는 통합 백신 검사 함수 호출 (파일이면 파일검사, 폴더면 폴더 하위 전체검사)
                    is_safe = self.run_av_scan(item)
                    
                    if not is_safe:
                        msg = f"🚨 보안 위협 탐지 ({selected_engine})\n\n감염 의심 항목: {item}\n\n데이터 안전을 위해 전송을 중단합니다."
                        self.root.after(0, lambda m=msg: messagebox.showerror("보안 위험", m))
                        self.root.after(0, self.reset_transfer_state); return

                # 최종 완료 메시지에 총 파일 개수 포함
                self.root.after(0, lambda t=total_items, f=total_files_count, e=selected_engine: 
                                self.log(f"✅ 백신 검사 완료 ({e}): 총 {t}개 항목(내부 파일 약 {f}개)이 안전합니다.", force_now=True))

            # 예약 로직 처리 (인자로 받은 값들 사용)
            if not sched_date:
                self.root.after(0, lambda: self.log("⚠️ 즉시 전송 시작", force_now=True))
                # 필요한 UI 상태 업데이트 후 메인 로직 실행
                self.root.after(0, lambda: self.status_msg.config(text="⏳ 데이터 전송 중...", fg="darkgreen"))
                self.start_transfer_thread_logic(specific_files, src_type) 
                return

            # 예약 시간 검증 케이스
            try:
                hour = int(hour_str)
                minute = int(min_str)
            
                # 12시간제 -> 24시간제 변환
                if ampm == "PM" and hour < 12: hour += 12
                if ampm == "AM" and hour == 12: hour = 0

                if not re.match(r"^\d{8}$", sched_date):
                    raise ValueError("날짜는 YYYYMMDD 형식이어야 합니다.")
            
                target_time = datetime.strptime(f"{sched_date}{hour:02d}{minute:02d}", "%Y%m%d%H%M")
                now = datetime.now()
            
                # 예약 시간이 현재보다 과거인 경우 즉시 실행
                if target_time <= now:
                    self.root.after(0, lambda: self.log("⚠️ 과거 시간 설정: 즉시 전송을 시작합니다.", force_now=True))
                    self.root.after(0, lambda: self.status_msg.config(text="⏳ 데이터 전송 중...", fg="darkgreen"))
                    self.start_transfer_thread_logic(specific_files, src_type)
                else:
                    # 미래 시간인 경우 대기 로직 진입
                    diff = (target_time - now).total_seconds()
                    # 예약 대기 중에도 '전송 중단' 버튼을 누를 수 있도록 UI 상태 변경
                    self.root.after(0, lambda: self.stop_btn.config(
                        state="normal", 
                        bg="#f44336", 
                        fg="white", 
                        cursor=self.CURSOR_HAND
                    ))
                    self.root.after(0, lambda: self.start_btn.config(state="disabled"))

                    self.root.after(0, lambda: self.log(f"⏰ 전송 예약 완료: {target_time.strftime('%Y-%m-%d %p %I:%M')} (약 {int(diff)}초 후 시작)", force_now=True))
                    self.root.after(0, lambda: self.status_msg.config(
                        text=f"⏳ 예약 대기 중: {target_time.strftime('%p %I:%M')}", 
                        fg="purple", 
                        font=('맑은 고딕', 9, 'bold')
                    ))
                    
                    # 중단 가능하도록 1초 단위 대기 루프
                    while diff > 0:
                        # 사용자가 '중단' 버튼을 누르면 self.cancel_requested가 True
                        if self.cancel_requested:
                            self.root.after(0, lambda: self.log("🛑 예약 전송이 사용자에 의해 취소되었습니다.", force_now=True))
                            self.root.after(0, self.reset_transfer_state)
                            return
                        
                        # 1초씩 끊어서 대기(중단 버튼 빠른 반응)
                        time.sleep(min(1, diff))
                        diff -= 1
                    
                    # 대기 완료 후 로그 기록 및 실행
                    self.root.after(0, lambda: self.log("🚀 예약 시간이 되어 전송 프로세스를 시작합니다.", force_now=True))
                    self.root.after(0, lambda: self.status_msg.config(text="⏳ 데이터 전송 중...", fg="darkgreen"))
                    self.start_transfer_thread_logic(specific_files, src_type)

            except ValueError as ve:
                self.root.after(0, lambda: messagebox.showerror("형식 오류", f"예약 정보가 올바르지 않습니다.\n({str(ve)})"))
                self.root.after(0, lambda: self.log(f"❌ 예약 설정 오류: {str(ve)}", force_now=True))
                self.root.after(0, self.reset_transfer_state)

        except Exception as e:
            self.root.after(0, lambda: self.log(f"❌ 전송 준비 중 예상치 못한 오류: {str(e)}", force_now=True))
            self.root.after(0, self.reset_transfer_state)

    # ------ 멀티 스레드 전송 -------
    def start_transfer_thread_logic(self, specific_files_from_menu=None, src_type=None):
        """
        실제 전송 로직이 실행되는 부분
        """
        try:
            # UI 상태 변경은 root.after를 통해 메인스레드에서 실행
            self.root.after(0, lambda: self.start_btn.config(state="disabled"))
            self.root.after(0, lambda: self.stop_btn.config(
                state="normal", 
                bg="#f44336", 
                fg="white", 
                relief="raised", 
                cursor=self.CURSOR_HAND
            ))

            self.transfer_running = True
            self.cancel_requested = False 

            # 인자로 넘어온 src_type이 없으면 UI에서 가져옴
            if not src_type:
                src_type = self.src_type_var.get()
            specific_files = []
            src_path_entry = self.src_entries.get("Source Bucket/Path:")
            current_path = src_path_entry.get().strip() if src_path_entry else ""

            # 우클릭 메뉴를 통해 들어온 파일 리스트가 있다면 바로 할당하고 종료
            if specific_files_from_menu:
                specific_files = specific_files_from_menu
                self.log(f"📥 우클릭 선택 항목 전송 시작 ({len(specific_files):,}개)", force_now=True)

            # 로컬 파일 시스템 경로 수집
            elif "Local" in src_type:
                if current_path:
                    specific_files.append(current_path)
                    self.log(f"🔎 로컬 경로 확인: {current_path}")
                else:
                    self.log("⚠ 오류: 로컬 소스 경로가 입력되지 않았습니다.", force_now=True)
                    self.reset_transfer_state()
                    return

            # 클라우드 소스(S3 등)인 경우
            else:
                # 현재 선택된 모든 항목 가져오기 (Shift/Ctrl/마우스 선택 모두 포함)
                selected_items = self.src_tree.selection()

                # 선택된 항목이 있는 경우: 선택된 것만 전송
                if selected_items:
                    self.log(f"🚀 선택된 {len(selected_items):,}개 항목을 전송합니다.", force_now=True)
                    for i in selected_items:
                        item_data = self.src_tree.item(i)
                        val = item_data.get('values')
                        file_name = str(val[0]) if val else item_data.get('text')
                    
                        # 경로 결합 로직
                        if current_path:
                            clean_path = current_path.strip('/')
                            full_key = file_name if file_name.startswith(clean_path) else f"{clean_path}/{file_name.lstrip('/')}"
                        else:
                            full_key = file_name
                        
                        if not full_key.endswith('/'): # 디렉토리 제외
                            specific_files.append(full_key)
                        
                else:
                    # 선택된 항목이 없거나 조회를 안 한 경우 -> S3 직접 스캔
                    self.log(f"🔍 조회를 생략했거나 선택 항목이 없습니다. '{current_path}' 경로 전체를 스캔합니다...", force_now=True)
                    
                    # 실제 S3 리스트를 가져오는 메서드 호출 (self.get_all_cloud_keys)
                    # 이 메서드는 boto3 등을 이용해 실제 버킷 내의 모든 Key를 리스트로 반환
                    specific_files = self.get_all_cloud_keys(current_path)

            # --- 최종 검증 및 실행 단계 ---
            if not specific_files:
                self.log("⚠ 전송 대상이 없습니다. 경로를 확인하거나 [조회] 후 선택해 주세요.", force_now=True)
                self.root.after(0, self.reset_transfer_state)
                return

            # 실제 파일 처리 함수 호출 (이미 워커 스레드 위에서 실행 중이므로 직접 호출)
            self.process_transfer(specific_files)

        except Exception as e:
            self.log(f"❌ 전송 준비 중 오류 발생: {str(e)}", force_now=True)
            self.root.after(0, self.reset_transfer_state)

    def get_all_cloud_keys(self, path_input):
        """Treeview에 데이터가 없어도 S3 API를 통해 직접 전체 Key 목록을 가져옴"""
        all_keys = []
        try:
            # 현재 UI 입력창들에서 인증 정보 수집
            # 소스(Source) 설정 섹션에서 정보를 가져옴
            info = {
                "Endpoint URL:": self.src_entries.get("Endpoint URL:").get().strip(),
                "Access Key:": self.src_entries.get("Access Key:").get().strip(),
                "Secret Key:": self.src_entries.get("Secret Key:").get().strip(),
                "Source Bucket/Path:": path_input
            }

            # 기존 함수를 사용하여 클라이언트 생성
            self.s3_client = self.create_s3_client(info)
        
            if not self.s3_client:
                return []

            # 버킷과 프리픽스 분리
            parts = path_input.strip('/').split('/', 1)
            bucket_name = parts[0]
            prefix = parts[1] if len(parts) > 1 else ""

            # Paginator로 전체 목록 스캔
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if not key.endswith('/'):  # 파일만 추가
                            all_keys.append(key)
        
            self.log(f"✅ 직접 스캔 완료: {len(all_keys):,}개 파일 확인", force_now=True)
            return all_keys

        except Exception as e:
            self.log(f"❌ S3 직접 스캔 중 오류: {str(e)}", force_now=True)
            return []

    def process_transfer(self, specific_files):
        """
        멀티스레드 기반 데이터 전송 및 상세 통계 로그 출력
        Sync(동기화) 및 Checksum(무결성 검증) 기능 통합
        """
        self.transfer_running = True
        self.is_transferring = True
        start_time = time.time()

        # 락(Lock) 객체 생성 (스레드 안전성 확보용)
        self.stats_lock = threading.Lock()

        self.status_msg.config(text="🔄 타겟 서버 연결 확인 중...", fg="blue")
        self.log("🔄 타겟 서버 연결 확인 중 (Timeout 1s)...", force_now=True)
        # 스레드 안에서도 GUI 예약을 즉시 반영하도록 강제함
        # self.root.update_idletasks()

        try:
            # 정보 수집
            tgt_info = {k: v.get().strip() for k, v in self.tgt_entries.items()}
            src_info = {k: v.get().strip() for k, v in self.src_entries.items()}

            tgt_url = tgt_info.get("Endpoint URL:", "")
            tgt_ak = tgt_info.get("Access Key:", "")
            tgt_sk = tgt_info.get("Secret Key:", "")
            self.cur_target_bucket = tgt_info.get("Target Bucket:", tgt_info.get("Bucket(or Root):", ""))

            if not tgt_url or not self.cur_target_bucket:
                self.log("❌ 오류: Target 설정(URL 또는 Bucket)이 올바르지 않습니다.", force_now=True)
                self.reset_transfer_state()
                return
        
            # Target S3 클라이언트 생성 (최대한 가볍게 설정)
            tgt_s3_config = Config(
                s3={'addressing_style': 'path'}, 
                signature_version='s3v4', 
                retries={'max_attempts': 3},   # 연결 시도 횟수
                connect_timeout=5,             # 연결 시도 시간 5초
                read_timeout=5,                # 읽기 타임아웃 5초
                proxies={'http': None, 'https': None}
            )

            tgt_s3 = boto3.client('s3', 
                                  aws_access_key_id=tgt_ak, 
                                  aws_secret_access_key=tgt_sk,
                                  endpoint_url=tgt_url, 
                                  verify=False, 
                                  config=tgt_s3_config)
          
            # ActiveScale 호환성을 위한 헤더 제거 등록(서명 직전 및 전송 직전에 강제 제거)
            def disable_unsupported_features(request, **kwargs):
                # ActiveScale에서 거부하는 대표적인 헤더들
                bad_headers = [
                    'Expect', 
                    'Content-MD5', 
                    'x-amz-sdk-checksum-algorithm', 
                    'x-amz-checksum-crc32',
                    'x-amz-checksum-crc32c',
                    'x-amz-checksum-sha1',
                    'x-amz-checksum-sha256'
                ]
                for h in bad_headers:
                    if h in request.headers:
                        del request.headers[h]
                    # 소문자로 들어오는 경우도 대비
                    if h.lower() in request.headers:
                        del request.headers[h.lower()]

            # 두 가지 이벤트 단계에 모두 등록하여 확실히 제거
            tgt_s3.meta.events.register('before-sign.s3.*', disable_unsupported_features)
            tgt_s3.meta.events.register('before-send.s3.PutObject', disable_unsupported_features)

            self.log("🚀 전송 엔진 및 이벤트 리스너 가동...", force_now=True)

            if self.cancel_requested:
                self.reset_transfer_state()
                return
                                    
            src_type = self.src_type_var.get()
            all_tasks = []   # (원본경로, 대상키, 사이즈, 로컬여부)
            
            # --- 소스 분석 로직 시작, 전송 모드에 따른 분기 처리
            self.log(f"🔍 소스 분석 단계 진입: {specific_files}", force_now=True)
            if "Local" in src_type:
                # 입력된 경로들 중 세미콜론(;)이 있으면 분리하여 처리
                refined_paths = []
                # specific_files가 이미 리스트인지, 문자열인지에 따라 처리가 달라질 수 있음
                if isinstance(specific_files, str):
                    specific_files = [specific_files]

                for p in specific_files:
                    refined_paths.extend([item.strip() for item in p.split(';') if item.strip()])

                for item in refined_paths:
                    if not os.path.exists(item):
                        self.log(f"❌ 파일을 찾을 수 없음: {item}", force_now=True)
                        continue
                    
                    if self.cancel_requested:
                        self.log("🛑 소스 분석 중 중단됨", force_now=True)
                        return
                    
                    item = item.replace("\\", "/")
                    if not os.path.exists(item):
                        self.root.after(0, lambda i=item: self.log(f"⚠️ 경로 없음 제외: {i}", force_now=True))
                        continue
                        
                    if os.path.isfile(item):
                        all_tasks.append((item, os.path.basename(item), os.path.getsize(item), True))
                    elif os.path.isdir(item):
                        base_parent = os.path.dirname(item)
                        for r, _, fs in os.walk(item):
                            for f in fs:
                                fp = os.path.join(r, f).replace("\\", "/")
                                rp = os.path.relpath(fp, base_parent).replace("\\", "/")
                                all_tasks.append((fp, rp, os.path.getsize(fp), True))
            else:
                # --- 클라우드(S3 to S3) 분석 로직 ---
                self.log("☁ 클라우드 객체 분석 중...", force_now=True)
                
                # 버킷명을 더 유연하게 찾기 (라벨명이 달라도 대응)
                src_bucket = ""
                for k, v in src_info.items():
                    if "Bucket" in k:
                        src_bucket = v.strip()
                        break
                
                # 만약 위에서 못 찾았다면 직접 지정 
                if not src_bucket:
                    src_bucket = src_info.get("Source Bucket/Path:", "").split('/')[0]
                
                # 조회 경로(Prefix) 추출: '버킷명/' 같은 경로가 있다면 활용
                raw_path = ""
                for k, v in src_info.items():
                    if "Path" in k or "Bucket" in k:
                        if v.strip() and v.strip() != src_bucket:
                            raw_path = v.strip()
                            break
                
                current_prefix = raw_path.rstrip('/') + '/' if raw_path else ""
                
                src_s3 = self.create_s3_client(src_info)
                
                if not src_s3 or not src_bucket:
                    self.log(f"❌ 오류: 소스 정보 부족 (Bucket: '{src_bucket}')", force_now=True)
                    self.reset_transfer_state()
                    return

                for key in specific_files:
                    if self.cancel_requested:
                        self.log("🛑 소스 객체 수집 중 중단됨", force_now=True)
                        return
                    
                    try:
                        clean_name = key.strip()

                        # Key 정규화
                        normalized_key = self.normalize_key(src_bucket, clean_name)

                        self.root.after(
                            0,
                            lambda k=normalized_key, b=src_bucket:
                                self.log(f"🔍 객체 확인 중: {k} (Bucket: {b})", force_now=True)
                        )

                        resp = src_s3.head_object(
                            Bucket=src_bucket,
                            Key=normalized_key
                        )

                        all_tasks.append(
                            (normalized_key, normalized_key, resp['ContentLength'], False)
                        )
                        
                    except Exception as e:
                        # 실패 시 시도했던 전체 경로를 로그에 남김
                        full_err_path = locals().get('full_key', clean_name)
                        self.root.after(0, lambda k=full_err_path, m=str(e): self.log(f"⚠ 분석 실패 [{k}]: {m}", force_now=True))
            # --- 소스 분석 로직 끝 ----

            # 병렬 처리를 위한 워커 수 설정 (GUI 입력값 반영)
            try:
                # GUI Entry에서 값 읽기
                user_workers = int(self.worker_var.get())
            
                # 최소/최대 안전 범위 제한
                if user_workers < 1: user_workers = 2
                if user_workers > 30: user_workers = 30
            except Exception:
                user_workers = 5    # 에러 시 기본값 5개
            

            # 전송 시작 확인
            total_count = len(all_tasks)
            # self.log(f"Ready to Start: Tasks={total_count}, Workers={user_workers}", force_now=True)

            if total_count == 0:
                self.log("⚠ 전송할 대상을 찾지 못했습니다.", force_now=True)
                self.reset_transfer_state()
                return
            
            # UI 체크박스 상태 읽기
            is_sync_enabled = self.sync_mode_var.get()
            is_verify_enabled = self.verify_mode_var.get()

            # 사용자가 GUI에 입력한 최적화 설정값 가져오기
            transfer_config = self.get_transfer_config()
          
            self.log(f"🚀 전송 시작: 총 {total_count:,}개 파일", force_now=True)
            self.log(f"ℹ️ 설정: 임계값(Threshold) {self.threshold_var.get()}MB, 조각(Chunk Size) {self.chunk_var.get()}MB, 병렬 워커(Max Worker) {user_workers}개", force_now=True)
          
            # [Worker 및 실행부]
            success_count = 0
            skip_count = 0
            fail_count = 0
            total_bytes = 0

            def upload_worker(task):
                nonlocal success_count, skip_count, fail_count, total_bytes
                # 워커 시작 즉시 체크
                if self.cancel_requested: return False
                
                # 인자 4개를 정확히 언패킹 (경로, 대상키, 사이즈, 로컬여부)
                src_p, tgt_k, size, is_local = task

                # 설정하신 임계값 (예: 100MB)
                # THRESHOLD = 1024 * 1024
                # GUI의 임계값(MB)을 Byte로 변환하여 사용
                THRESHOLD_BYTES = transfer_config.multipart_threshold
                CHUNK_SIZE = transfer_config.multipart_chunksize
                data_source = None # 리소스 관리를 위해 외부 선언

                # extra_args를 미리 선언하여 UnboundLocalError 방지
                extra_args = {}
                content_type = 'application/octet-stream' # 기본값 설정
                src_etag = None

                # --- 사이즈에 따른 하이브리드 방식 전송, 기본 put_object 방식, 지정 사이스 이상은 Multipart Upload 방식 --
                try:
                    if self.cancel_requested: return False
                    
                    # 로컬 파일의 현재 수정 시간(Timestamp 문자열)
                    local_mtime_val = str(os.path.getmtime(src_p)) if is_local else None

                    # --- [동기화/Skip 로직] ---
                    if is_sync_enabled:
                        try:
                            # 타겟 S3에서 해당 파일의 메타데이터 확인
                            t_meta = tgt_s3.head_object(Bucket=self.cur_target_bucket, Key=tgt_k)
                            target_size = t_meta['ContentLength']
                                                        
                            # 로컬 파일인 경우: 사이즈, 생성시간 비교 (로컬 MD5 계산은 성능 저하 우려)
                            if is_local:
                                # 저장된 사용자 정의 메타데이터 추출 (boto3는 'Metadata' 딕셔너리에 담아줌)
                                s_metadata = t_meta.get('Metadata', {})
                                # S3 호환 저장소마다 키 대소문자 처리가 다를 수 있어 두 가지 모두 체크
                                stored_mtime = s_metadata.get('original-mtime') or s_metadata.get('original_mtime')

                                # 사이즈가 같고, 메타데이터에 기록된 수정시간이 로컬과 일치하면 Skip
                                if target_size == size and stored_mtime == local_mtime_val:
                                    with self.stats_lock:
                                        skip_count += 1
                                    self.log(f"⏭️ [SKIP] 메타데이터 일치(Sync): {tgt_k}")
                                    return True
                            
                            # 클라우드 소스인 경우: 사이즈와 ETag 모두 비교
                            else:
                                s_bucket = src_info.get("Bucket(or Root):", src_info.get("Source Bucket/Path:", "")).split('/')[0]
                                src_meta = src_s3.head_object(Bucket=s_bucket, Key=src_p)
                                src_etag = src_meta['ETag'].strip('"')
                                
                                # 1차 검증: 사이즈 비교 (가장 확실함)
                                if target_size == size:
                                    # 2차 검증: ETag 비교 (단일 업로드인 경우만 일치함)
                                    target_etag = t_meta['ETag'].strip('"')
        
                                    # ETag가 완벽히 같거나, 혹은 멀티파트 객체인 경우 사이즈만 믿고 Skip
                                    if src_etag == target_etag or "-" in target_etag:
                                        with self.stats_lock:
                                            skip_count += 1
                                        self.log(f"⏭️ [SKIP] 동일 Size 객체 확인(Cloud): {tgt_k}")
                                        return True
                        except:
                            # 타겟에 파일이 없는 경우(404) 등은 예외를 통과하여 전송 진행
                            pass

                    # --- [전송 준비 단계] ---
                    # 업로드 시 함께 보낼 메타데이터 및 설정 준비
                    if is_local:
                        data_source = open(src_p, 'rb')
                        content_type = 'application/octet-stream'
                        # 업로드 시 Metadata에 로컬 수정시간 포함
                        extra_args = {
                            'ContentType': content_type,
                            'Metadata': {'original-mtime': local_mtime_val}
                        }
                    else:
                        # 클라우드 소스 (S3 to ActiveScale)
                        s_bucket = src_info.get("Bucket(or Root):", src_info.get("Source Bucket/Path:", "")).split('/')[0]
                        response = src_s3.get_object(Bucket=s_bucket, Key=src_p)
                        data_source = io.BytesIO(response['Body'].read())
                        content_type = response.get('ContentType', 'application/octet-stream')
                        extra_args['ContentType'] = content_type
                        
                    if self.cancel_requested: return False

                    # --- [실제 전송 실행: ActiveScale 호환성 모드] ---
                    if size < THRESHOLD_BYTES:
                        if self.cancel_requested: return False

                        # 단일 PutObject (메타데이터 포함 시도)
                        self.log(f"🚀 [Put] {tgt_k} ({size/1024/1024:.1f}MB)")
                        # put_object는 Metadata를 직접 인자로 전달
                        tgt_s3.put_object(
                            Bucket=self.cur_target_bucket,
                            Key=tgt_k,
                            Body=data_source,
                            ContentType=content_type,
                            Metadata={'original-mtime': local_mtime_val} if is_local else {}
                        )
                    else:
                        if self.cancel_requested: return False
                        # [ActiveScale 수동 멀티파트]
                        self.log(f"📦 [Multipart-Manual] {tgt_k} ({size/1024/1024:.1f}MB)")
                        
                        # 1단계: 멀티파트 시작 (헤더 최소화)
                        # Metadata나 ContentType을 아예 제외하고 시작하여 NotImplemented 방지
                        mpu = tgt_s3.create_multipart_upload(
                            Bucket=self.cur_target_bucket,
                            Key=tgt_k
                        )
                        upload_id = mpu['UploadId']
                        parts = []

                        try:
                            part_number = 1
                            while True:
                                chunk = data_source.read(CHUNK_SIZE)
                                if not chunk: break
                                
                                # 2단계: 각 파트 업로드
                                # self.log(f"📦 [{tgt_k}] Part {part_number} 전송 중...", force_now=True)
                                part = tgt_s3.upload_part(
                                    Bucket=self.cur_target_bucket,
                                    Key=tgt_k,
                                    PartNumber=part_number,
                                    UploadId=upload_id,
                                    Body=chunk
                                )
                                parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
                                part_number += 1
                                if self.cancel_requested: raise Exception("Transfer Cancelled")

                            self.log(f"📦 [{tgt_k}] {len(parts)}개 파트 분할 전송 성공...", force_now=True)
                            # 3단계: 멀티파트 완료
                            tgt_s3.complete_multipart_upload(
                                Bucket=self.cur_target_bucket,
                                Key=tgt_k,
                                UploadId=upload_id,
                                MultipartUpload={'Parts': parts}
                            )
                        except Exception as e:
                            tgt_s3.abort_multipart_upload(Bucket=self.cur_target_bucket, Key=tgt_k, UploadId=upload_id)
                            raise e

                    # 타겟 메타데이터 획득 (검증 및 Skip 확인용)
                    v_meta = tgt_s3.head_object(Bucket=self.cur_target_bucket, Key=tgt_k)
                    v_etag = v_meta['ETag'].strip('"')

                    # [Checksum 무결성 검증 모드] 전송 후 확인
                    if is_verify_enabled and src_etag:
                        # ETag에 '-'가 포함되어 있는지 확인 (둘 중 하나라도 멀티파트라면 단순 비교 불가)
                        is_multipart = ("-" in src_etag) or ("-" in v_etag)

                        if is_multipart:
                            # 멀티파트 객체는 ETag 체계가 다르므로 크기 검증으로 대체
                            # v_size는 타겟 객체 정보(head_object 등)에서 가져온 값이어야 합니다.
                            if src_size == v_size: 
                                self.log(f"ℹ️ [Info] 멀티파트 객체 ETag 검증 Skip (크기 일치 확인): {tgt_k}")
                                is_verified = True
                            else:
                                self.log(f"❌ [Error] 크기 불일치: {tgt_k} (Src:{src_size} != Tgt:{v_size})")
                                is_verified = False
                        else:
                            # 둘 다 단일 객체인 경우 일반적인 MD5 Hash(ETag) 비교
                            if src_etag.strip('"') == v_etag.strip('"'): # 따옴표 제거 후 비교 권장
                                is_verified = True
                            else:
                                is_verified = False
            
                        if not is_verified:
                            raise Exception(f"Integrity Check Failed (Data Corrupted): {tgt_k}")
    
                        self.log(f"✅ [Verified] {tgt_k}")
                    else:
                        self.log(f"🚀 [Success] {tgt_k}")

                    with self.stats_lock:
                        success_count += 1
                        total_bytes += size
                    return True

                except Exception as e:
                    if self.cancel_requested: return False

                    # 동일 파일이 이미 있어 발생한 상황이거나 ETag mismatch인 경우 Skip 처리
                    # 에러 메시지에 'ETag mismatch'가 포함되어 있다면 skip_count로 돌림
                    if "ETag mismatch" in str(e):
                        with self.stats_lock:
                            skip_count += 1
                        self.log(f"⏭️ [SKIP] 동일 객체 확인 (ETag 검증 우회): {tgt_k}")
                        return True
                    
                    with self.stats_lock:
                        fail_count += 1
                    self.root.after(0, lambda k=tgt_k, err=str(e): self.log(f"❌ 실패 [{k}]: {err}", force_now=True))
                    return False
                finally:
                    # 스트림 리소스 해제
                    if data_source and hasattr(data_source, 'close'):
                        data_source.close()
            
            # 병렬 처리, 개수가 작으면 ActiveScale 부하 적음
            # PC의 사양과 ActiveScale 장비의 성능에 따라 user_workers 입력값 조정 필요(기본값 5)
            # Connection Timeout이 빈번하게 발생한다면 이 수치를 작게 하는 것이 좋음
            try:
                # ThreadPoolExecutor 시작 (with 문 사용 시 자동으로 종료 관리됨)
                with concurrent.futures.ThreadPoolExecutor(max_workers=user_workers) as executor:
                    self.log("🚀 스레드 풀 생성 완료, 작업 제출 중...", force_now=True)
                    # 작업 제출
                    futures = {executor.submit(upload_worker, t): t for t in all_tasks}
                    # 결과 수집 루프
                    for i, future in enumerate(concurrent.futures.as_completed(futures)):
                        # 10개 파일마다 한 번씩 GUI 이벤트를 강제로 처리 (버튼 클릭 감지용)
                        if i % 10 == 0: self.root.update()

                        # 중단 플래그 체크
                        if self.cancel_requested:
                            # 아직 실행되지 않은 작업들 취소 시도
                            for f in futures:
                                f.cancel()
                            # executor.shutdown(wait=False, cancel_futures=True)
                            self.log("🛑 중단 확인: 즉시 복귀합니다.", force_now=True)
                            return  # with 문을 벗어나며 executor가 정리됨
            
                        # 진행률 UI 업데이트
                        done = success_count + fail_count
                        prog = (done / total_count) * 100

                        self.root.after(0, lambda p=prog: self.progress.configure(value=p))
                        self.root.after(0, lambda d=done, t=total_count: self.status_msg.config(text=f"⏳ 전송 중... ({d}/{t})"))
                                                                
            except Exception as e:
                self.log(f"❌ 전송 루프 오류: {str(e)}", force_now=True)

            # 결과 요약 출력
            end_time = time.time()
            duration = max(time.time() - start_time, 0.1)
            mb_val = total_bytes / (1024 * 1024)
            size_str = f"{mb_val:.2f} MB" if mb_val >= 0.01 else f"{total_bytes/1024:.2f} KB"
            avg_speed = mb_val / duration if duration > 0 else 0

            summary = (
                f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"   전송 결과 요약 (Summary)\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"   • 전송 파일 수: {success_count} / {total_count} (Skip(Sync): {skip_count}  실패: {fail_count})\n"
                f"   • 총 전송 용량: {size_str}\n"
                f"   • 총 소요 시간: {duration:.2f} 초\n"
                f"   • 평균 전송 속도: {avg_speed:.2f} MB/s\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            )
            
            self.log(summary)
            self.status_msg.config(text="✅ 작업 완료", fg="blue")

            if success_count > 0:
                self.root.after(1000, lambda: self.refresh_list_thread(keep_log=True))

        except Exception as e:
            self.log(f"❌ 치명적 오류 발생: {str(e)}", force_now=True)
            self.root.after(0, lambda: self.status_msg.config(text="❌ 오류 발생", fg="red"))
            self.root.after(5000, lambda: self.status_msg.config(text="준비 완료", fg="black"))
        finally:
            self.reset_transfer_state()

    def reset_transfer_state(self):
        """전송이 끝난 후(성공, 실패, 중단 포함) 모든 UI와 플래그를 초기화"""
        # 플래그 초기화
        self.transfer_running = False
        self.is_transferring = False
        self.cancel_requested = False

        self.root.after(0, lambda: self.src_combo.config(state="readonly"))     # normal보다 readonly 권장
        # self.root.after(0, lambda: self.source_path_entry.config(state="normal"))
        src_path_widget = self.src_entries.get("Source Bucket/Path:")
        if src_path_widget:
             self.root.after(0, lambda: src_path_widget.config(state="normal"))
        
        # 버튼 상태 복구 (메인 스레드 안전하게)
        self.root.after(0, lambda: self.start_btn.config(state="normal"))
        self.root.after(0, lambda: self.stop_btn.config(
            state="disabled", 
            bg="#f0f0f0",      # 기본 회색
            fg="#a0a0a0",      # 회색 글자
            relief="raised",     # 살짝 들어간 형태
            cursor="arrow",
            bd=2
        ))

        # 프로그레스 바 및 상태 메시지 초기화
        self.root.after(0, lambda: self.progress.configure(value=0))
        # 특정 상황이 아니면 상태바 메시지를 비우거나 기본값으로 변경
        current_status = self.status_msg.cget("text")
        if "진행 중" in current_status or "준비 중" in current_status or "확인 중" in current_status or "중단 중"in current_status:
            self.root.after(0, lambda: self.status_msg.config(text="대기 중 (준비 완료)", fg="black"))

        self.log("🔄 UI 상태가 초기화되었습니다. (대기 모드)", force_now=True)

    # 원본 버킷 데이타 목록조회
    def check_bucket_files(self, mode="source"):
        """
        원본(Source) 데이터의 목록을 조회합니다. 
        """
        # 상태바 초기화: 검은색 글씨로 변경 및 메시지 갱신
        self.status_msg.config(text="⏳ Source 버킷 목록 조회 중...", fg="darkgreen", font=('맑은 고딕', 9))

        # if hasattr(self, 'clear_log'): 
            # self.clear_log()
        
        self.root.after(0, lambda: self.log("🔍 원본 데이터(Source) 목록 조회 시작"))

        try:
            # 로컬 파일 시스템 선택 시 예외 처리
            if "Local" in self.src_type_var.get():
                self.root.after(0, lambda: self.log("💻 모드: 로컬 파일 시스템"))
                messagebox.showinfo("알림", "로컬 경로는 '파일/폴더 선택' 버튼을 이용해 목록을 확인해 주세요.")
                self.status_msg.config(text="✅ 준비 완료", fg="blue")
                return

            # Source 입력 필드(딕셔너리)에서 데이터 추출 및 유효성 검사
            # setup_ui에서 정의한 self.src_entries 사용
            try:
                cfg = {
                    "mode": "source",
                    "ep": self.src_entries["Endpoint URL:"].get().strip(),
                    "ak": self.src_entries["Access Key:"].get().strip(),
                    "sk": self.src_entries["Secret Key:"].get().strip(),
                    "bucket": self.src_entries["Source Bucket/Path:"].get().strip(),
                    "filter": self.src_filter.get().strip().lower(),
                    "max": self.src_max.get().strip()
                }

            except KeyError as e:
                err_msg = f"⚠️ 설정 오류: Source 입력 필드를 찾을 수 없습니다. ({e})"
                self.root.after(0, lambda: self.log(err_msg))
                messagebox.showerror("프로그램 오류", err_msg)
                return

            # 필수 입력값 검증: Endpoint + Bucket만 검사
            if not cfg["ep"] or not cfg["bucket"]:
                err_msg = "Endpoint URL과 Bucket은 필수 입력 항목입니다."
                self.root.after(0, lambda: self.log(f"⚠️ 입력 확인: {err_msg}"))
                messagebox.showwarning("입력 확인", err_msg)
                self.status_msg.config(text="❌ 입력 미비", fg="red")
                return
            
            # URL 형식 검증 (http/https 누락 방지)
            if not cfg["ep"].startswith(('http://', 'https://')):
                err_msg = "Endpoint URL은 http:// 또는 https://로 시작해야 합니다."
                self.root.after(0, lambda: self.log(f"⚠️ 형식 오류: {err_msg}"))
                messagebox.showwarning("형식 확인", err_msg)
                self.status_msg.config(text="❌ URL 형식 오류", fg="red")
                return

            # Public / Private 모드 로그
            if not cfg["ak"] or not cfg["sk"]:
                self.root.after(0, lambda: self.log("🔓 Access/Secret Key 없음 → Public(Anonymous) Source 모드로 조회합니다."))
            else:
                self.root.after(0, lambda: self.log("🔐 인증된 Source 모드로 조회합니다."))

            # 백그라운드 스레드에서 목록 조회 실행
            self.root.after(0, lambda: self.log(f"🌐 서버 접속 시도 중: {cfg['ep']}"))

            # 조회 버튼 중복 클릭 방지가 필요 시 여기서 버튼을 Disable
            threading.Thread(target=self._fetch_list_worker, args=(cfg,), daemon=True).start()

        except Exception as e:
            self.root.after(0, lambda: self.log(f"❌ Source 조회 준비 중 예외 발생: {str(e)}"))
            self.root.after(0, lambda: self.status_msg.config(text="❌ 실행 에러", fg="red"))
            self.root.after(5000, lambda: self.status_msg.config(text="준비 완료", fg="black"))

    def _fetch_list_worker(self, cfg):
        try:
            # limit = int(cfg["max"]) if cfg["max"].isdigit() else 100
            try:
                limit = int(cfg["max"])
                if limit <= 0: limit = 1000  # 0 이하 입력 시 최대치인 1000(API 최대치)으로 설정
            except:
                limit = 100

            self.root.after(0, lambda: self.log(f"🔍 [{cfg['bucket']}] 버킷 조회 (필터: '{cfg['filter'] if cfg['filter'] else '전체'}', Max: {limit:,})"))

            # Public / Private Source 분기
            if not cfg["ak"] or not cfg["sk"]:
                s3 = boto3.client(
                    's3',
                    endpoint_url=cfg['ep'],
                    verify=False,
                    config=Config(
                        signature_version=UNSIGNED,
                        s3={'addressing_style': 'path'},
                        connect_timeout=5
                    )
                )
            else:
                s3 = boto3.client(
                    's3',
                    aws_access_key_id=cfg['ak'],
                    aws_secret_access_key=cfg['sk'],
                    endpoint_url=cfg['ep'],
                    verify=False,
                    config=Config(
                        signature_version='s3v4',
                        s3={'addressing_style': 'path'},
                        connect_timeout=5
                    )
                )
 
            # 기존 트리뷰 목록 초기화 (UI 업데이트는 메인 스레드에서 수행 권장)
            self.root.after(0, lambda: self.src_tree.delete(*self.src_tree.get_children()))

            # Paginator 사용으로 1000개 이상의 데이터도 안정적으로 처리
            paginator = s3.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=cfg['bucket'],
                PaginationConfig={'MaxItems': limit, 'PageSize': 1000}
            )

            items_to_insert = []
            count = 0
            total_size = 0
            KST = timezone(timedelta(hours=9))

            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        # 필터링
                        if cfg["filter"] and cfg["filter"] not in key.lower():
                            continue

                        # UTC -> KST (+9시간)
                        raw_time = obj['LastModified']
                        if raw_time.tzinfo is None:
                            raw_time = raw_time.replace(tzinfo=timezone.utc)
                        kst_time_str = raw_time.astimezone(KST).strftime('%Y-%m-%d %H:%M:%S')

                        f_size = self.format_size_windows_kb(obj['Size'])
                        
                        # 필터링된 결과 내에서 짝수/홀수 판단 (줄무늬 유지)
                        row_tag = 'oddrow' if count % 2 != 0 else 'evenrow'

                        # 데이터를 리스트에 보관
                        items_to_insert.append((key, f_size, kst_time_str, row_tag))
                        
                        count += 1
                        total_size += obj['Size']  # S3 객체 사이즈 합산
                        if count >= limit: break
                if count >= limit: break

            # [성능 개선] 메인 스레드에서 UI 한 번에 업데이트
            def update_ui(final_items, final_count, final_total):
                # 렌더링 중지 (깜빡임 방지)
                self.src_tree.update_idletasks() # 이전 작업 처리

                self.src_tree.delete(*self.src_tree.get_children())
                for item in final_items:
                    self.src_tree.insert("", "end", values=(item[0], item[1], item[2]), tags=(item[3],))
                
                size_str = self.get_readable_size(final_total)
                self.root.after(0, lambda c=final_count: self.log(f"✅ 조회 성공: 총 {c:,}개 항목 / 총 용량: {size_str}"))
                self.status_msg.config(text=f"✅ 조회 완료 ({final_count:,} items)", fg="blue")

                # 5초 뒤 상태바 초기화
                self.root.after(5000, lambda: self.status_msg.config(text="준비 완료", fg="black"))

            self.root.after(0, lambda: update_ui(items_to_insert, count, total_size))
            
        except Exception as e: 
            self.root.after(0, lambda: self.log(f"❌ 조회 실패: {str(e)}"))
            self.root.after(0, lambda: self.status_msg.config(text="❌ 조회 실패", fg="red"))
            self.root.after(5000, lambda: self.status_msg.config(text="준비 완료", fg="black"))

    # =========================================================
    # ----- Target(ActiveScale) 버킷 관련 프로세스 로직 ----------
    # =========================================================
    def get_client(self):
        try:
            s3_config = Config(
                signature_version='s3v4',
                s3={'addressing_style': 'path', 'payload_signing_enabled': False},
                retries={'max_attempts': 3}
            )
            # Target 전용이므로 tgt_entries를 직접 참조
            client = boto3.client(
                's3',
                endpoint_url=self.tgt_entries["Endpoint URL:"].get().strip(),
                aws_access_key_id=self.tgt_entries["Access Key:"].get().strip(),
                aws_secret_access_key=self.tgt_entries["Secret Key:"].get().strip(),
                verify=False, config=s3_config
            )
            def pre_sign_fix(request, **kwargs):
                request.headers['x-amz-content-sha256'] = 'UNSIGNED-PAYLOAD'
            client.meta.events.register('before-sign.s3.*', pre_sign_fix)
            return client
        except Exception as e:
            self.log(f"❌ Target 클라이언트 생성 실패: {e}", force_now=True)
            return None

    # ActiveScale 버킷 목록조회 GUI 멈춤 방지를 위한 스레드 래퍼
    def refresh_list_thread(self, keep_log=False):
        """
        스레드 시작 전 UI 값을 안전하게 캡처하여 전달
        """
        # 1. 메인 스레드에서 안전하게 UI 입력값들을 미리 읽어옴 (Thread-Safe)
        try:
            tgt_info = {k: v.get().strip() for k, v in self.tgt_entries.items()}
            filter_text = self.tgt_filter.get().strip().lower()
            
            try:
                val = int(self.tgt_max.get().strip())
                limit_keys = val if val > 0 else 1000
            except:
                limit_keys = 100
        
            # 캡처된 값들을 인자로 넘겨 스레드 실행
            threading.Thread(
                target=self.refresh_list, 
                args=(keep_log, tgt_info, filter_text, limit_keys), 
                daemon=True
            ).start()
        
        except Exception as e:
            self.log(f"❌ 입력값 읽기 실패: {str(e)}", force_now=True)

    # ActiveScale 버킷 목록조회
    def refresh_list(self, keep_log, tgt_info, filter_text, limit_keys):
        """
        인자로 받은 데이터를 사용하여 백그라운드 작업 수행
        """
        # 상태바 초기화 (UI 업데이트이므로 after 사용)
        self.status_msg.config(text="⏳ Target 버킷 목록 조회 중...", fg="darkgreen")

        # if not keep_log:
            # self.root.after(0, self.clear_log)

        try:
            # 이전 트리 목록 초기화 (after를 사용하여 메인스레드에서 실행)
            self.root.after(0, lambda: self.tgt_tree.delete(*self.tgt_tree.get_children()))

            self.root.after(0, lambda: self.log("⏳ Target 서버 연결 및 권한 확인 중..."))
            
            # 강화된 클라이언트 생성 및 권한 검토 (tgt_info 사용)
            client = self.create_s3_client(tgt_info) 
            bucket = tgt_info.get("Target Bucket:", "").strip()

            if not client:
                # create_s3_client 내부에서 이미 상세 로그를 남기므로 여기선 상태만 표시
                self.root.after(0, lambda: self.status_msg.config(text="❌ 연결/권한 실패", fg="red"))
                return
            
            if not bucket:
                self.root.after(0, lambda: self.log("❌ 오류: Target Bucket 명칭이 없습니다."))
                return

            self.root.after(0, lambda: self.log(f"🔍 '{bucket}' 버킷 조회 시도 중 (최대 {limit_keys:,}개)..."))

            # 페이징 처리를 활용한 데이터 조회
            paginator = client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=bucket, 
                PaginationConfig={'MaxItems': limit_keys, 'PageSize': 1000}
            )

            items_to_insert = []
            count = 0
            total_size = 0
            KST = timezone(timedelta(hours=9))
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if filter_text and filter_text not in key.lower(): 
                            continue
                    
                        raw_time = obj['LastModified']
                        if raw_time.tzinfo is None:
                            raw_time = raw_time.replace(tzinfo=timezone.utc)
                        kst_time_str = raw_time.astimezone(KST).strftime('%Y-%m-%d %H:%M:%S')

                        f_size = self.format_size_windows_kb(obj['Size'])
                        row_tag = 'oddrow' if count % 2 != 0 else 'evenrow'

                        items_to_insert.append((key, f_size, kst_time_str, row_tag))
                        count += 1
                        total_size += obj['Size']

                        if count >= limit_keys: break
                if count >= limit_keys: break

            # UI 업데이트 전담 함수
            def update_ui_bulk(final_items, final_count, final_total_size):
                for item in final_items:
                    self.tgt_tree.insert("", "end", iid=item[0], 
                                         values=(item[0], item[1], item[2]), 
                                         tags=(item[3],))
                
                if final_count > 0:
                    str_size = self.get_readable_size(final_total_size)
                    self.log(f"✅ 조회 성공: {final_count:,}개 항목 / 총 용량: {str_size}")
                    self.status_msg.config(text=f"✅ 조회 완료 ({final_count:,} items)", fg="blue")
                else:
                    self.log("✅ 조회 결과: 조건에 맞는 파일이 존재하지 않습니다.")
                    self.status_msg.config(text="✅ 결과 없음", fg="black")

            # 메인 스레드에 UI 업데이트 요청
            self.root.after(0, lambda: update_ui_bulk(items_to_insert, count, total_size))
            self.root.after(5000, lambda: self.status_msg.config(text="준비 완료", fg="black"))

        except Exception as e:
            err_msg = str(e)
            if "접속 실패" not in err_msg:
                self.root.after(0, lambda: self.log(f"❌ 조회 중 오류 발생: {err_msg}"))
            self.root.after(0, lambda: self.status_msg.config(text="❌ 조회 실패 (네트워크 확인)", fg="red"))
            self.root.after(5000, lambda: self.status_msg.config(text="준비 완료", fg="black"))

    def _manual_download(self, bucket, key, target_path):
        client = self.get_client()
        url = client.generate_presigned_url(
            ClientMethod='get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=3600
        )
        headers = {'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'}
        response = requests.get(url, headers=headers, verify=False, stream=True)
        if response.status_code == 200:
            with open(target_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=65536):
                    f.write(chunk)
        else:
            raise Exception(f"HTTP {response.status_code}")

    # ActiveScale 버킷 선택파일 다운로드
    def download_file(self):
        selected_keys = self.tgt_tree.selection()
        if not selected_keys:
            messagebox.showwarning("선택 확인", "파일을 선택해주세요.")
            return
            
        bucket = self.tgt_entries["Target Bucket:"].get().strip()
        
        # 저장 경로 결정
        if len(selected_keys) == 1:
            save_path = filedialog.asksaveasfilename(initialdir=os.getcwd(), initialfile=os.path.basename(selected_keys[0]), title="파일 저장", filetypes=(("모든 파일", "*.*"),))
            if not save_path: return
            target_list = [(selected_keys[0], save_path)]
        else:
            target_dir = filedialog.askdirectory(title="저장 폴더 선택")
            if not target_dir: return
            target_list = [(key, os.path.join(target_dir, os.path.basename(key))) for key in selected_keys]

        # --- 통계용 변수 초기화 ---
        total = len(target_list)
        success_count = 0
        total_size_bytes = 0
        global_start_time = time.time()

        skip_all = False
        overwrite_all = False

        self.status_msg.config(text="⏳ 파일 다운로드 시작...", fg="darkgreen", font=('맑은 고딕', 9))
        self.root.after(0, lambda: self.log(f"📥 다운로드 시작 (총 {total:,}개 항목)..."))
        for idx, (key, path) in enumerate(target_list):
            # 파일 중복 체크 로직
            if os.path.exists(path):
                if skip_all:
                    self.root.after(0, lambda: self.log(f"⏭️ 건너뜀 (모두 건너뛰기 설정됨): {key}"))
                    continue
                if not overwrite_all:
                    # 사용자에게 질문
                    answer = messagebox.askyesnocancel("파일 중복", f"이미 파일이 존재합니다:\n{os.path.basename(path)}\n\n덮어쓰시겠습니까?\n(아니오 클릭 시 건너뜁니다)")
                    
                    if answer is None: # 취소
                        self.root.after(0, lambda: self.log("🛑 사용자가 전체 작업을 취소했습니다."))
                        break
                    elif answer is False: # 아니오 (건너뛰기)
                        # 추가로 "모두 건너뛸지" 물어보기 (UI 간소화를 위해 메시지박스 활용)
                        if total - idx > 1:
                            if messagebox.askyesno("전체 적용", "남은 모든 중복 파일을 건너뛸까요?"):
                                skip_all = True
                        self.root.after(0, lambda: self.log(f"⏭️ 건너뜀: {key}"))
                        continue
                    else: # 예 (덮어쓰기)
                        if total - idx > 1:
                            if messagebox.askyesno("전체 적용", "남은 모든 중복 파일을 덮어쓸까요?"):
                                overwrite_all = True

            try:
                # Treeview에서 크기 문자열 가져오기 ("1,024 KB" 형태)
                file_size_raw = str(self.tgt_tree.item(key)['values'][1])
                
                # 숫자만 추출 (콤마, 공백, KB 제거)
                numeric_size = re.sub(r'[^0-9]', '', file_size_raw)
        
                # 크기 계산
                # 숫자가 비어있을 경우를 대비해 기본값 0 처리
                file_size_kb = int(numeric_size) if numeric_size else 0
                file_size_bytes = file_size_kb * 1024
        
                start_time = time.time()
                # 실제 다운로드 호출
                self._manual_download(bucket, key, path)
                duration = time.time() - start_time

                success_count += 1
                total_size_bytes += file_size_bytes

                size_mb = file_size_bytes / (1024 * 1024)
                speed = size_mb / duration if duration > 0 else 0
                
                # 로그 출력 및 progress바 업데이트
                self.root.after(0, lambda k=key, i=idx: self.log(f"  └─ [{i+1}/{total}] {os.path.basename(k)} 완료 ({size_mb:.2f} MB, {duration:.2f}초, {speed:.2f} MB/s)"))
                self.root.after(0, lambda v=((idx + 1) / total * 100): self.progress.configure(value=v))
                                              
                self.progress['value'] = ((idx + 1) / total) * 100
                # self.root.update()

            except Exception as e:
                self.root.after(0, lambda k=key, err=str(e): self.log(f"  └─ ❌ 실패({k}): {err}"))
                
        # --- 최종 요약 리포트 ---
        global_duration = time.time() - global_start_time
        total_mb = total_size_bytes / (1024 * 1024)
        avg_speed = total_mb / global_duration if global_duration > 0 else 0

        summary = (
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  다운로드 완료 보고서\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  • 성공/전체: {success_count} / {total}\n"
            f"  • 총 다운로드 용량: {total_mb:.2f} MB\n"
            f"  • 총 소요 시간: {global_duration:.2f} 초\n"
            f"  • 전체 평균 속도: {avg_speed:.2f} MB/s\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        self.root.after(0, lambda: self.log(summary))
        self.root.after(5000, lambda: self.status_msg.config(text="준비 완료", fg="black", font=('맑은 고딕', 9)))
        messagebox.showinfo("완료", "선택파일 다운로드 프로세스가 완료되었습니다.\n로그에서 상세 결과를 확인하세요.")    

        # 다운로드 완료 후 선택 하이라이트 해제
        self.tgt_tree.selection_remove(self.tgt_tree.selection())
        
        if success_count > 0:
            if total == 1: subprocess.Popen(f'explorer /select,"{os.path.abspath(target_list[0][1])}"')
            else: os.startfile(os.path.dirname(target_list[0][1]))
            
        self.root.after(2000, lambda: self.progress.config(value=0))

    # ActiveScale 버킷 선택파일 실행
    def run_file(self, event=None):
        selected = self.tgt_tree.selection()
        if not selected: return

        file_key = selected[0]
        bucket = self.tgt_entries["Target Bucket:"].get().strip()
        try:
            # temp_path = os.path.join(tempfile.gettempdir(), os.path.basename(file_key))
            # 사용자의 다운로드 폴더 경로를 자동으로 가져옴
            downloads_path = str(Path.home() / "Downloads")
            temp_path = os.path.join(downloads_path, os.path.basename(file_key))
            
            # self.root.after(0, lambda: self.log(f"📥 실행 준비 (임시폴더 다운로드): {file_key}"))
            self.root.after(0, lambda t_p=temp_path: self.log(f"📥 실행 준비 (다운로드 폴더): {t_p}"))

            self._manual_download(bucket, file_key, temp_path)
            if file_key.lower().endswith('.py'): subprocess.Popen(['python', temp_path], shell=True)
            else: os.startfile(temp_path)
            self.root.after(0, lambda: self.log(f"✅ 파일 실행 성공."))

            # 실행 후에도 선택 해제
            self.tgt_tree.selection_remove(file_key)

        except Exception as e:
            error_msg = str(e)
            self.root.after(0, lambda msg=error_msg: self.log(f"❌ 실행 실패: {msg}"))

    # ActiveScale 버킷 선택파일 삭제
    def delete_selected_files(self):
        selected_keys = self.tgt_tree.selection()
        if not selected_keys:
            messagebox.showwarning("삭제 확인", "삭제할 파일을 선택해주세요.")
            return

        # 커스텀 팝업 창 호출 (True/False 반환)
        if not self.custom_ask_delete(len(selected_keys)):
            # '아니오'를 누를 경우 선택 하이라이트 해제
            self.tgt_tree.selection_remove(selected_keys)
            self.root.after(0, lambda: self.log("↩️ 삭제가 취소되었습니다. "))
            return

        # 사용자가 명시적으로 '예'를 클릭한 경우에만 삭제 진행
        client = self.get_client()
        bucket = self.tgt_entries["Target Bucket:"].get().strip()
        
        success_count = 0
        self.status_msg.config(text="⏳ 파일 삭제 중...", fg="red", font=('맑은 고딕', 9))
        self.root.after(0, lambda: self.log(f"🔥 삭제 시작 ({len(selected_keys):,}개)..."))

        for key in selected_keys:
            try:
                # S3 객체 삭제 API 호출
                client.delete_object(Bucket=bucket, Key=key)
                success_count += 1
                self.status_msg.config(text="✅ 삭제 완료 및 목록 갱신 중", fg="blue", font=('맑은 고딕', 9, 'bold'))
                self.root.after(0, lambda: self.log(f"✅ 삭제 성공: {key}"))
            except Exception as e:
                self.root.after(0, lambda: self.log(f"❌ 삭제 실패({key}): {e}"))
                self.status_msg.config(text="❌ 삭제 실패", fg="red", font=('맑은 고딕', 9, 'bold'))

        self.root.after(0, lambda: self.log(f"📊 최종 결과: {success_count}/{len(selected_keys):,} 삭제 완료."))
        
        # 삭제 후 목록 새로고침
        self.refresh_list_thread(keep_log=True)

        self.root.after(5000, lambda: self.status_msg.config(text="준비 완료", fg="black", font=('맑은 고딕', 9)))

    def custom_ask_delete(self, count):
        # 결과값을 담을 변수
        self.confirm_res = False
        
        # 팝업 설정
        dialog = tk.Toplevel(self.root)
        dialog.title("정말 삭제하시겠습니까?")
        dialog.geometry("380x200")
        dialog.resizable(False, False)
        dialog.grab_set() # 팝업 뒤의 메인창 클릭 방지 (Modal)

        # 메시지 레이블
        tk.Label(dialog, text=f"선택한 {count}개의 파일을 삭제하시겠습니까?", font=('맑은 고딕', 10)).pack(pady=(20, 5))
        tk.Label(dialog, text="이 작업은 되돌릴 수 없습니다.", font=('맑은 고딕', 10)).pack()
        
        # [빨간색 깜빡임 레이블]
        blink_label = tk.Label(dialog, text="휴지통 이동 없이 영구삭제됩니다.", fg="red", font=('맑은 고딕', 10, 'bold'))
        blink_label.pack(pady=5)

        # 깜빡임 함수 정의
        def blink():
            if blink_label.winfo_exists(): # 창이 닫혔는지 확인
                current_color = blink_label.cget("foreground")
                next_color = "black" if current_color == "red" else "red"
                blink_label.config(fg=next_color)
                dialog.after(800, blink) # 0.5초~0.8초 주기가 경고 인지에 가장 효과적

        blink() # 깜빡임 시작

        # 버튼 영역
        btn_frame = tk.Frame(dialog)
        btn_frame.pack(pady=20)

        def on_yes():
            self.confirm_res = True
            dialog.destroy()

        def on_no():
            self.confirm_res = False
            dialog.destroy()

        # [아니오] 버튼에 포커스 설정 (실수 방지)
        btn_no = tk.Button(btn_frame, text="아니오", width=10, command=on_no)
        btn_no.pack(side="right", padx=10)
        btn_no.focus_set() # 기본 포커스

        btn_yes = tk.Button(btn_frame, text="예", width=10, command=on_yes, bg="#ffebee", fg="#c62828")
        btn_yes.pack(side="right", padx=10)

        # 엔터키 이벤트 바인딩 (현재 포커스된 버튼 실행)
        dialog.bind('<Return>', lambda e: dialog.focus_get().invoke())
        dialog.bind('<Escape>', lambda e: on_no())

        self.root.wait_window(dialog) # 창이 닫힐 때까지 대기
        return self.confirm_res

    def apply_initial_config(self):
        for k, v in self.default_config.items():
            if k in self.tgt_entries: 
                self.tgt_entries[k].insert(0, v)

if __name__ == "__main__":
    root = tk.Tk(); app = ActiveScaleObjectOperations(root); root.mainloop()