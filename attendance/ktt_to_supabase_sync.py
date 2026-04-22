import os
import sys
import configparser
import threading
import queue
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from datetime import datetime, timedelta
import json
import pyodbc
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
 
# ─────────────────────────────────────────────
#  설정 파일 경로
# ─────────────────────────────────────────────
CONFIG_FILE = "tiss_config.ini"
INI_FILE    = "tiss_db_upload_time.ini"

# .env 파일 로드
load_dotenv()

# 코드에 고정된 마스터 설정 (보안이 필요한 정보)
MASTER_CONFIG = {
    "mdb_path": r"C:\Program Files (x86)\KTT_FPReader\dbFolder\hds_fpsystem.mdb",
    "supabase_url": os.getenv("SUPABASE_URL", ""),
    "supabase_key": os.getenv("SUPABASE_KEY", ""),
}
 
MDB_PWD_CANDIDATES = [
    r"SysTools@365#CL",
    r"tjdgustltmxpa",
    r""
]
 
EMP_NUM_MAP = {
    "3050019": "1230084",
}
 
# ─────────────────────────────────────────────
#  설정 읽기/저장
# ─────────────────────────────────────────────
def load_app_config():
    cfg = configparser.ConfigParser()
    # 1. 기본 운영 설정값
    defaults = {
        "interval_min": "240",
        "auto_start": "0",
    }
    # 2. 파일에서 읽기
    if os.path.exists(CONFIG_FILE):
        cfg.read(CONFIG_FILE, encoding="utf-8")
    if not cfg.has_section("Settings"):
        cfg.add_section("Settings")

    # 3. 운영 설정값 보정 (없으면 기본값)
    for k, v in defaults.items():
        if not cfg.has_option("Settings", k):
            cfg.set("Settings", k, v)
    
    # 4. 중요 접속 정보는 파일 내용과 상관없이 MASTER_CONFIG로 강제 덮어쓰기
    for k, v in MASTER_CONFIG.items():
        cfg.set("Settings", k, v)

    return cfg
 
def save_app_config(cfg):
    """
    운영 설정(간격, 자동시작)만 추출하여 파일에 저장
    """
    save_cfg = configparser.ConfigParser()
    save_cfg.add_section("Settings")
    
    # 저장할 항목만 선별 (MDB, Supabase 관련은 제외)
    save_cfg.set("Settings", "interval_min", cfg.get("Settings", "interval_min"))
    save_cfg.set("Settings", "auto_start", cfg.get("Settings", "auto_start"))
    
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        save_cfg.write(f)

# ─────────────────────────────────────────────
#  INI 동기화 시간 관리
# ─────────────────────────────────────────────
def get_last_sync_times():
    config = configparser.ConfigParser()
    times = {}
    if os.path.exists(INI_FILE):
        config.read(INI_FILE, encoding="utf-8")
        if config.has_section("LastSyncTime"):
            for place in config.options("LastSyncTime"):
                times[place] = config.get("LastSyncTime", place)
    return times
 
def save_sync_times(times_dict):
    config = configparser.ConfigParser()
    config.add_section("LastSyncTime")
    for place, time_str in times_dict.items():
        config.set("LastSyncTime", place, time_str)
    with open(INI_FILE, "w", encoding="utf-8") as f:
        config.write(f)
 
# ─────────────────────────────────────────────
#  MDB 연결
# ─────────────────────────────────────────────
def get_mdb_connection(mdb_path):
    last_err = None
    for pwd in MDB_PWD_CANDIDATES:
        try:
            conn_str = (
                f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};"
                f"DBQ={mdb_path};PWD={pwd};"
            )
            conn = pyodbc.connect(conn_str, timeout=5)
            return conn, None
        except Exception as e:
            last_err = e
            continue
    return None, f"MDB 연결 실패 (경로/암호 확인): {last_err}"

# ─────────────────────────────────────────────
#  cursor 안전 조회 헬퍼
# ─────────────────────────────────────────────
def fetch_all(conn, query):
    """cursor를 명시적으로 열고 닫으며 결과 반환"""
    cur = conn.cursor()
    try:
        cur.execute(query)
        return cur.fetchall()
    finally:
        cur.close()

def read_sql_safe(conn, query):
    """pd.read_sql 대신 cursor를 직접 제어하여 DataFrame 반환"""
    cur = conn.cursor()
    try:
        cur.execute(query)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return pd.DataFrame.from_records(rows, columns=cols)
    finally:
        cur.close()
 
# ─────────────────────────────────────────────
#  동기화 로직 (백그라운드 스레드)
# ─────────────────────────────────────────────
def run_sync(mdb_path, supabase_url, supabase_key, log_queue):
    def log(msg, level="INFO"):
        ts = datetime.now().strftime("%H:%M:%S")
        log_queue.put(("log", f"[{ts}] [{level}] {msg}"))
 
    def update_stat(key, val):
        log_queue.put(("stat", key, val))
 
    log("동기화 시작...")
    update_stat("last_run", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    update_stat("status", "실행 중")
 
    now_dt  = datetime.now()
    # 특정일자만 Upload Test
    # test_date_str = "2026-04-01 23:59:59"
    # now_dt = datetime.strptime(test_date_str, "%Y-%m-%d %H:%M:%S")
    now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
 
    # MDB 연결
    conn, err = get_mdb_connection(mdb_path)
    if not conn:
        log(err, "ERROR"); update_stat("status", "오류"); return
 
    try:
        supabase = create_client(supabase_url, supabase_key)
 
        # 활성 리더기 탐색
        all_last_times = list(get_last_sync_times().values())
        if all_last_times:
            global_start_dt = min([datetime.strptime(t, "%Y-%m-%d %H:%M:%S") for t in all_last_times]) - timedelta(minutes=5)
        else:
            global_start_dt = now_dt - timedelta(days=1)
        global_start_str = global_start_dt.strftime("%Y-%m-%d %H:%M:%S")
 
        place_query = f"""
            SELECT DISTINCT str_accTerminalPlace
            FROM tb_workresult
            WHERE date_Attestation >= #{global_start_str}#
              AND date_Attestation <= #{now_str}#
              AND str_accTerminalPlace IS NOT NULL
              AND Len(str_workempNum) = 7
        """
        # cursor 명시적 해제
        places = [row[0] for row in fetch_all(conn, place_query)]

        log(f"활성 리더기 {len(places)}개 탐지: {places}")
        update_stat("readers", str(places))
 
        last_sync_times  = get_last_sync_times()
        all_processed    = []
 
        for place in places:
            last_time_str = last_sync_times.get(place.lower(),
                (now_dt - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"))
            start_dt    = datetime.strptime(last_time_str, "%Y-%m-%d %H:%M:%S") - timedelta(minutes=5)
            start_search = start_dt.strftime("%Y-%m-%d %H:%M:%S")
            log(f"[{place}] 조회: {start_search} ~ {now_str}")
 
            query = f"""
                SELECT date_Attestation, str_workempNum, str_workempName, str_accTerminalPlace
                FROM tb_workresult
                WHERE date_Attestation >= #{start_search}#
                  AND date_Attestation <= #{now_str}#
                  AND str_accTerminalPlace = '{place}'
                  AND Len(str_workempNum) = 7
            """
            # pd.read_sql → cursor 직접 제어
            df_part = read_sql_safe(conn, query)

            if not df_part.empty:
                all_processed.append(df_part)
                last_sync_times[place.lower()] = df_part["date_Attestation"].max().strftime("%Y-%m-%d %H:%M:%S")
                log(f"[{place}] {len(df_part)}건 조회")
 
        if not all_processed:
            log("새로운 데이터 없음", "OK")
            update_stat("status", "대기 중")
            return
 
        df = pd.concat(all_processed)
        df["date_Attestation"] = pd.to_datetime(df["date_Attestation"])
        df["only_date"] = df["date_Attestation"].dt.date
 
        first_in = df.loc[df.groupby(["str_workempNum","only_date"])["date_Attestation"].idxmin()]
        last_out = df.loc[df.groupby(["str_workempNum","only_date"])["date_Attestation"].idxmax()]
        final_df = pd.concat([first_in, last_out]).drop_duplicates().sort_values(by="date_Attestation")
 
        log(f"처리 대상: {len(final_df)}건")
        inserted = updated = skipped = 0
 
        for _, row in final_df.iterrows():
            emp_num_raw = str(row["str_workempNum"]).strip()
            event_dt    = row["date_Attestation"]
            e_date      = event_dt.strftime("%Y-%m-%d")
            e_time      = event_dt.strftime("%H:%M:%S")
            emp_no      = EMP_NUM_MAP.get(emp_num_raw, emp_num_raw)
 
            user_res = supabase.from_("profiles").select("user_id").eq("emp_no", emp_no).execute()
            if not user_res.data:
                log(f"사원번호 {emp_no} → profiles 없음, 건너뜀", "WARN")
                skipped += 1; continue
            user_id = user_res.data[0]["user_id"]
 
            existing = supabase.from_("attendance_status").select("*") \
                .eq("user_id", user_id).eq("status_code", "AT990").eq("start_date", e_date).execute()
 
            if existing.data:
                ext       = existing.data[0]
                new_start = e_time if e_time < ext["start_time"] else ext["start_time"]
                new_end   = e_time if e_time > (ext["end_time"] or "00:00:00") else ext["end_time"]
                supabase.from_("attendance_status").update({
                    "start_time": new_start,
                    "end_time": new_end,
                    "updated_at": datetime.now().isoformat(),
                    "work_description": "지문인식기 로그 KTT Auto 갱신"
                }).eq("id", ext["id"]).execute()
                log(f"UPDATE {emp_no} {e_date} {new_start}~{new_end}")
                updated += 1
            else:
                supabase.from_("attendance_status").insert({
                    "user_id": user_id,
                    "status_code": "AT990",
                    "start_date": e_date,
                    "end_date": e_date,
                    "start_time": e_time,
                    "end_time": e_time,
                    "work_description": "지문인식기 로그 신규 Insert",
                    "last_modifier": "System_Auto"
                }).execute()
                log(f"INSERT {emp_no} {e_date} {e_time}")
                inserted += 1
 
        save_sync_times(last_sync_times)
        summary = f"완료 — 신규:{inserted} 수정:{updated} 건너뜀:{skipped}"
        log(summary, "OK")
        update_stat("last_result", summary)
        update_stat("status", "대기 중")
        log_queue.put(("ini_refresh", None))
 
    except Exception as e:
        log(f"오류: {e}", "ERROR")
        update_stat("status", "오류")
    finally:
        # autocommit 상태 확인 후 rollback으로 잔여 트랜잭션 정리
        try:
            conn.rollback()
        except Exception:
            pass
        conn.close()
 
# ─────────────────────────────────────────────
#  메인 GUI
# ─────────────────────────────────────────────
class KTTSyncApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("KT텔레캅 지문인식기 ↔ TiSS(Supabase) 동기화")
        self.geometry("950x650")
        self.resizable(True, True)
        self.configure(bg="#1e1e2e")
 
        self.cfg        = load_app_config()
        self.log_queue  = queue.Queue()
        self.timer_job  = None
        self.is_running = False
        self.countdown  = 0
 
        self._build_ui()
        self._load_ini_display()
        self._poll_queue()
 
        if self.cfg.get("Settings", "auto_start") == "1":
            self.after(1000, self._start_auto)
 
    # ── UI 구성 ──────────────────────────────
    def _build_ui(self):
        DARK   = "#1e1e2e"
        PANEL  = "#2a2a3e"
        ACCENT = "#7c6af7"
        TEXT   = "#cdd6f4"
        GREEN  = "#a6e3a1"
        RED    = "#f38ba8"
        YELLOW = "#f9e2af"
 
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("TNotebook",         background=DARK,  borderwidth=0)
        style.configure("TNotebook.Tab",     background=PANEL, foreground=TEXT,
                        padding=[14,6], font=("Malgun Gothic", 10))
        style.map("TNotebook.Tab",           background=[("selected", ACCENT)])
        style.configure("TFrame",            background=DARK)
        style.configure("TLabel",            background=DARK,  foreground=TEXT,
                        font=("Malgun Gothic", 10))
        style.configure("TEntry",            fieldbackground=PANEL, foreground=TEXT,
                        insertcolor=TEXT,  font=("Malgun Gothic", 10))
        style.configure("TButton",           background=ACCENT, foreground="#ffffff",
                        font=("Malgun Gothic", 10, "bold"), borderwidth=0)
        style.map("TButton",                 background=[("active", "#9b8fff")])
        style.configure("Danger.TButton",    background=RED,    foreground="#1e1e2e")
        style.map("Danger.TButton",          background=[("active", "#ff9eb5")])
        style.configure("TCheckbutton",      background=DARK,   foreground=TEXT,
                        font=("Malgun Gothic", 10))
        style.configure("TLabelframe",       background=DARK,   foreground=ACCENT,
                        font=("Malgun Gothic", 10, "bold"))
        style.configure("TLabelframe.Label", background=DARK,   foreground=ACCENT)
 
        self.colors = {"DARK": DARK, "PANEL": PANEL, "ACCENT": ACCENT,
                       "TEXT": TEXT, "GREEN": GREEN, "RED": RED, "YELLOW": YELLOW}
 
        # ── 상단 헤더 ──
        hdr = tk.Frame(self, bg=PANEL, height=52)
        hdr.pack(fill="x")
        tk.Label(hdr, text="🔌 KTT Fingerprint(mdb) to TiSS(Supabase DB) Sync Tool", bg=PANEL, fg=ACCENT,
                 font=("Malgun Gothic", 15, "bold")).pack(side="left", padx=18, pady=10)
        self.lbl_status = tk.Label(hdr, text="● 대기 중", bg=PANEL, fg=GREEN,
                                   font=("Malgun Gothic", 11, "bold"))
        self.lbl_status.pack(side="right", padx=18)
        self.lbl_countdown = tk.Label(hdr, text="", bg=PANEL, fg=YELLOW,
                                      font=("Malgun Gothic", 10))
        self.lbl_countdown.pack(side="right", padx=8)
 
        # ── 탭 ──
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True, padx=10, pady=8)
 
        tab_main    = ttk.Frame(nb)
        tab_config  = ttk.Frame(nb)
        tab_ini     = ttk.Frame(nb)
 
        nb.add(tab_main,   text="  📋 모니터링  ")
        nb.add(tab_config, text="  ⚙️  설정  ")
        nb.add(tab_ini,    text="  🕐 동기화 이력  ")

        nb.bind("<<NotebookTabChanged>>", self._on_tab_changed)
 
        self._build_tab_main(tab_main)
        self._build_tab_config(tab_config)
        self._build_tab_ini(tab_ini)
 
    # ── 탭 1: 모니터링 ───────────────────────
    def _build_tab_main(self, parent):
        C = self.colors
 
        # 상태 카드 행
        card_row = tk.Frame(parent, bg=C["DARK"])
        card_row.pack(fill="x", padx=8, pady=(10,4))
 
        self.stat_vars = {}
        cards = [
            ("last_run",    "마지막 실행",   "—"),
            ("last_result", "마지막 결과",   "—"),
            ("readers",     "활성 리더기",   "—"),
            ("status",      "상태",         "대기 중"),
        ]
        for key, label, default in cards:
            f = tk.Frame(card_row, bg=C["PANEL"], bd=0, padx=12, pady=8)
            f.pack(side="left", fill="x", expand=True, padx=4)
            tk.Label(f, text=label, bg=C["PANEL"], fg="#888aaa",
                     font=("Malgun Gothic", 8)).pack(anchor="w")
            var = tk.StringVar(value=default)
            self.stat_vars[key] = var
            tk.Label(f, textvariable=var, bg=C["PANEL"], fg=C["TEXT"],
                     font=("Malgun Gothic", 10, "bold"),
                     wraplength=160, justify="left").pack(anchor="w")
 
        # 버튼
        btn_row = tk.Frame(parent, bg=C["DARK"])
        btn_row.pack(fill="x", padx=8, pady=4)
        self.btn_run = ttk.Button(btn_row, text="▶  지금 동기화",
                                  command=self._manual_run)
        self.btn_run.pack(side="left", padx=(0,6))
        self.btn_auto = ttk.Button(btn_row, text="⏱  자동 시작",
                                   command=self._toggle_auto)
        self.btn_auto.pack(side="left", padx=(0,6))
        ttk.Button(btn_row, text="🗑  로그 지우기",
                   command=self._clear_log, style="Danger.TButton").pack(side="left")
 
        # 로그 영역
        log_frame = ttk.LabelFrame(parent, text=" 실행 로그 ", padding=6)
        log_frame.pack(fill="both", expand=True, padx=8, pady=(4,8))
 
        self.log_box = scrolledtext.ScrolledText(
            log_frame, bg="#11111b", fg=C["TEXT"],
            font=("Consolas", 10), insertbackground=C["TEXT"],
            relief="flat", bd=0, wrap="word"
        )
        self.log_box.pack(fill="both", expand=True)
        # 색상 태그
        self.log_box.tag_config("ERROR",  foreground="#f38ba8")
        self.log_box.tag_config("WARN",   foreground="#f9e2af")
        self.log_box.tag_config("OK",     foreground="#a6e3a1")
        self.log_box.tag_config("INFO",   foreground="#cdd6f4")
 
    # ── 탭 2: 설정 ───────────────────────────
    def _build_tab_config(self, parent):
        C = self.colors
        pad = {"padx": 10, "pady": 5}
 
        frm = ttk.LabelFrame(parent, text=" 시스템 접속 정보 (고정) ", padding=12)
        frm.pack(fill="x", padx=12, pady=12)
        frm.columnconfigure(1, weight=1)
 
        fields = [
            ("MDB 경로",        "mdb_path"),
            ("Supabase URL",    "supabase_url"),
            ("Supabase Key",    "supabase_key"),
        ]
        self.cfg_entries = {}
        for r, (label, key) in enumerate(fields):
            ttk.Label(frm, text=label).grid(row=r, column=0, sticky="w", **pad)
            e = ttk.Entry(frm, width=70,
                          show="*" if "key" in key else "")
            e.insert(0, self.cfg.get("Settings", key))
            e.configure(state="readonly")
            e.grid(row=r, column=1, sticky="ew", **pad)
            self.cfg_entries[key] = e
 
        # 자동 실행 간격
        frm2 = ttk.LabelFrame(parent, text=" 자동 동기화 ", padding=12)
        frm2.pack(fill="x", padx=12, pady=4)
        frm2.columnconfigure(1, weight=1)
 
        ttk.Label(frm2, text="실행 간격 (분)").grid(row=0, column=0, sticky="w", **pad)
        self.entry_interval = ttk.Entry(frm2, width=10)
        self.entry_interval.insert(0, self.cfg.get("Settings", "interval_min"))
        self.entry_interval.grid(row=0, column=1, sticky="w", **pad)
 
        self.var_autostart = tk.IntVar(value=int(self.cfg.get("Settings","auto_start")))
        ttk.Checkbutton(frm2, text="프로그램 시작 시 자동 실행",
                        variable=self.var_autostart).grid(row=1, column=0,
                        columnspan=2, sticky="w", **pad)
 
        ttk.Button(parent, text="💾  설정 저장",
                   command=self._save_config).pack(pady=12)
 
    # ── 탭 3: 동기화 이력 (INI) ──────────────
    def _build_tab_ini(self, parent):
        C = self.colors
        btn_row = tk.Frame(parent, bg=C["DARK"])
        btn_row.pack(fill="x", padx=8, pady=8)
        ttk.Button(btn_row, text="🔄  새로고침",
                   command=self._load_ini_display).pack(side="left", padx=4)
        """
        동기화 이력파일(supabase_upload_times.ini) 초기화 막음
        ttk.Button(btn_row, text="🗑  이력 초기화",
                   command=self._reset_ini,
                   style="Danger.TButton").pack(side="left", padx=4)
        """
 
        cols = ("리더기", "마지막 동기화 시간")
        self.tree = ttk.Treeview(parent, columns=cols, show="headings", height=16)
        style = ttk.Style()
        style.configure("Treeview",
                        background=C["PANEL"], fieldbackground=C["PANEL"],
                        foreground=C["TEXT"], font=("Malgun Gothic", 10),
                        rowheight=28)
        style.configure("Treeview.Heading",
                        background=C["ACCENT"], foreground="#ffffff",
                        font=("Malgun Gothic", 10, "bold"))
        for col in cols:
            self.tree.heading(col, text=col)
            self.tree.column(col, anchor="center",
                             width=200 if col == "리더기" else 300)
 
        vsb = ttk.Scrollbar(parent, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True, padx=(8,0), pady=4)
        vsb.pack(side="right", fill="y", pady=4, padx=(0,8))
 
        # INI 파일 경로 표시
        tk.Label(parent,
                 text=f"INI 파일: {os.path.abspath(INI_FILE)}",
                 bg=C["DARK"], fg="#ffffff",
                 font=("Malgun Gothic", 8)).pack(anchor="w", padx=8, pady=2)
    
    def _on_tab_changed(self, event):
        # 현재 선택된 탭 위젯을 가져옴
        selected_tab = event.widget.select()
        # 해당 탭의 텍스트(이름)를 확인
        tab_text = event.widget.tab(selected_tab, "text")

        # " 설정 " 이라는 글자가 포함된 탭으로 이동했을 때
        if "설정" in tab_text:
            # Entry 위젯에 포커스를 줌
            if hasattr(self, 'entry_interval'):
                self.entry_interval.focus_set()
                # 입력된 텍스트 전체 선택 (선택 사항: 바로 수정하기 편함)
                self.entry_interval.selection_range(0, tk.END)
 
    # ── 액션 ─────────────────────────────────
    def _manual_run(self):
        if self.is_running:
            messagebox.showinfo("알림", "이미 동기화가 진행 중입니다."); return
        self._fire_sync()
 
    def _fire_sync(self):
        self.is_running = True
        self.btn_run.config(state="disabled")
        self._set_status("실행 중", "yellow")
        mdb  = self.cfg.get("Settings","mdb_path")
        url  = self.cfg.get("Settings","supabase_url")
        key  = self.cfg.get("Settings","supabase_key")
        t = threading.Thread(target=run_sync,
                             args=(mdb, url, key, self.log_queue),
                             daemon=True)
        t.start()
 
    def _toggle_auto(self):
        if self.timer_job:
            self.after_cancel(self.timer_job)
            self.timer_job = None
            self.btn_auto.config(text="⏱  자동 시작")
            self.lbl_countdown.config(text="")
            self._append_log("자동 동기화 중지됨", "WARN")
        else:
            self.btn_auto.config(text="⏹  자동 중지")
            self._append_log("자동 동기화 시작됨", "OK")
            self._start_auto()
 
    def _start_auto(self):
        self.btn_auto.config(text="⏹  자동 중지")
        self._fire_sync()
        interval_min = int(self.cfg.get("Settings","interval_min") or 240)
        self.countdown = interval_min * 60
        self._tick()
 
    def _tick(self):
        if self.countdown > 0:
            mins, secs = divmod(self.countdown, 60)
            self.lbl_countdown.config(text=f"다음 실행까지 {mins:02d}:{secs:02d}")
            self.countdown -= 1
            self.timer_job = self.after(1000, self._tick)
        else:
            self.lbl_countdown.config(text="")
            self._fire_sync()
            interval_min = int(self.cfg.get("Settings","interval_min") or 240)
            self.countdown = interval_min * 60
            self.timer_job = self.after(1000, self._tick)
 
    def _save_config(self):
        for key, entry in self.cfg_entries.items():
            self.cfg.set("Settings", key, entry.get())
        self.cfg.set("Settings","interval_min", self.entry_interval.get())
        self.cfg.set("Settings","auto_start",   str(self.var_autostart.get()))
        save_app_config(self.cfg)
        messagebox.showinfo("저장 완료", "설정이 저장되었습니다.")
 
    def _load_ini_display(self):
        for row in self.tree.get_children():
            self.tree.delete(row)
        times = get_last_sync_times()
        if times:
            for place, ts in sorted(times.items()):
                self.tree.insert("", "end", values=(place, ts))
        else:
            self.tree.insert("", "end", values=("(이력 없음)", "—"))
 
    def _reset_ini(self):
        if messagebox.askyesno("확인", "동기화 이력을 모두 초기화하시겠습니까?"):
            save_sync_times({})
            self._load_ini_display()
            self._append_log("동기화 이력 초기화됨", "WARN")
 
    def _clear_log(self):
        self.log_box.configure(state="normal")
        self.log_box.delete("1.0", "end")
        self.log_box.configure(state="disabled")
 
    # ── 상태 표시 ─────────────────────────────
    def _set_status(self, text, color="green"):
        palette = {"green": "#a6e3a1", "yellow": "#f9e2af", "red": "#f38ba8"}
        fg = palette.get(color, "#cdd6f4")
        self.lbl_status.config(text=f"● {text}", fg=fg)
 
    def _append_log(self, msg, level="INFO"):
        self.log_box.configure(state="normal")
        self.log_box.insert("end", msg + "\n", level)
        self.log_box.see("end")
        self.log_box.configure(state="disabled")
 
    # ── 큐 폴링 ──────────────────────────────
    def _poll_queue(self):
        try:
            while True:
                item = self.log_queue.get_nowait()
                if item[0] == "log":
                    _, msg = item
                    level = "INFO"
                    for lv in ("ERROR","WARN","OK"):
                        if f"[{lv}]" in msg:
                            level = lv; break
                    self._append_log(msg, level)
                    if level == "ERROR":
                        self._set_status("오류", "red")
                    # 스레드 완료 감지
                    if "[OK]" in msg or "[ERROR]" in msg:
                        self.is_running = False
                        self.btn_run.config(state="normal")
                        if level != "ERROR":
                            self._set_status("대기 중", "green")
                elif item[0] == "stat":
                    _, key, val = item
                    if key in self.stat_vars:
                        self.stat_vars[key].set(val)
                    if key == "status":
                        color = "yellow" if val == "실행 중" else \
                                "red"    if val == "오류"   else "green"
                        self._set_status(val, color)
                elif item[0] == "ini_refresh":
                    self._load_ini_display()
        except queue.Empty:
            pass
        self.after(200, self._poll_queue)
 
    def on_close(self):
        if self.timer_job:
            self.after_cancel(self.timer_job)
            self.timer_job = None

        # 동기화 실행 중이면 완료 대기 안내
        if self.is_running:
            if not messagebox.askyesno(
                "종료 확인",
                "동기화가 진행 중입니다.\n강제 종료하면 MDB 락 파일이 잔류할 수 있습니다.\n그래도 종료하시겠습니까?"
            ):
                return
            
        self.destroy()
 
if __name__ == "__main__":
    app = KTTSyncApp()
    app.protocol("WM_DELETE_WINDOW", app.on_close)
    app.mainloop()