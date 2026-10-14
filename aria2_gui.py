"""拾流 FlowDock：基于 aria2 的桌面下载器，支持队列、续传和实时日志。"""

# 常用参数：也可以在添加任务的窗口中选择保存目录。
from pathlib import Path

APP_DIRECTORY = Path(__file__).resolve().parent
ARIA2_PATH = APP_DIRECTORY / "aria2-1.37.0-win-64bit-build1" / "aria2c.exe"
DEFAULT_DOWNLOAD_DIRECTORY = APP_DIRECTORY / "Downloads"
MAX_ACTIVE_DOWNLOADS = 3
MAX_CONCURRENT_DOWNLOADS_SETTING = 16  # 设置界面允许选择 1～16 个同时下载的文件。
ASSET_DIRECTORY = APP_DIRECTORY / "flowdock_assets"

# 高级参数：通常不需要修改。
STATE_DIRECTORY = APP_DIRECTORY / "aria2_gui_data"
STATE_FILE = STATE_DIRECTORY / "tasks.json"
REFRESH_INTERVAL_MS = 250
MAX_VISIBLE_LOG_LINES = 1500
CONNECTIONS_PER_DOWNLOAD = 4
RPC_TIMEOUT_SECONDS = 8
RPC_WARNING_INTERVAL_SECONDS = 30

# 界面颜色集中设置，方便后续调整外观。
PAGE_COLOR = "#f5f7fb"
SURFACE_COLOR = "#ffffff"
TEXT_COLOR = "#1d2940"
MUTED_COLOR = "#8b96aa"
ACCENT_COLOR = "#5267e9"
LINE_COLOR = "#edf0f6"
CURRENT_THEME = "day"
NIGHT_COLORS = {
    "white": "#172033", "#ffffff": "#172033", "#f5f7fb": "#0e1525",
    "#1d2940": "#e4eaf5", "#8b96aa": "#9aa9c0", "#edf0f6": "#2a354b",
    "#f1f4fa": "#263149", "#536079": "#bdc8dc", "#e6ebf5": "#34415d",
    "#f0f3ff": "#26375b", "#f9faff": "#1c2942", "#edf0ff": "#263454",
    "#6374c5": "#a3b3f2", "#edf0f5": "#303c52", "#e8f8f1": "#183e35",
    "#19916c": "#6bd9af", "#fff0f2": "#492a39", "#d35167": "#ff9aab",
    "#fff6e7": "#453c2b", "#ba8b36": "#ecc47d", "#eef0ff": "#2b365b",
    "#5b6bdd": "#acb8ff", "#f1f4f8": "#283349", "#8792a5": "#afbdd2",
    "#617089": "#bdc9dd", "#a4adbd": "#8596b1", "#f5f7fc": "#202b42",
    "#79849a": "#b3bfd4", "#f7f9fd": "#1b2940", "#a0a9ba": "#97a8c2",
    "#e9edfc": "#273552", "#dfe5fb": "#33456c", "#a0aabd": "#a9b7ce",
    "#f6f8fc": "#243149", "#edf1fa": "#35435d", "#fafbfe": "#1e2a3f",
    "#96a0b3": "#a5b5cf", "#96a1b4": "#94a5bf", "#e9edf5": "#34405a",
    "#67748c": "#b6c5dd", "#f3f5fa": "#2b3750", "#70809a": "#a6b6d0",
    "#73819a": "#b4c1d8", "#eef1ff": "#27385c",
}

import collections
import datetime
import hashlib
import json
import os
import queue
import re
import secrets
import socket
import subprocess
import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from urllib.parse import quote, unquote, urlparse
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener


def theme_color(color):
    """把日间配色转换为当前主题的颜色；输入、返回均为颜色字符串。"""
    if CURRENT_THEME == "night":
        return NIGHT_COLORS.get(color.lower(), color)
    return color


def theme_canvas(canvas):
    """更新已经绘制的画布图形，使表头和任务行跟随当前主题。"""
    reverse_colors = {night: day for day, night in NIGHT_COLORS.items()}
    for item in canvas.find_all():
        for option in ("fill", "outline"):
            try:
                color = canvas.itemcget(item, option)
                original = reverse_colors.get(color, color) if CURRENT_THEME == "day" else color
                canvas.itemconfigure(item, **{option: theme_color(original)})
            except tk.TclError:
                continue  # 图片没有 fill/outline 属性。


def readable_size(byte_count):
    """把字节数转为便于阅读的大小；输入字节数，返回文字。"""
    size = float(byte_count)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}"
        size /= 1024


def zenodo_record_id(address):
    """识别正式 Zenodo 记录页面、记录编号或 DOI，返回编号或 None。"""
    address = address.strip()
    if address.isdigit():
        return address
    match = re.fullmatch(r"(?:https?://doi\.org/)?10\.5281/zenodo\.(\d+)", address)
    if match:
        return match.group(1)
    parsed = urlparse(address)
    if parsed.scheme in ("http", "https") and parsed.hostname in ("zenodo.org", "www.zenodo.org"):
        match = re.fullmatch(r"/records?/(\d+)/?", parsed.path)
        if match:
            return match.group(1)
    return None


def fetch_zenodo_files(address):
    """读取公开记录元数据，返回标题和文件清单；不下载研究文件。"""
    record_id = zenodo_record_id(address)
    if not record_id:
        raise ValueError("请输入 Zenodo 记录页面、数字编号或 10.5281/zenodo.… 格式的 DOI。")
    request = Request(f"https://zenodo.org/api/records/{record_id}",
                      headers={"Accept": "application/json", "User-Agent": "FlowDock/1.0 (Zenodo record file importer)"})
    for attempt in range(3):
        try:
            with build_opener().open(request, timeout=25) as response:
                record = json.load(response)
            break
        except HTTPError as error:
            if error.code == 429 and attempt < 2:
                retry_after = error.headers.get("Retry-After", "")
                delay = int(retry_after) if retry_after.isdigit() else 5 * (2 ** attempt)
                if delay > 60:
                    raise ValueError("Zenodo 正在限流，请稍后再解析；软件不会绕过服务端限制。") from error
                time.sleep(max(1, delay))
                continue
            descriptions = {403: "记录访问受限或被服务器拒绝", 404: "记录不存在", 429: "请求过多，请稍后再试"}
            raise ValueError(f"Zenodo 返回 HTTP {error.code}：{descriptions.get(error.code, '服务暂时不可用')}。") from error
        except (URLError, TimeoutError, OSError) as error:
            raise ValueError(f"无法连接 Zenodo，请检查网络或系统代理：{error}") from error
    raw_files = record.get("files", [])
    if isinstance(raw_files, dict):
        raw_files = list(raw_files.get("entries", {}).values())
    files = []
    for entry in raw_files:
        name = entry.get("key") or entry.get("filename")
        if not name or Path(name).name != name or name in (".", "..") or any(character in name for character in '\\/:*?"<>|'):
            raise ValueError("记录中存在 Windows 无法保存的文件名，请到 Zenodo 页面检查。")
        links = entry.get("links", {})
        link = links.get("content") or links.get("download")
        if not link:
            link = f"https://zenodo.org/records/{record_id}/files/{quote(name, safe='')}?download=1"
        files.append({"name": name, "size": int(entry.get("size", 0)),
                      "url": link, "checksum": entry.get("checksum", "")})
    if not files:
        raise ValueError("该记录没有可直接下载的文件，可能尚未公开或需要登录授权。")
    return record.get("metadata", {}).get("title", "Zenodo 记录"), files


def verify_download_file(path, expected_checksum, expected_size, on_progress=None, should_stop=None):
    """流式校验文件大小与公开摘要，返回校验说明；失败时抛出中文异常。"""
    path = Path(path)
    if expected_size and path.stat().st_size != expected_size:
        raise ValueError("文件大小与 Zenodo 公布值不一致，请检查下载文件。")
    if not expected_checksum:
        if expected_size:
            return "文件大小检查通过；Zenodo 未提供摘要，无法核验内容摘要。"
        return "Zenodo 未提供可核验的大小和摘要，无法确认文件内容完整性。"
    algorithm, separator, expected = expected_checksum.partition(":")
    if not separator:
        algorithm, expected = "md5", expected_checksum
    algorithm = algorithm.lower().replace("-", "")
    if algorithm not in ("md5", "sha1", "sha256", "sha512"):
        raise ValueError(f"暂不支持 {algorithm} 校验，不能确认文件内容完整性。")
    digest = hashlib.new(algorithm)
    processed = 0
    last_report = 0
    with path.open("rb") as downloaded_file:
        while True:
            if should_stop and should_stop():
                raise InterruptedError("完整性校验已中止，下次继续任务时重新检查。")
            block = downloaded_file.read(4 * 1024 * 1024)
            if not block:
                break
            digest.update(block)
            processed += len(block)
            if on_progress and time.monotonic() - last_report >= 2:
                on_progress(f"正在校验 {algorithm.upper()}：已读取 {readable_size(processed)}")
                last_report = time.monotonic()
    if digest.hexdigest().lower() != expected.lower():
        raise ValueError(f"{algorithm.upper()} 校验不一致，文件可能不完整；请移开该文件后重新下载。")
    return f"大小及 {algorithm.upper()} 校验通过，与 Zenodo 公布值一致。"


def rpc_call(task, method, parameters=None):
    """向任务自己的本机 aria2 发送命令，返回 RPC 结果。"""
    payload = {"jsonrpc": "2.0", "id": "gui", "method": "aria2." + method,
               "params": ["token:" + task["secret"]] + (parameters or [])}
    request = Request(f"http://127.0.0.1:{task['port']}/jsonrpc",
                      data=json.dumps(payload).encode(),
                      headers={"Content-Type": "application/json"})
    # 本机控制连接不应经过系统代理。
    with build_opener(ProxyHandler({})).open(request, timeout=RPC_TIMEOUT_SECONDS) as response:
        result = json.load(response)
    if "error" in result:
        raise RuntimeError(result["error"]["message"])
    return result["result"]


def rounded_box(canvas, left, top, right, bottom, radius=12, **options):
    """在 Canvas 上绘制圆角界面背景；返回图形编号。"""
    radius = min(radius, (right - left) / 2, (bottom - top) / 2)
    points = [left + radius, top, right - radius, top, right, top,
              right, top + radius, right, bottom - radius, right, bottom,
              right - radius, bottom, left + radius, bottom, left, bottom,
              left, bottom - radius, left, top + radius, left, top]
    for option in ("fill", "outline"):
        if option in options:
            options[option] = theme_color(options[option])
    return canvas.create_polygon(points, smooth=True, splinesteps=24, **options)


class ModernButton(tk.Canvas):
    """带圆角与悬停反馈的按钮；输入标题、回调及颜色。"""

    def __init__(self, parent, text, command, width=106, height=38,
                 fill="#f1f4fa", foreground="#536079", hover="#e6ebf5",
                 background="white", anchor="center"):
        from tkinter import font as tkfont
        # Windows 显示缩放会增大文字，按钮须同时预留文字及内边距。
        button_font = tkfont.Font(root=parent, family="Microsoft YaHei UI", size=10)
        width = max(width, button_font.measure(text) + 32)
        height = max(height, button_font.metrics("linespace") + 16)
        super().__init__(parent, width=width, height=height, bg=background,
                         highlightthickness=0, bd=0, cursor="hand2", takefocus=True)
        self.label = text
        self.command = command
        self.fill_color = fill
        self.foreground = foreground
        self.hover_color = hover
        self.text_anchor = anchor
        self.hovering = False
        self.bind("<Configure>", self.draw)
        self.bind("<Enter>", self.enter)
        self.bind("<Leave>", self.leave)
        self.bind("<Button-1>", self.activate)
        self.bind("<Return>", self.activate)
        self.bind("<space>", self.activate)
        self.bind("<FocusIn>", self.draw)
        self.bind("<FocusOut>", self.draw)

    def draw(self, event=None):
        """根据尺寸和鼠标状态重绘按钮，无返回值。"""
        self.delete("all")
        width, height = self.winfo_width(), self.winfo_height()
        rounded_box(self, 1, 1, width - 1, height - 1, radius=10,
                    fill=self.hover_color if self.hovering else self.fill_color,
                    outline=ACCENT_COLOR if self.focus_get() == self else "")
        self.create_text(18 if self.text_anchor == "w" else width / 2, height / 2,
                         text=self.label, fill="white" if self.foreground == "white" else theme_color(self.foreground), anchor=self.text_anchor,
                         font=("Microsoft YaHei UI", 10))

    def enter(self, event=None):
        self.hovering = True
        self.draw()

    def leave(self, event=None):
        self.hovering = False
        self.draw()

    def activate(self, event=None):
        self.focus_set()
        self.command()


class TaskList(tk.Canvas):
    """绘制任务行、进度条和状态标签，提供原任务表使用的接口。"""

    ROW_HEIGHT = 82

    def __init__(self, parent, on_empty_click):
        super().__init__(parent, bg="white", highlightthickness=0, bd=0, takefocus=True)
        self.rows = {}
        self.selected_ids = []
        self.pending_draw = None
        self.on_empty_click = on_empty_click
        self.hovered_id = None
        self.bind("<Configure>", self.schedule_draw)
        self.bind("<Button-1>", self.select_row)
        self.bind("<Motion>", self.hover_row)
        self.bind("<Leave>", self.leave_rows)
        self.bind("<MouseWheel>", self.scroll_rows)
        self.bind("<Up>", self.move_selection)
        self.bind("<Down>", self.move_selection)

    def exists(self, task_id):
        return task_id in self.rows

    def item(self, task_id, values, tags=(), detail=""):
        """更新一行数据，只在数据改变时重绘。"""
        row = (values, tags, detail)
        if self.rows.get(task_id) != row:
            self.rows[task_id] = row
            self.schedule_draw()

    def insert(self, parent, index, iid, values, tags=(), detail=""):
        self.item(iid, values, tags, detail)

    def get_children(self):
        return tuple(self.rows)

    def selection(self):
        return tuple(self.selected_ids)

    def selection_set(self, task_id):
        self.selected_ids = [task_id] if task_id in self.rows else []
        self.event_generate("<<TreeviewSelect>>")
        self.schedule_draw()

    def delete(self, *items):
        """删除任务行；保留 Canvas 自己删除图形的能力。"""
        for item_id in items:
            if item_id in self.rows:
                del self.rows[item_id]
                if item_id in self.selected_ids:
                    self.selected_ids.remove(item_id)
            else:
                super().delete(item_id)
        self.schedule_draw()

    def schedule_draw(self, event=None):
        if self.pending_draw is None:
            self.pending_draw = self.after_idle(self.draw)

    def row_at(self, event):
        index = int(self.canvasy(event.y) // self.ROW_HEIGHT)
        identifiers = list(self.rows)
        return identifiers[index] if 0 <= index < len(identifiers) else None

    def select_row(self, event):
        self.focus_set()
        task_id = self.row_at(event)
        if not self.rows:
            self.on_empty_click()
            return
        if task_id is None:
            self.selected_ids = []
        elif event.state & 0x0004:
            if task_id in self.selected_ids:
                self.selected_ids.remove(task_id)
            else:
                self.selected_ids.append(task_id)
        else:
            self.selected_ids = [task_id]
        self.event_generate("<<TreeviewSelect>>")
        self.schedule_draw()

    def move_selection(self, event):
        identifiers = list(self.rows)
        if not identifiers:
            return
        index = identifiers.index(self.selected_ids[0]) if self.selected_ids else 0
        index = max(0, min(len(identifiers) - 1, index + (1 if event.keysym == "Down" else -1)))
        self.selection_set(identifiers[index])
        total_height = max(self.winfo_height(), len(identifiers) * self.ROW_HEIGHT)
        self.yview_moveto(max(0, index * self.ROW_HEIGHT - self.winfo_height() / 2) / total_height)

    def hover_row(self, event):
        hovered = self.row_at(event)
        if hovered != self.hovered_id:
            self.hovered_id = hovered
            self.schedule_draw()

    def leave_rows(self, event):
        self.hovered_id = None
        self.schedule_draw()

    def scroll_rows(self, event):
        self.yview_scroll(-int(event.delta / 120), "units")

    def fit_text(self, text, available_width, font):
        """按实际字体宽度截断文字，避免覆盖相邻列。"""
        from tkinter import font as tkfont
        measured = tkfont.Font(font=font)
        if measured.measure(text) <= available_width:
            return text
        while text and measured.measure(text + "…") > available_width:
            text = text[:-1]
        return text + "…"

    def draw(self):
        """绘制当前任务及空状态，进度条使用真实字节比例。"""
        self.pending_draw = None
        super().delete("all")
        width = self.winfo_width()
        row_font = ("Microsoft YaHei UI", 10)
        small_font = ("Microsoft YaHei UI", 8)
        height = max(self.winfo_height(), len(self.rows) * self.ROW_HEIGHT)
        self.configure(scrollregion=(0, 0, width, height))
        if not self.rows:
            center_x, center_y = width / 2, self.winfo_height() / 2
            self.create_text(center_x, center_y - 26, text="准备好下一次下载", fill=TEXT_COLOR,
                             font=("Microsoft YaHei UI", 16, "bold"))
            self.create_text(center_x, center_y + 12, text="点击这里或右上角「＋ 新建下载」，批量添加链接",
                             fill=MUTED_COLOR, font=("Microsoft YaHei UI", 10))
            self.configure(cursor="hand2")
            theme_canvas(self)
            return
        self.configure(cursor="arrow")
        name_end = max(255, width - 480)
        for index, (task_id, (values, tags, detail)) in enumerate(self.rows.items()):
            name, size, percent, status, speed = values
            top = index * self.ROW_HEIGHT
            background = "#f0f3ff" if task_id in self.selected_ids else ("#f9faff" if task_id == self.hovered_id else "white")
            self.create_rectangle(0, top, width, top + self.ROW_HEIGHT, fill=background, outline="")
            if task_id in self.selected_ids:
                rounded_box(self, 0, top + 19, 3, top + 61, radius=1, fill=ACCENT_COLOR, outline="")
            suffix = Path(name).suffix.lstrip(".").upper()[:4] or "FILE"
            rounded_box(self, 18, top + 20, 59, top + 62, radius=10, fill="#edf0ff", outline="")
            self.create_text(38, top + 41, text=suffix, fill="#6374c5", font=("Segoe UI", 8, "bold"))
            self.create_text(74, top + 29, text=self.fit_text(name, name_end - 90, row_font),
                             anchor="w", font=row_font, fill=TEXT_COLOR)
            self.create_text(74, top + 53, text=self.fit_text(detail, name_end - 90, small_font),
                             anchor="w", font=small_font, fill=MUTED_COLOR)
            self.create_text(name_end + 12, top + 40, text=size, anchor="w", font=row_font, fill="#536079")
            progress_left = name_end + 117
            progress_right = width - 222
            percent = max(0, min(100, float(percent)))
            color = "#21b587" if status == "已完成" else ("#e26878" if status == "下载失败" else ACCENT_COLOR)
            rounded_box(self, progress_left, top + 36, progress_right, top + 41, radius=2, fill="#edf0f5", outline="")
            if percent > 0:
                rounded_box(self, progress_left, top + 36,
                            progress_left + max(2, (progress_right - progress_left) * percent / 100),
                            top + 41, radius=2, fill=color, outline="")
            self.create_text(progress_left, top + 56, text=f"{percent:.1f}%", anchor="w", font=small_font, fill=MUTED_COLOR)
            pill_colors = {"已完成": ("#e8f8f1", "#19916c"), "下载失败": ("#fff0f2", "#d35167"),
                           "已暂停": ("#fff6e7", "#ba8b36"), "下载中": ("#eef0ff", "#5b6bdd")}
            pill_bg, pill_fg = pill_colors.get(status, ("#f1f4f8", "#8792a5"))
            rounded_box(self, width - 199, top + 27, width - 123, top + 53, radius=13, fill=pill_bg, outline="")
            self.create_text(width - 161, top + 40, text=status, fill=pill_fg, font=small_font)
            self.create_text(width - 18, top + 40, text=speed, anchor="e", font=("Segoe UI", 10), fill="#617089")
            self.create_line(18, top + self.ROW_HEIGHT - 1, width - 18, top + self.ROW_HEIGHT - 1, fill=LINE_COLOR)
        theme_canvas(self)


class DownloadApp:
    """组织界面和下载任务；后台线程通过事件队列更新主界面。"""

    def __init__(self, root):
        self.root = root
        self.tasks = {}
        self.queue_sequence = 0
        self.last_download_directory = str(DEFAULT_DOWNLOAD_DIRECTORY)
        self.max_active_downloads = MAX_ACTIVE_DOWNLOADS
        self.theme = "day"
        self.events = queue.Queue()
        self.filter_name = "全部任务"
        self.closing = False
        self.dirty = False
        self.last_save = 0
        self.log_selection = None
        STATE_DIRECTORY.mkdir(exist_ok=True)
        self.load_settings()
        self.build_interface()
        self.apply_theme(self.theme)
        self.load_tasks()
        root.protocol("WM_DELETE_WINDOW", self.close)
        root.after(REFRESH_INTERVAL_MS, self.refresh)

    def build_interface(self):
        """创建现代浅色下载中心、任务概况和独立日志面板。"""
        self.root.title("拾流 FlowDock · 下载中心")
        icon_file = ASSET_DIRECTORY / "flowdock.ico"
        if icon_file.exists():
            self.root.iconbitmap(str(icon_file))
        self.root.geometry("1280x860")
        self.root.minsize(1100, 740)
        self.root.configure(bg=PAGE_COLOR)
        self.root.option_add("*Font", ("Microsoft YaHei UI", 10))
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Modern.Vertical.TScrollbar", background="#dce2ed",
                        troughcolor="white", borderwidth=0, arrowsize=0,
                        lightcolor="white", darkcolor="white", width=7)
        style.map("Modern.Vertical.TScrollbar", background=[("active", "#b7c3d8")])
        style.configure("Modern.TPanedwindow", background=PAGE_COLOR, sashwidth=12)
        style.configure("Modern.TCheckbutton", background="#151e30", foreground="#99aac4")
        style.map("Modern.TCheckbutton", background=[("active", "#151e30")],
                  foreground=[("active", "white")])

        sidebar = tk.Frame(self.root, bg="white", width=206)
        sidebar.pack(side="left", fill="y")
        sidebar.pack_propagate(False)
        brand = tk.Frame(sidebar, bg="white")
        brand.pack(fill="x", padx=23, pady=(32, 35))
        logo = tk.Canvas(brand, width=48, height=48, bg="white", highlightthickness=0)
        logo.pack(side="left", padx=(0, 12))
        self.brand_icon = tk.PhotoImage(file=str(ASSET_DIRECTORY / "flowdock-48.png"))
        logo.create_image(24, 24, image=self.brand_icon)
        brand_text = tk.Frame(brand, bg="white")
        brand_text.pack(side="left")
        tk.Label(brand_text, text="拾流", bg="white", fg=TEXT_COLOR,
                 font=("Microsoft YaHei UI", 18, "bold")).pack(anchor="w")
        tk.Label(brand_text, text="FLOWDOCK", bg="white", fg=MUTED_COLOR,
                 font=("Segoe UI", 8)).pack(anchor="w")
        tk.Label(sidebar, text="工作空间", anchor="w", bg="white", fg="#a4adbd",
                 font=("Microsoft YaHei UI", 9)).pack(fill="x", padx=30, pady=(0, 10))
        self.navigation = {}
        for label in ("全部任务", "下载中", "排队中", "校验中", "已暂停", "已完成", "下载失败"):
            button = ModernButton(sidebar, label,
                                  lambda selected=label: self.set_filter(selected),
                                  width=172, height=44, fill="white", hover="#f5f7fc",
                                  foreground="#79849a", anchor="w")
            button.pack(fill="x", padx=17, pady=3)
            self.navigation[label] = button
        footer = tk.Frame(sidebar, bg="#f7f9fd", padx=15, pady=15)
        footer.pack(side="bottom", fill="x", padx=17, pady=23)
        ModernButton(sidebar, "设置", self.settings_dialog, width=172, height=42,
                     fill="#f5f7fc", hover="#e9edfc", anchor="w").pack(side="bottom", fill="x", padx=17, pady=(10, 0))
        tk.Label(footer, text="●  引擎已就绪", bg="#f7f9fd", fg="#36a783",
                 font=("Microsoft YaHei UI", 9)).pack(anchor="w")
        tk.Label(footer, text="aria2 1.37.0  ·  断点续传", bg="#f7f9fd", fg="#a0a9ba",
                 font=("Microsoft YaHei UI", 8)).pack(anchor="w", pady=(7, 0))

        main = tk.Frame(self.root, bg=PAGE_COLOR, padx=30, pady=26)
        main.pack(side="left", expand=True, fill="both")
        header = tk.Frame(main, bg=PAGE_COLOR)
        header.pack(fill="x", pady=(0, 24))
        title_group = tk.Frame(header, bg=PAGE_COLOR)
        title_group.pack(side="left")
        self.title = tk.Label(title_group, text="下载中心", bg=PAGE_COLOR, fg=TEXT_COLOR,
                              font=("Microsoft YaHei UI", 23, "bold"))
        self.title.pack(anchor="w")
        tk.Label(title_group, text="所有文件，井然有序。", bg=PAGE_COLOR, fg=MUTED_COLOR,
                 font=("Microsoft YaHei UI", 10)).pack(anchor="w", pady=(6, 0))
        ModernButton(header, "＋  新建下载", self.add_dialog, width=140, height=44,
                     fill=ACCENT_COLOR, foreground="white", hover="#4358d9",
                     background=PAGE_COLOR).pack(side="right", pady=8)
        ModernButton(header, "导入 Zenodo", self.zenodo_dialog, width=122, height=44,
                     fill="#e9edfc", foreground=ACCENT_COLOR, hover="#dfe5fb",
                     background=PAGE_COLOR).pack(side="right", padx=10, pady=8)

        stats = tk.Frame(main, bg=PAGE_COLOR)
        stats.pack(fill="x", pady=(0, 23))
        self.stat_values = {}
        for index, (key, label, color) in enumerate((("speed", "总下载速度", ACCENT_COLOR),
                                                    ("active", "正在下载", TEXT_COLOR),
                                                    ("complete", "已完成", "#1eab80"),
                                                    ("queued", "排队等待", TEXT_COLOR))):
            stats.columnconfigure(index, weight=1, uniform="stats")
            card = tk.Frame(stats, bg="white", padx=20, pady=14)
            card.grid(row=0, column=index, sticky="nsew", padx=(0 if index == 0 else 6, 0 if index == 3 else 6))
            tk.Label(card, text=label, bg="white", fg=MUTED_COLOR,
                     font=("Microsoft YaHei UI", 9)).pack(anchor="w")
            number = tk.Label(card, text="0 B/s" if key == "speed" else "0", bg="white", fg=color,
                              font=("Segoe UI", 21, "bold"))
            number.pack(anchor="w", pady=(6, 0))
            self.stat_values[key] = number

        panes = ttk.Panedwindow(main, orient="vertical", style="Modern.TPanedwindow")
        panes.pack(fill="both", expand=True)
        table_frame = tk.Frame(panes, bg="white")
        panes.add(table_frame, weight=3)
        toolbar = tk.Frame(table_frame, bg="white", padx=18, pady=12)
        toolbar.pack(fill="x")
        self.list_title = tk.Label(toolbar, text="全部任务", bg="white", fg=TEXT_COLOR,
                                   font=("Microsoft YaHei UI", 11, "bold"))
        self.list_title.pack(side="left", padx=(0, 12))
        self.task_count = tk.Label(toolbar, text="0 个任务", bg="white", fg="#a0aabd",
                                   font=("Microsoft YaHei UI", 9))
        self.task_count.pack(side="left")
        actions = (("移除", self.remove_selected, 56), ("打开目录", self.open_directory, 80),
                   ("暂停", self.pause_selected, 56), ("开始 / 继续", self.resume_selected, 98))
        for label, callback, width in actions:
            ModernButton(toolbar, label, callback, width=width, height=33,
                         fill="#f6f8fc", hover="#edf1fa").pack(side="right", padx=(6, 0))
        headings = tk.Canvas(table_frame, height=35, bg="#fafbfe", highlightthickness=0)
        headings.pack(fill="x", padx=1)

        def draw_headings(event):
            headings.delete("all")
            name_end = max(255, event.width - 480)
            for label, position, anchor in (("文件名称", 24, "w"), ("大小", name_end + 12, "w"),
                                             ("进度", name_end + 117, "w"),
                                             ("状态", event.width - 161, "center"),
                                             ("下载速度", event.width - 18, "e")):
                headings.create_text(position, 17, text=label, anchor=anchor, fill="#96a0b3",
                                     font=("Microsoft YaHei UI", 8))
            theme_canvas(headings)
        headings.bind("<Configure>", draw_headings)
        body = tk.Frame(table_frame, bg="white")
        body.pack(fill="both", expand=True)
        self.table = TaskList(body, self.add_dialog)
        scrollbar = ttk.Scrollbar(body, orient="vertical", style="Modern.Vertical.TScrollbar", command=self.table.yview)
        self.table.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        self.table.pack(fill="both", expand=True)
        self.table.bind("<<TreeviewSelect>>", self.show_selected_log)

        log_frame = tk.Frame(panes, bg="#151e30")
        panes.add(log_frame, weight=2)
        log_header = tk.Frame(log_frame, bg="#151e30", padx=19, pady=11)
        log_header.pack(fill="x")
        tk.Label(log_header, text="●", fg="#35c796", bg="#151e30",
                 font=("Segoe UI", 9)).pack(side="left", padx=(0, 9))
        self.log_title = tk.Label(log_header, text="实时日志 · 选择任务查看下载详情", bg="#151e30",
                                  fg="#d2dced", font=("Microsoft YaHei UI", 9))
        self.log_title.pack(side="left")
        self.follow_log = tk.BooleanVar(value=True)
        ttk.Checkbutton(log_header, text="跟随日志", variable=self.follow_log,
                        style="Modern.TCheckbutton").pack(side="right")
        tk.Frame(log_frame, height=1, bg="#243048").pack(fill="x", padx=18)
        log_body = tk.Frame(log_frame, bg="#151e30", padx=8, pady=6)
        log_body.pack(fill="both", expand=True)
        log_scrollbar = ttk.Scrollbar(log_body, style="Modern.Vertical.TScrollbar")
        log_scrollbar.pack(side="right", fill="y")
        self.log = tk.Text(log_body, bg="#151e30", fg="#a1b2cc", relief="flat", bd=0,
                           wrap="word", height=7, padx=12, pady=5, spacing1=3,
                           font=("Consolas", 10), state="disabled", highlightthickness=0,
                           selectbackground="#34496b", yscrollcommand=log_scrollbar.set)
        self.log.pack(fill="both", expand=True)
        log_scrollbar.configure(command=self.log.yview)
        self.summary = tk.Label(main, text="就绪", anchor="w", bg=PAGE_COLOR, fg="#96a1b4",
                                font=("Microsoft YaHei UI", 8))
        self.summary.pack(fill="x", pady=(12, 0))
        self.set_filter("全部任务")

    def new_task(self, url, directory, task_id=None):
        """由地址和保存目录建立任务状态，返回任务字典。"""
        task_id = task_id or secrets.token_hex(8)
        self.queue_sequence += 1
        filename = unquote(Path(urlparse(url).path).name) or "等待获取文件名"
        return {"id": task_id, "url": url, "directory": directory, "name": filename,
                "status": "排队中", "total": 0, "completed": 0, "speed": 0,
                "queue_order": self.queue_sequence, "queued": True,
                "resuming": False, "engine_status": None,
                "expected_checksum": "", "expected_size": 0, "zenodo_name": "",
                "verification": "", "verifying": False,
                "process": None, "gid": None, "port": None, "secret": "",
                "logs": collections.deque(maxlen=MAX_VISIBLE_LOG_LINES),
                "log_file": STATE_DIRECTORY / (task_id + ".log"), "control_pending": False}

    def apply_theme(self, theme):
        """即时切换现有窗口的颜色，保留下载线程、任务选择和日志内容。"""
        global CURRENT_THEME
        self.theme = theme
        CURRENT_THEME = theme

        def recolor(widget):
            if not hasattr(widget, "_day_colors"):
                widget._day_colors = {}
                for option in ("background", "foreground", "insertbackground", "selectbackground",
                               "highlightbackground", "highlightcolor", "activebackground", "activeforeground"):
                    if option in widget.keys():
                        widget._day_colors[option] = widget.cget(option)
            for option, original in widget._day_colors.items():
                widget.configure(**{option: theme_color(str(original))})
            if isinstance(widget, ModernButton):
                widget.draw()
            elif isinstance(widget, TaskList):
                widget.schedule_draw()
            elif isinstance(widget, tk.Canvas):
                theme_canvas(widget)
            for child in widget.winfo_children():
                recolor(child)

        recolor(self.root)
        style = ttk.Style()
        surface, text = theme_color("white"), theme_color(TEXT_COLOR)
        style.configure("Modern.TPanedwindow", background=theme_color(PAGE_COLOR))
        style.configure("Modern.Vertical.TScrollbar", background=theme_color("#e6ebf5"),
                        troughcolor=surface, lightcolor=surface, darkcolor=surface)
        style.configure("Zenodo.Treeview", background=surface, fieldbackground=surface, foreground=text)
        style.map("Zenodo.Treeview", background=[("selected", theme_color("#eef1ff"))],
                  foreground=[("selected", text)])
        style.configure("Zenodo.Treeview.Heading", background=theme_color("#f1f4fa"), foreground=text)
        style.configure("Settings.TRadiobutton", background=surface, foreground=text, padding=8)
        style.map("Settings.TRadiobutton", background=[("active", surface)], foreground=[("active", text)])
        style.configure("Settings.TSpinbox", fieldbackground=surface, foreground=text,
                        background=theme_color("#f1f4fa"), padding=8)
        style.map("Settings.TSpinbox", fieldbackground=[("readonly", surface)],
                  foreground=[("readonly", text)])

    def settings_dialog(self):
        """显示并发数和外观设置，保存后即时生效，取消时恢复原主题。"""
        dialog = tk.Toplevel(self.root)
        dialog.title("拾流 · 设置")
        dialog.geometry("560x450")
        dialog.minsize(520, 430)
        dialog.transient(self.root)
        dialog.grab_set()
        dialog.iconbitmap(str(ASSET_DIRECTORY / "flowdock.ico"))
        dialog.configure(bg="white")
        body = tk.Frame(dialog, bg="white", padx=28, pady=24)
        body.pack(fill="both", expand=True)
        tk.Label(body, text="设置", bg="white", fg=TEXT_COLOR,
                 font=("Microsoft YaHei UI", 20, "bold")).pack(anchor="w", pady=(0, 20))
        tk.Label(body, text="同时下载文件数", bg="white", fg=TEXT_COLOR).pack(anchor="w")
        concurrency = tk.StringVar(value=str(self.max_active_downloads))
        ttk.Spinbox(body, values=tuple(range(1, MAX_CONCURRENT_DOWNLOADS_SETTING + 1)),
                    textvariable=concurrency, state="readonly", width=8,
                    style="Settings.TSpinbox").pack(anchor="w", pady=10)
        tk.Label(body, text="超过上限的任务自动排队；降低上限不打断当前下载。", bg="white",
                 fg=MUTED_COLOR, wraplength=460, justify="left",
                 font=("Microsoft YaHei UI", 9)).pack(anchor="w", pady=(0, 22))
        tk.Label(body, text="外观模式", bg="white", fg=TEXT_COLOR).pack(anchor="w")
        original_theme = self.theme
        theme_choice = tk.StringVar(value=self.theme)
        theme_row = tk.Frame(body, bg="white")
        theme_row.pack(fill="x", pady=8)
        for label, value in (("日间模式", "day"), ("夜间模式", "night")):
            ttk.Radiobutton(theme_row, text=label, value=value, variable=theme_choice,
                            style="Settings.TRadiobutton",
                            command=lambda: self.apply_theme(theme_choice.get())).pack(side="left", padx=(0, 20))
        error_text = tk.StringVar()
        tk.Label(body, textvariable=error_text, bg="white", fg="#d35167",
                 wraplength=460, font=("Microsoft YaHei UI", 9)).pack(anchor="w")

        def cancel():
            dialog.destroy()
            self.apply_theme(original_theme)

        def save():
            try:
                limit = int(concurrency.get())
                if not 1 <= limit <= MAX_CONCURRENT_DOWNLOADS_SETTING:
                    raise ValueError()
            except (ValueError, tk.TclError):
                error_text.set(f"请选择 1～{MAX_CONCURRENT_DOWNLOADS_SETTING} 个文件。")
                return
            old_limit = self.max_active_downloads
            self.max_active_downloads = limit
            self.apply_theme(theme_choice.get())
            try:
                self.save_settings()
            except OSError as error:
                self.max_active_downloads = old_limit
                error_text.set(f"设置未能保存，请检查文件夹写入权限：{error}")
                return
            dialog.destroy()

        actions = tk.Frame(body, bg="white")
        actions.pack(side="bottom", fill="x", pady=(12, 0))
        ModernButton(actions, "保存设置", save, width=118, height=42,
                     fill=ACCENT_COLOR, foreground="white", hover="#4358d9").pack(side="right")
        ModernButton(actions, "取消", cancel, width=76, height=42).pack(side="right", padx=10)
        dialog.protocol("WM_DELETE_WINDOW", cancel)
        self.apply_theme(self.theme)
        dialog.update_idletasks()
        required_height = max(450, body.winfo_reqheight() + 16)
        dialog.minsize(560, required_height)
        dialog.geometry(f"600x{required_height}")

    def load_settings(self):
        """读取上次成功添加任务时的目录；首次启动使用默认目录。"""
        settings_file = STATE_DIRECTORY / "settings.json"
        if not settings_file.exists():
            return
        try:
            settings = json.loads(settings_file.read_text(encoding="utf-8"))
            directory = settings.get("last_download_directory")
            if isinstance(directory, str) and directory.strip():
                self.last_download_directory = directory
            concurrency = settings.get("max_active_downloads", MAX_ACTIVE_DOWNLOADS)
            if type(concurrency) is int and 1 <= concurrency <= MAX_CONCURRENT_DOWNLOADS_SETTING:
                self.max_active_downloads = concurrency
            if settings.get("theme") in ("day", "night"):
                self.theme = settings["theme"]
        except (OSError, ValueError, AttributeError):
            # 配置损坏不影响启动，后续成功添加任务时会重新保存。
            pass

    def save_settings(self):
        """统一保存目录、并发数和主题，避免各项设置互相覆盖。"""
        settings_file = STATE_DIRECTORY / "settings.json"
        temporary = settings_file.with_suffix(".tmp")
        settings = {"last_download_directory": self.last_download_directory,
                    "max_active_downloads": self.max_active_downloads, "theme": self.theme}
        temporary.write_text(json.dumps(settings, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(settings_file)

    def remember_directory(self, directory):
        """保存本次目录，同时保留并发数量和主题偏好。"""
        self.last_download_directory = str(directory)
        try:
            self.save_settings()
        except OSError as error:
            messagebox.showwarning("目录偏好未保存", f"本次仍可下载，但重启后可能需要重新选择目录。\n{error}")

    def load_tasks(self):
        """读取上次的任务记录；未完成任务默认暂停，不自动联网。"""
        if not STATE_FILE.exists():
            return
        try:
            for record in json.loads(STATE_FILE.read_text(encoding="utf-8")):
                task = self.new_task(record["url"], record["directory"], record["id"])
                for field in ("name", "total", "completed", "expected_checksum", "expected_size", "zenodo_name", "verification"):
                    task[field] = record.get(field, task[field])
                task["status"] = "已完成" if record.get("status") == "已完成" else "已暂停"
                task["queued"] = False
                if task["log_file"].exists():
                    with task["log_file"].open(encoding="utf-8", errors="replace") as log_file:
                        task["logs"].extend(collections.deque(
                            (line.rstrip() for line in log_file), maxlen=MAX_VISIBLE_LOG_LINES))
                self.tasks[task["id"]] = task
                self.append_log(task, "已恢复任务记录，点击“开始 / 继续”可续传。")
            if self.tasks and not (STATE_DIRECTORY / "settings.json").exists():
                # 兼容旧版：尚无目录偏好时，取最后一个任务的保存目录。
                self.last_download_directory = next(reversed(self.tasks.values()))["directory"]
        except (OSError, ValueError, KeyError) as error:
            messagebox.showwarning("任务记录读取失败", f"请检查 {STATE_FILE}\n{error}")

    def save_tasks(self):
        """以临时文件替换方式保存任务元数据，避免写入半截记录。"""
        fields = ("id", "url", "directory", "name", "status", "total", "completed",
                  "expected_checksum", "expected_size", "zenodo_name", "verification")
        records = [{field: task[field] for field in fields} for task in self.tasks.values()]
        temporary = STATE_FILE.with_suffix(".tmp")
        temporary.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(STATE_FILE)
        self.dirty = False
        self.last_save = time.monotonic()

    def add_dialog(self):
        """打开批量添加窗口，验证地址后加入队列；无返回值。"""
        dialog = tk.Toplevel(self.root)
        dialog.title("拾流 · 新建下载")
        dialog.iconbitmap(str(ASSET_DIRECTORY / "flowdock.ico"))
        dialog.geometry("700x530")
        dialog.minsize(620, 490)
        dialog.configure(bg="white")
        dialog.transient(self.root)
        dialog.grab_set()
        body = tk.Frame(dialog, bg="white", padx=28, pady=25)
        body.pack(fill="both", expand=True)
        tk.Label(body, text="添加下载链接", bg="white", fg=TEXT_COLOR,
                 font=("Microsoft YaHei UI", 19, "bold")).pack(anchor="w")
        tk.Label(body, text="每行一个链接，支持批量添加 HTTP、HTTPS 和 FTP 地址。",
                 bg="white", fg=MUTED_COLOR, font=("Microsoft YaHei UI", 9)).pack(anchor="w", pady=(8, 18))
        urls = tk.Text(body, height=9, wrap="word", relief="flat", bd=0,
                       bg="#f5f7fc", fg=TEXT_COLOR, padx=14, pady=13,
                       highlightthickness=1, highlightbackground="#e9edf5",
                       highlightcolor=ACCENT_COLOR, insertbackground=ACCENT_COLOR,
                       font=("Consolas", 11))
        urls.pack(fill="both", expand=True, pady=(0, 20))
        tk.Label(body, text="保存位置", bg="white", fg="#67748c",
                 font=("Microsoft YaHei UI", 9)).pack(anchor="w")
        directory_row = tk.Frame(body, bg="white")
        directory_row.pack(fill="x", pady=8)
        directory = tk.StringVar(value=self.last_download_directory)
        tk.Entry(directory_row, textvariable=directory, relief="flat", bg="#f5f7fc",
                 fg=TEXT_COLOR, highlightthickness=1, highlightbackground="#e9edf5",
                 highlightcolor=ACCENT_COLOR).pack(side="left", fill="x", expand=True, ipady=9)

        def browse():
            initial_directory = Path(directory.get()).expanduser()
            if not initial_directory.is_dir():
                initial_directory = APP_DIRECTORY
            selected = filedialog.askdirectory(parent=dialog, initialdir=initial_directory)
            if selected:
                directory.set(selected)

        ModernButton(directory_row, "选择目录", browse, width=95, height=38).pack(side="right", padx=(8, 0))

        def submit():
            addresses = list(dict.fromkeys(line.strip() for line in urls.get("1.0", "end").splitlines() if line.strip()))
            if not addresses:
                messagebox.showwarning("还没有地址", "请至少粘贴一个下载地址。", parent=dialog)
                return
            if any(zenodo_record_id(address) for address in addresses):
                if len(addresses) != 1:
                    messagebox.showinfo("请单独导入记录", "请每次填写一个 Zenodo 页面地址，解析后可以批量勾选其中的文件。", parent=dialog)
                    return
                selected_directory = directory.get()
                dialog.destroy()
                self.zenodo_dialog(addresses[0], selected_directory)
                return
            for address in addresses:
                parsed = urlparse(address)
                if parsed.scheme.lower() not in ("http", "https", "ftp") or not parsed.hostname:
                    messagebox.showerror("地址格式不正确", f"请检查这个地址：\n{address}", parent=dialog)
                    return
            if not directory.get().strip():
                messagebox.showerror("请选择目录", "保存目录不能为空。", parent=dialog)
                return
            destination = Path(directory.get()).expanduser().resolve()
            try:
                destination.mkdir(parents=True, exist_ok=True)
            except OSError as error:
                messagebox.showerror("无法创建目录", str(error), parent=dialog)
                return
            for address in addresses:
                task = self.new_task(address, str(destination))
                self.tasks[task["id"]] = task
                self.append_log(task, "任务已加入下载队列。")
            self.remember_directory(destination)
            self.dirty = True
            self.set_filter("全部任务")
            dialog.destroy()

        buttons = tk.Frame(body, bg="white")
        buttons.pack(fill="x", pady=(18, 0))
        ModernButton(buttons, "开始下载", submit, width=125, height=42,
                     fill=ACCENT_COLOR, foreground="white", hover="#4358d9").pack(side="right")
        ModernButton(buttons, "取消", dialog.destroy, width=76, height=42,
                     fill="white", hover="#f3f5fa").pack(side="right", padx=10)
        urls.focus_set()
        self.apply_theme(self.theme)

    def zenodo_dialog(self, address="", directory=None):
        """异步解析 Zenodo 记录，让用户勾选文件后加入现有队列。"""
        dialog = tk.Toplevel(self.root)
        dialog.title("拾流 · 导入 Zenodo")
        dialog.geometry("920x740")
        dialog.minsize(820, 620)
        dialog.configure(bg="white")
        dialog.iconbitmap(str(ASSET_DIRECTORY / "flowdock.ico"))
        dialog.transient(self.root)
        dialog.grab_set()
        body = tk.Frame(dialog, bg="white", padx=26, pady=22)
        body.pack(fill="both", expand=True)
        # 只有文件列表随窗口伸缩，底部路径和操作按钮始终保留完整空间。
        body.columnconfigure(0, weight=1)
        body.rowconfigure(4, weight=1)
        tk.Label(body, text="从 Zenodo 导入", bg="white", fg=TEXT_COLOR,
                 font=("Microsoft YaHei UI", 19, "bold")).grid(row=0, column=0, sticky="w")
        tk.Label(body, text="解析记录 → 勾选文件 → 队列下载 → 完整性校验", bg="white",
                 fg=MUTED_COLOR, font=("Microsoft YaHei UI", 9)).grid(row=1, column=0, sticky="w", pady=(7, 14))
        link_row = tk.Frame(body, bg="white")
        link_row.grid(row=2, column=0, sticky="ew")
        record_address = tk.StringVar(value=address)
        entry = tk.Entry(link_row, textvariable=record_address, bg="#f5f7fc", relief="flat",
                         highlightthickness=1, highlightbackground=LINE_COLOR,
                         highlightcolor=ACCENT_COLOR, fg=TEXT_COLOR)
        entry.pack(side="left", fill="x", expand=True, ipady=9)
        status = tk.StringVar(value="输入记录页面地址、编号或 DOI，再点击解析。")
        state = {"files": [], "selected": set(), "busy": False}
        results = queue.Queue()
        selection_text = tk.StringVar(value="尚未选择文件")

        def update_selection():
            size = sum(state["files"][index]["size"] for index in state["selected"])
            selection_text.set(f"已选 {len(state['selected'])}/{len(state['files'])} 个文件，共 {readable_size(size)}")
            for index in range(len(state["files"])):
                table.set(str(index), "selected", "☑" if index in state["selected"] else "☐")

        def receive_result():
            if not dialog.winfo_exists():
                return
            try:
                succeeded, result = results.get_nowait()
            except queue.Empty:
                self.root.after(100, receive_result)
                return
            state["busy"] = False
            parse_button.label = "解析文件"
            parse_button.draw()
            if not succeeded:
                status.set(str(result))
                return
            title, files = result
            state["files"] = files
            state["selected"] = set()  # 避免默认勾选数十 GB 的大文件。
            status.set(title)
            for index, file in enumerate(files):
                table.insert("", "end", iid=str(index), values=("☐", file["name"], readable_size(file["size"]),
                             file["checksum"].split(":")[0].upper() if file["checksum"] else "未提供"))
            update_selection()

        def parse_record():
            if state["busy"]:
                return
            value = record_address.get().strip()
            if not zenodo_record_id(value):
                status.set("地址格式不正确，例如：https://zenodo.org/records/14727217")
                return
            state["busy"] = True
            state["files"] = []
            state["selected"] = set()
            for item in table.get_children():
                table.delete(item)
            selection_text.set("正在获取文件清单…")
            status.set("正在连接 Zenodo；如果服务器限流，会等待后重试。")
            parse_button.label = "解析中…"
            parse_button.draw()

            def fetch():
                try:
                    results.put((True, fetch_zenodo_files(value)))
                except Exception as error:
                    results.put((False, str(error)))

            threading.Thread(target=fetch, daemon=True).start()
            self.root.after(100, receive_result)

        parse_button = ModernButton(link_row, "解析文件", parse_record, width=100,
                                    fill=ACCENT_COLOR, foreground="white", hover="#4358d9")
        parse_button.pack(side="right", padx=(10, 0))
        entry.bind("<Return>", lambda event: parse_record())
        status_label = tk.Label(body, textvariable=status, bg="white", fg="#70809a", anchor="w",
                                wraplength=760, font=("Microsoft YaHei UI", 9))
        status_label.grid(row=3, column=0, sticky="ew", pady=12)
        status_label.bind("<Configure>", lambda event: status_label.configure(wraplength=max(200, event.width)))
        table_frame = tk.Frame(body, bg="white")
        table_frame.grid(row=4, column=0, sticky="nsew")
        style = ttk.Style()
        style.configure("Zenodo.Treeview", rowheight=32, background="white", fieldbackground="white",
                        foreground=TEXT_COLOR, borderwidth=0, font=("Microsoft YaHei UI", 9))
        style.configure("Zenodo.Treeview.Heading", background="#f1f4fa", padding=8,
                        foreground="#73819a", font=("Microsoft YaHei UI", 9))
        table = ttk.Treeview(table_frame, columns=("selected", "name", "size", "checksum"),
                             show="headings", height=6, style="Zenodo.Treeview")
        for key, title, width in (("selected", "选择", 45), ("name", "文件名称", 410),
                                   ("size", "大小", 100), ("checksum", "校验", 75)):
            table.heading(key, text=title)
            table.column(key, width=width, minwidth=40, stretch=key == "name")
        scroll = ttk.Scrollbar(table_frame, command=table.yview)
        scroll.pack(side="right", fill="y")
        table.configure(yscrollcommand=scroll.set)
        table.pack(fill="both", expand=True)

        def toggle_file(event):
            item = table.identify_row(event.y)
            if not item:
                return
            index = int(item)
            if index in state["selected"]:
                state["selected"].remove(index)
            else:
                state["selected"].add(index)
            update_selection()

        table.bind("<ButtonRelease-1>", toggle_file)
        table.bind("<space>", lambda event: toggle_focused())

        def toggle_focused():
            if table.focus():
                index = int(table.focus())
                if index in state["selected"]:
                    state["selected"].remove(index)
                else:
                    state["selected"].add(index)
                update_selection()

        def select_all(selected):
            state["selected"] = set(range(len(state["files"]))) if selected else set()
            update_selection()

        choices = tk.Frame(body, bg="white")
        choices.grid(row=5, column=0, sticky="ew", pady=8)
        ModernButton(choices, "全选", lambda: select_all(True), width=60, height=30).pack(side="left")
        ModernButton(choices, "清空", lambda: select_all(False), width=60, height=30).pack(side="left", padx=8)
        tk.Label(choices, textvariable=selection_text, bg="white", fg=MUTED_COLOR,
                 font=("Microsoft YaHei UI", 9)).pack(side="right")
        destination = tk.StringVar(value=directory or self.last_download_directory)
        path_row = tk.Frame(body, bg="white")
        path_row.grid(row=6, column=0, sticky="ew", pady=(4, 12))
        tk.Entry(path_row, textvariable=destination, bg="#f5f7fc", relief="flat",
                 highlightthickness=1, highlightbackground=LINE_COLOR,
                 highlightcolor=ACCENT_COLOR).pack(side="left", fill="x", expand=True, ipady=9)

        def browse():
            current = Path(destination.get()).expanduser()
            chosen = filedialog.askdirectory(parent=dialog, initialdir=current if current.is_dir() else APP_DIRECTORY)
            if chosen:
                destination.set(chosen)

        ModernButton(path_row, "保存位置", browse, width=94).pack(side="right", padx=(10, 0))

        def add_selected():
            if state["busy"] or not state["selected"]:
                status.set("请先解析记录，再勾选需要下载的文件。")
                return
            if not destination.get().strip():
                status.set("请选择保存目录。")
                return
            try:
                folder = Path(destination.get()).expanduser().resolve()
                folder.mkdir(parents=True, exist_ok=True)
            except OSError as error:
                status.set(f"保存目录不可用：{error}")
                return
            for index in sorted(state["selected"]):
                file = state["files"][index]
                task = self.new_task(file["url"], str(folder))
                task.update(name=file["name"], zenodo_name=file["name"], total=file["size"],
                            expected_size=file["size"], expected_checksum=file["checksum"])
                self.tasks[task["id"]] = task
                self.append_log(task, "Zenodo 文件已入队，下载完成后核对文件大小及公开摘要。")
            self.remember_directory(folder)
            self.dirty = True
            self.set_filter("全部任务")
            dialog.destroy()

        ModernButton(body, "加入下载队列", add_selected, width=170, height=44,
                     fill=ACCENT_COLOR, foreground="white", hover="#4358d9").grid(row=7, column=0, sticky="e")
        entry.focus_set()
        self.apply_theme(self.theme)
        if address:
            parse_record()

    def append_log(self, task, line):
        """给指定任务追加带时间的日志，并同步到日志文件。"""
        line = line.strip()
        if not line:
            return
        entry = f"[{datetime.datetime.now():%H:%M:%S}] {line}"
        task["logs"].append(entry)
        try:
            with task["log_file"].open("a", encoding="utf-8") as output:
                output.write(entry + "\n")
        except OSError:
            pass
        if self.table.selection() and self.table.selection()[0] == task["id"]:
            self.log.configure(state="normal")
            self.log.insert("end", entry + "\n")
            if int(self.log.index("end-1c").split(".")[0]) > MAX_VISIBLE_LOG_LINES:
                self.log.delete("1.0", "2.0")
            if self.follow_log.get():
                self.log.see("end")
            self.log.configure(state="disabled")

    def start_task(self, task):
        """为任务启动独立 aria2 引擎，后台处理控制与状态读取。"""
        task["status"] = "连接中"
        task["queued"] = False
        task["resuming"] = False
        task["engine_status"] = None
        task["gid"] = None
        task["secret"] = secrets.token_hex(24)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            task["port"] = listener.getsockname()[1]
        command = [str(ARIA2_PATH), "--no-conf=true", "--enable-rpc=true",
                   "--rpc-listen-all=false", f"--rpc-listen-port={task['port']}",
                   f"--rpc-secret={task['secret']}", "--rpc-allow-origin-all=false",
                   f"--stop-with-process={os.getpid()}", "--console-log-level=notice",
                   "--enable-color=false", "--show-console-readout=false",
                   "--auto-save-interval=1", "--file-allocation=none"]
        try:
            task["process"] = subprocess.Popen(command, stdout=subprocess.PIPE,
                                                stderr=subprocess.STDOUT,
                                                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        except OSError as error:
            task["status"] = "下载失败"
            self.append_log(task, f"无法启动 aria2，请检查程序路径：{error}")
            return
        threading.Thread(target=self.read_output, args=(task,), daemon=True).start()
        threading.Thread(target=self.monitor_task, args=(task,), daemon=True).start()

    def read_output(self, task):
        """逐行读取引擎输出，转交主线程显示，避免界面卡住。"""
        for raw_line in iter(task["process"].stdout.readline, b""):
            self.events.put((task["id"], "log", raw_line.decode("utf-8", errors="replace")))
        task["process"].stdout.close()

    def monitor_task(self, task):
        """读取实际字节进度；输入任务，向主线程发送状态和结果。"""
        try:
            for attempt in range(40):
                if self.closing:
                    return
                try:
                    rpc_call(task, "getVersion")
                    break
                except Exception:
                    if task["process"].poll() is not None:
                        raise RuntimeError("aria2 提前退出，请查看该任务的引擎日志。")
                    time.sleep(0.1)
            options = {"dir": task["directory"], "continue": "true",
                       "allow-overwrite": "false", "auto-file-renaming": "false",
                       "max-connection-per-server": str(CONNECTIONS_PER_DOWNLOAD),
                       "split": str(CONNECTIONS_PER_DOWNLOAD), "max-tries": "5",
                       "retry-wait": "5", "connect-timeout": "20"}
            if task["zenodo_name"]:
                options["out"] = task["zenodo_name"]
            task["gid"] = rpc_call(task, "addUri", [[task["url"]], options])
            monitor_interrupted = False
            last_warning = 0
            last_progress_log = 0
            while not self.closing:
                try:
                    status = rpc_call(task, "tellStatus", [task["gid"]])
                    if monitor_interrupted:
                        self.events.put((task["id"], "log", "已恢复与下载引擎的连接，继续更新进度。"))
                        self.events.put((task["id"], "monitor_warning", False))
                    monitor_interrupted = False
                except RuntimeError:
                    # 引擎明确返回的 RPC 错误（例如任务不存在）不能当成网络超时。
                    raise
                except Exception as error:
                    exit_code = task["process"].poll()
                    if exit_code is not None:
                        raise RuntimeError(f"下载引擎已退出（退出码 {exit_code}），请查看该任务此前的日志。") from error
                    if self.closing:
                        break
                    # 本机状态接口暂时无响应不等于下载停止，不能因此关闭正常引擎。
                    if not monitor_interrupted or time.monotonic() - last_warning >= RPC_WARNING_INTERVAL_SECONDS:
                        self.events.put((task["id"], "log", f"警告：暂时无法读取下载进度，但 aria2 仍在运行；保留下载并自动重连。原因：{error}"))
                        self.events.put((task["id"], "monitor_warning", True))
                        last_warning = time.monotonic()
                    monitor_interrupted = True
                    time.sleep(1)
                    continue
                if status["status"] == "complete" and task["zenodo_name"]:
                    checking_status = dict(status, status="verifying")
                    self.events.put((task["id"], "status", checking_status))
                    self.events.put((task["id"], "log", "传输完成，正在读取本地文件进行完整性校验…"))
                    downloaded_path = Path(task["directory"]) / task["zenodo_name"]
                    description = verify_download_file(
                        downloaded_path, task["expected_checksum"], task["expected_size"],
                        on_progress=lambda message: self.events.put((task["id"], "log", message)),
                        should_stop=lambda: self.closing)
                    self.events.put((task["id"], "verified", description))
                    break
                self.events.put((task["id"], "status", status))
                if status["status"] in ("complete", "error", "removed"):
                    break
                if status["status"] == "active" and time.monotonic() - last_progress_log >= 2:
                    reported_total = int(status.get("totalLength", 0))
                    total = reported_total or task["expected_size"] or task["total"]
                    completed = int(status.get("completedLength", 0)) if reported_total else task["completed"]
                    speed = int(status.get("downloadSpeed", 0))
                    self.events.put((task["id"], "log",
                                     f"已下载 {readable_size(completed)} / {readable_size(total)}，速度 {readable_size(speed)}/s"))
                    last_progress_log = time.monotonic()
                time.sleep(0.5)
        except Exception as error:
            self.events.put((task["id"], "failure", str(error)))
        finally:
            self.stop_engine(task)

    def stop_engine(self, task):
        """优先正常关闭引擎以保留续传信息，超时才终止进程。"""
        process = task.get("process")
        if process is None or process.poll() is not None:
            return
        try:
            rpc_call(task, "shutdown")
            process.wait(timeout=4)
        except Exception:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()

    def control_task(self, task, method):
        """异步暂停或恢复任务，命令失败时保留日志说明。"""
        if task["control_pending"] or not task["gid"]:
            return
        task["control_pending"] = True

        def control():
            try:
                rpc_call(task, method, [task["gid"]])
                self.events.put((task["id"], "log", "已暂停。" if method == "pause" else "继续下载。"))
            except Exception as error:
                self.events.put((task["id"], "log", f"操作未成功：{error}"))
                if method == "unpause":
                    self.events.put((task["id"], "resume_failed", None))
            finally:
                self.events.put((task["id"], "controlled", None))

        threading.Thread(target=control, daemon=True).start()

    def pause_selected(self):
        """暂停选中的任务，排队任务直接设为暂停。"""
        for task_id in self.table.selection():
            task = self.tasks[task_id]
            if task["status"] == "排队中":
                task["status"] = "已暂停"
                task["queued"] = False
                self.append_log(task, "已暂停排队，点击继续可重新加入队尾。")
            elif task["status"] == "下载中":
                self.control_task(task, "pause")
        self.dirty = True

    def resume_selected(self):
        """恢复选中的暂停或失败任务，优先进入等待队列。"""
        for task_id in self.table.selection():
            task = self.tasks[task_id]
            if task["status"] in ("已暂停", "下载失败"):
                # 恢复任务先进入队尾，统一由调度器分配空位，避免超过并发上限。
                self.queue_sequence += 1
                task["queue_order"] = self.queue_sequence
                task["queued"] = True
                task["status"] = "排队中"
                self.append_log(task, "已加入队尾，空出下载位置后自动开始。")
        self.dirty = True

    def open_directory(self):
        """在 Windows 文件管理器中打开所选任务的保存目录。"""
        selection = self.table.selection()
        if selection:
            try:
                os.startfile(self.tasks[selection[0]]["directory"])
            except OSError as error:
                messagebox.showerror("无法打开目录", str(error))

    def remove_selected(self):
        """仅移除停止任务的界面记录，保留下载文件和日志。"""
        for task_id in self.table.selection():
            task = self.tasks[task_id]
            if task["verifying"] or (task["process"] is not None and task["process"].poll() is None):
                messagebox.showinfo("任务仍在运行", "请在任务完成后移除；也可关闭软件后重新打开，再移除暂停记录。")
                return
        for task_id in self.table.selection():
            del self.tasks[task_id]
        self.dirty = True

    def set_filter(self, label):
        """切换侧栏状态筛选；输入导航文字，无返回值。"""
        self.filter_name = label
        self.list_title.configure(text=label)
        for name, button in self.navigation.items():
            button.fill_color = "#eef1ff" if name == label else "white"
            button.foreground = ACCENT_COLOR if name == label else "#79849a"
            button.draw()

    def show_selected_log(self, event=None):
        """把选中任务的日志显示在下方面板，保留每个任务自己的日志。"""
        selection = self.table.selection()
        selected = selection[0] if selection else None
        if selected == self.log_selection:
            return
        self.log_selection = selected
        self.log.configure(state="normal")
        self.log.delete("1.0", "end")
        if selected and selected in self.tasks:
            task = self.tasks[selected]
            self.log_title.configure(text="实时日志 · " + task["name"][:42])
            self.log.insert("end", "\n".join(task["logs"]) + "\n")
        else:
            self.log_title.configure(text="实时日志 · 选择一个任务查看")
        self.log.see("end")
        self.log.configure(state="disabled")

    def refresh(self):
        """消费后台事件、更新任务表并调度队列；每 250 毫秒执行一次。"""
        if self.closing:
            return
        for event_index in range(300):
            try:
                task_id, event_type, value = self.events.get_nowait()
            except queue.Empty:
                break
            if task_id not in self.tasks:
                continue
            task = self.tasks[task_id]
            if event_type == "log":
                self.append_log(task, value)
            elif event_type == "controlled":
                task["control_pending"] = False
            elif event_type == "monitor_warning":
                task["monitor_warning"] = value
                if value:
                    task["speed"] = 0
            elif event_type == "resume_failed":
                task["resuming"] = False
                task["queued"] = False
                task["status"] = "已暂停"
            elif event_type == "failure":
                task["status"] = "下载失败"
                task["speed"] = 0
                task["verifying"] = False
                self.append_log(task, value)
                self.dirty = True
            elif event_type == "verified":
                task["status"] = "已完成"
                task["speed"] = 0
                task["verifying"] = False
                task["verification"] = value
                self.append_log(task, value)
                self.dirty = True
            elif event_type == "status":
                mapping = {"active": "下载中", "waiting": "连接中", "paused": "已暂停",
                           "complete": "已完成", "error": "下载失败", "removed": "已暂停",
                           "verifying": "校验中"}
                previous = task["status"]
                task["engine_status"] = value["status"]
                task["verifying"] = value["status"] == "verifying"
                # 引擎暂停时仍定期报告 paused；界面排队状态不能被这些报告覆盖。
                if value["status"] == "paused" and task["queued"]:
                    task["status"] = "排队中"
                elif value["status"] == "paused" and task["resuming"]:
                    task["status"] = "连接中"
                elif value["status"] in ("complete", "error", "removed") and task["queued"]:
                    # 重试失败任务时，先等旧引擎退出，再启动新引擎。
                    task["status"] = "排队中"
                else:
                    task["status"] = mapping.get(value["status"], "连接中")
                    if value["status"] == "active":
                        task["resuming"] = False
                for target, source in (("total", "totalLength"), ("completed", "completedLength"), ("speed", "downloadSpeed")):
                    if target in ("total", "completed") and not int(value.get("totalLength", 0)):
                        continue  # 连接或续传初始化期间，保留已经知道的大小和进度。
                    task[target] = int(value.get(source, 0))
                if value.get("files"):
                    task["name"] = Path(value["files"][0]["path"]).name or task["name"]
                if previous != task["status"]:
                    self.append_log(task, "状态：" + task["status"])
                    if value.get("errorMessage"):
                        self.append_log(task, f"错误 {value.get('errorCode', '')}：{value['errorMessage']}")
                self.dirty = True

        active = sum(task["status"] in ("连接中", "下载中") for task in self.tasks.values())
        queue_tasks = sorted((task for task in self.tasks.values() if task["queued"]),
                             key=lambda task: task["queue_order"])
        for task in queue_tasks:
            if active >= self.max_active_downloads:
                break
            process = task["process"]
            if process is None or process.poll() is not None:
                self.start_task(task)
                if task["status"] == "连接中":
                    active += 1
            elif task["engine_status"] == "paused" and not task["control_pending"]:
                task["queued"] = False
                task["resuming"] = True
                task["status"] = "连接中"
                self.control_task(task, "unpause")
                active += 1

        queue_positions = {}
        for task in queue_tasks:
            if task["queued"]:
                queue_positions[task["id"]] = len(queue_positions) + 1

        visible = set()
        for task_id, task in self.tasks.items():
            matches = self.filter_name == "全部任务" or task["status"] == self.filter_name
            if self.filter_name == "下载中":
                matches = task["status"] in ("连接中", "下载中")
            if not matches:
                continue
            visible.add(task_id)
            percent = task["completed"] / task["total"] * 100 if task["total"] else 0
            if task["status"] == "已完成":
                percent = 100
            values = (task["name"], readable_size(task["total"]) if task["total"] else "等待获取",
                      percent, task["status"], readable_size(task["speed"]) + "/s")
            # 副标题补充来源与已下载大小，错误原因可在下方日志查看。
            source = urlparse(task["url"]).hostname or ""
            detail = f"{source}  ·  已下载 {readable_size(task['completed'])}"
            if task.get("monitor_warning"):
                detail = "状态暂未更新  ·  引擎仍在运行，正在自动重连"
            if task["id"] in queue_positions:
                detail = f"队列第 {queue_positions[task['id']]} 位  ·  等待空闲下载位置"
            tags = ("complete",) if task["status"] == "已完成" else (("error",) if task["status"] == "下载失败" else ())
            if self.table.exists(task_id):
                self.table.item(task_id, values=values, tags=tags, detail=detail)
            else:
                self.table.insert("", "end", iid=task_id, values=values, tags=tags, detail=detail)
        for task_id in self.table.get_children():
            if task_id not in visible:
                self.table.delete(task_id)
        self.show_selected_log()
        speed = sum(task["speed"] for task in self.tasks.values() if task["status"] == "下载中")
        completed = sum(task["status"] == "已完成" for task in self.tasks.values())
        failed = sum(task["status"] == "下载失败" for task in self.tasks.values())
        self.stat_values["speed"].configure(text=readable_size(speed) + "/s")
        self.stat_values["active"].configure(text=str(active))
        self.stat_values["complete"].configure(text=str(completed))
        self.stat_values["queued"].configure(text=str(len(queue_positions)))
        self.task_count.configure(text=f"{len(visible)} 个任务")
        self.summary.configure(text=f"↓ {readable_size(speed)}/s     下载中 {active}/{self.max_active_downloads} · 排队 {len(queue_positions)} · 已完成 {completed} · 失败 {failed}     共 {len(self.tasks)} 项")
        if self.dirty and time.monotonic() - self.last_save >= 2:
            try:
                self.save_tasks()
            except OSError as error:
                self.summary.configure(text=f"任务记录未保存，请检查目录写入权限：{error}")
        self.root.after(REFRESH_INTERVAL_MS, self.refresh)

    def close(self):
        """关闭前停止引擎并保存记录；再次启动后可手动续传。"""
        self.closing = True
        self.summary.configure(text="正在保存续传信息，请稍候…")

        def shutdown():
            workers = []
            for task in self.tasks.values():
                worker = threading.Thread(target=self.stop_engine, args=(task,), daemon=True)
                worker.start()
                workers.append(worker)
            for worker in workers:
                worker.join()
            self.events.put(("", "closed", None))

        def finish():
            if any(task["process"] is not None and task["process"].poll() is None for task in self.tasks.values()):
                self.root.after(200, finish)
                return
            try:
                self.save_tasks()
            except OSError as error:
                messagebox.showerror("无法保存任务", str(error))
            self.root.destroy()

        threading.Thread(target=shutdown, daemon=True).start()
        self.root.after(300, finish)


def main():
    """检查下载引擎并启动界面；缺少文件时显示中文提示。"""
    root = tk.Tk()
    if not ARIA2_PATH.is_file():
        root.withdraw()
        messagebox.showerror("找不到 aria2", f"请把 aria2 软件包放在脚本旁边：\n{ARIA2_PATH}")
        root.destroy()
        return
    DownloadApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
