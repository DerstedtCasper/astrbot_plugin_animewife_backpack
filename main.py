from astrbot.api.all import *
from astrbot.api.star import StarTools
from datetime import datetime, timedelta
import random
import os
import json
import aiohttp
import asyncio

# ==================== 常量定义 ====================

PLUGIN_DIR = StarTools.get_data_dir("astrbot_plugin_animewife")
CONFIG_DIR = os.path.join(PLUGIN_DIR, "config")
IMG_DIR = os.path.join(PLUGIN_DIR, "img", "wife")

# 确保目录存在
os.makedirs(CONFIG_DIR, exist_ok=True)
os.makedirs(IMG_DIR, exist_ok=True)

# 数据文件路径
RECORDS_FILE = os.path.join(CONFIG_DIR, "records.json")
SWAP_REQUESTS_FILE = os.path.join(CONFIG_DIR, "swap_requests.json")
NTR_STATUS_FILE = os.path.join(CONFIG_DIR, "ntr_status.json")
BACKPACKS_KEY = "__wife_backpacks__"

# ==================== 全局数据存储 ====================

records = {  # 统一的记录数据结构
    "ntr": {},        # 牛老婆记录
    "change": {},     # 换老婆记录
    "reset": {},      # 重置使用次数
    "swap": {}        # 交换老婆请求次数
}
swap_requests = {}  # 交换请求数据
ntr_statuses = {}  # NTR 开关状态

# ==================== 并发锁 ====================

config_locks = {}      # 群组配置锁
records_lock = asyncio.Lock()  # 记录数据锁
swap_lock = asyncio.Lock()     # 交换请求锁
ntr_lock = asyncio.Lock()      # NTR 状态锁


def get_config_lock(group_id: str) -> asyncio.Lock:
    """获取或创建群组配置锁"""
    if group_id not in config_locks:
        config_locks[group_id] = asyncio.Lock()
    return config_locks[group_id]

def get_today():
    """获取当前上海时区日期字符串"""
    utc_now = datetime.utcnow()
    return (utc_now + timedelta(hours=8)).date().isoformat()


def load_json(path: str) -> dict:
    """安全加载 JSON 文件"""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return {}


def save_json(path: str, data: dict) -> None:
    """保存数据到 JSON 文件"""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def load_group_config(group_id: str) -> dict:
    """加载群组配置"""
    return load_json(os.path.join(CONFIG_DIR, f"{group_id}.json"))


def save_group_config(group_id: str, config: dict) -> None:
    """保存群组配置"""
    save_json(os.path.join(CONFIG_DIR, f"{group_id}.json"), config)


def normalize_backpack(raw: object, size: int) -> list:
    """将背包槽位标准化为固定长度 list[entry|None]，并兼容旧格式。"""
    if size <= 0:
        return []

    items: list = []
    if isinstance(raw, list):
        normalized = []
        for x in raw:
            if not x:
                normalized.append(None)
            elif isinstance(x, str):
                normalized.append(x)
            elif isinstance(x, dict) and isinstance(x.get("img"), str) and x.get("img"):
                # {"img": "...", "note": "..."}
                normalized.append({"img": x.get("img"), "note": x.get("note")})
            else:
                normalized.append(None)
        items = normalized
    elif isinstance(raw, dict):
        # 允许用 {"1": "xx.jpg"} 这种形式初始化/迁移
        items = [None] * size
        for k, v in raw.items():
            try:
                idx = int(k)
            except Exception:
                continue
            if 1 <= idx <= size and v:
                if isinstance(v, str):
                    items[idx - 1] = v
                elif isinstance(v, dict) and isinstance(v.get("img"), str) and v.get("img"):
                    items[idx - 1] = {"img": v.get("img"), "note": v.get("note")}
    else:
        items = []

    if len(items) < size:
        items.extend([None] * (size - len(items)))
    elif len(items) > size:
        items = items[:size]
    return items


def first_empty_slot(items: list) -> int | None:
    """返回第一个空槽位（1-based），没有空位则返回 None。"""
    for i, x in enumerate(items, start=1):
        if not x:
            return i
        if isinstance(x, dict) and not x.get("img"):
            return i
    return None


def backpack_entry_to_img_note(entry: object) -> tuple[str | None, str | None]:
    if not entry:
        return None, None
    if isinstance(entry, str):
        return entry, None
    if isinstance(entry, dict):
        img = entry.get("img")
        note = entry.get("note")
        return (img, note) if isinstance(img, str) and img else (None, None)
    return None, None


def make_backpack_entry(img: str, note: str | None = None) -> object:
    if note:
        return {"img": img, "note": note}
    return img


def format_backpack_item(entry: object) -> str:
    img, note = backpack_entry_to_img_note(entry)
    if not img:
        return "(空)"
    name = format_wife_name(img)
    if note:
        name += f"（{note}）"
    return name


def normalize_cmd_text(text: str) -> str:
    """兼容 /!# 前缀唤醒模式：返回去掉前缀后的文本。"""
    s = (text or "").strip()
    if s and s[0] in ("/", "!", "#"):
        return s[1:].lstrip()
    return s


def format_wife_name(img: str) -> str:
    """将图片文件名转换为展示名。"""
    name = os.path.splitext(img)[0].split("/")[-1]
    if "!" in name:
        source, chara = name.split("!", 1)
        return f"《{source}》的{chara}"
    return name


def load_ntr_statuses():
    """加载 NTR 开关状态"""
    raw = load_json(NTR_STATUS_FILE)
    ntr_statuses.clear()
    ntr_statuses.update(raw)


def save_ntr_statuses():
    """保存 NTR 开关状态"""
    save_json(NTR_STATUS_FILE, ntr_statuses)


# ==================== 数据加载和保存函数 ====================

def load_records():
    """加载所有记录数据"""
    raw = load_json(RECORDS_FILE)
    records.clear()
    records.update({
        "ntr": raw.get("ntr", {}),
        "change": raw.get("change", {}),
        "reset": raw.get("reset", {}),
        "swap": raw.get("swap", {})
    })


def save_records():
    """保存所有记录数据"""
    save_json(RECORDS_FILE, records)


def load_swap_requests():
    """加载交换请求并清理过期数据"""
    raw = load_json(SWAP_REQUESTS_FILE)
    today = get_today()
    cleaned = {}
    
    for gid, reqs in raw.items():
        valid = {uid: rec for uid, rec in reqs.items() if rec.get("date") == today}
        if valid:
            cleaned[gid] = valid
    
    globals()["swap_requests"] = cleaned
    if raw != cleaned:
        save_json(SWAP_REQUESTS_FILE, cleaned)


def save_swap_requests():
    """保存交换请求"""
    save_json(SWAP_REQUESTS_FILE, swap_requests)


# 初始加载所有数据
load_records()
load_swap_requests()
load_ntr_statuses()

# ==================== 主插件类 ====================


@register(
    "astrbot_plugin_animewife",
    "DerstedtCasper",
    "群二次元老婆插件（自用改版）",
    "1.8.1",
    "https://github.com/DerstedtCasper/astrbot_plugin_animewife",
)
class WifePlugin(Star):
    """二次元老婆插件主类"""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._init_config()
        self._init_commands()
        self.admins = self.load_admins()

    def _init_config(self):
        """初始化配置参数"""
        self.need_prefix = self.config.get("need_prefix")
        self.ntr_max = self.config.get("ntr_max")
        self.ntr_possibility = self.config.get("ntr_possibility")
        self.change_max_per_day = self.config.get("change_max_per_day")
        self.swap_max_per_day = self.config.get("swap_max_per_day")
        self.reset_max_uses_per_day = self.config.get("reset_max_uses_per_day")
        self.reset_success_rate = self.config.get("reset_success_rate")
        self.reset_mute_duration = self.config.get("reset_mute_duration")
        self.image_base_url = self.config.get("image_base_url")
        self.image_list_url = self.config.get("image_list_url")
        try:
            self.backpack_size = max(1, int(self.config.get("backpack_size") or 7))
        except Exception:
            self.backpack_size = 7

    def _init_commands(self):
        """初始化命令映射表"""
        self.commands = {
            "老婆帮助": self.wife_help,
            "抽老婆": self.animewife,
            "查老婆": self.search_wife,
            "替换老婆": self.replace_wife,
            "老婆背包": self.show_backpack,
            "发老婆": self.send_wife,
            "牛老婆": self.ntr_wife,
            "重置牛": self.reset_ntr,
            "切换ntr开关状态": self.switch_ntr,
            "换老婆": self.change_wife,
            "重置换": self.reset_change_wife,
            "交换老婆": self.swap_wife,
            "同意交换": self.agree_swap_wife,
            "拒绝交换": self.reject_swap_wife,
            "查看交换请求": self.view_swap_requests,
        }

    def load_admins(self) -> list:
        """加载管理员列表"""
        path = os.path.join("data", "cmd_config.json")
        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                cfg = json.load(f)
                admins = cfg.get("admins_id", [])
                return [str(admin_id) for admin_id in admins]
        except Exception:
            return []

    def parse_at_target(self, event: AstrMessageEvent) -> str | None:
        """解析消息中的@目标用户"""
        if not event.message_obj or not hasattr(event.message_obj, "message"):
            return None
        for comp in event.message_obj.message:
            if isinstance(comp, At):
                return str(comp.qq)
        return None

    def parse_target(self, event: AstrMessageEvent) -> str | None:
        """解析命令目标用户"""
        target = self.parse_at_target(event)
        if target:
            return target
        
        msg = normalize_cmd_text(event.message_str)
        # 兼容“昵称 + 额外参数”的用法，例如：
        # - /牛老婆 昵称 3
        # - /查老婆 昵称
        for cmd in ("牛老婆", "查老婆"):
            if msg.startswith(cmd):
                rest = msg[len(cmd):].strip()
                if not rest:
                    return None
                first = rest.split()[0].strip()
                # 如果第一个参数是数字，通常是编号参数，不当作昵称匹配
                if first.isdigit():
                    return None
                group_id = str(event.message_obj.group_id)
                cfg = load_group_config(group_id)
                for uid, data in cfg.items():
                    if isinstance(data, list) and len(data) > 2 and data[2] == first:
                        return uid
        return None

    # ==================== 消息处理 ====================

    @event_message_type(EventMessageType.GROUP_MESSAGE)
    async def on_all_messages(self, event: AstrMessageEvent, *args, **kwargs):
        """消息分发处理（仅群聊监听）"""
        if not event.message_obj or not hasattr(event.message_obj, "group_id"):
            return
        
        # 检查是否需要前缀唤醒
        if self.need_prefix and not event.is_at_or_wake_command:
            return
        
        text = normalize_cmd_text(event.message_str)
        for cmd, func in self.commands.items():
            if text.startswith(cmd):
                async for res in func(event):
                    yield res
                break

    # ==================== 抽老婆相关 ====================

    async def animewife(self, event: AstrMessageEvent, *, record_to_backpack: bool = True):
        """抽老婆"""
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        today = get_today()
        size = self.backpack_size

        new_draw = False
        auto_slot: int | None = None
        backpack_full = False
        backpack_items: list | None = None
        
        async with get_config_lock(gid):
            cfg = load_group_config(gid)
            wife_data = cfg.get(uid)
            
            if not wife_data or not isinstance(wife_data, list) or wife_data[1] != today:
                # 今天还没抽，获取新老婆
                img = await self._fetch_wife_image()
                if not img:
                    yield event.plain_result("抱歉，今天的老婆获取失败了，请稍后再试~")
                    return

                cfg[uid] = [img, today, nick]

                # 老婆背包：仅在“抽老婆”场景自动记录（避免换老婆/内部调用刷满背包）
                if record_to_backpack:
                    backpacks = cfg.get(BACKPACKS_KEY, {})
                    if not isinstance(backpacks, dict):
                        backpacks = {}

                    items = normalize_backpack(backpacks.get(uid), size)
                    slot = first_empty_slot(items)
                    if slot is not None:
                        items[slot - 1] = img
                        auto_slot = slot
                        backpacks[uid] = items
                        cfg[BACKPACKS_KEY] = backpacks
                    else:
                        backpack_full = True
                        backpack_items = items

                new_draw = True
                save_group_config(gid, cfg)
            else:
                img = wife_data[0]
        
        extra_lines: list[str] = []
        if new_draw and record_to_backpack:
            if auto_slot is not None:
                extra_lines.append(f"已自动存入老婆背包：{auto_slot}号位（容量 {size}）")
            elif backpack_full:
                extra_lines.append(f"你的老婆背包已满（{size}/{size}），今天抽到的老婆不会自动保存。")
                extra_lines.append(f"如需保存，请发送 /替换老婆 <1-{size}> 选择一个位置替换；否则明天刷新后将消失。")
                if backpack_items is not None:
                    lines = []
                    for i, x in enumerate(backpack_items, start=1):
                        lines.append(f"{i}. {format_backpack_item(x)}")
                    extra_lines.append("当前背包：\n" + "\n".join(lines))

        # 生成并发送消息
        yield event.chain_result(self._build_wife_message(img, nick, extra_lines=extra_lines or None))

    async def _fetch_wife_image(self) -> str | None:
        """获取老婆图片"""
        imgs = await self._list_wife_images()
        return random.choice(imgs) if imgs else None

    async def _list_wife_images(self) -> list[str]:
        """获取老婆图片文件名列表（本地优先，其次网络）。"""
        try:
            local_imgs = [
                x
                for x in os.listdir(IMG_DIR)
                if x and os.path.isfile(os.path.join(IMG_DIR, x))
            ]
            if local_imgs:
                return local_imgs
        except Exception:
            pass

        url = (self.image_list_url or self.image_base_url or "").strip()
        if not url:
            return []

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        return []
                    text = await resp.text()
                    return [line.strip() for line in text.splitlines() if line.strip()]
        except Exception:
            return []

    def _build_wife_message(self, img: str, nick: str, *, extra_lines: list[str] | None = None):
        """构建老婆消息链"""
        name = os.path.splitext(img)[0].split("/")[-1]
        
        if "!" in name:
            source, chara = name.split("!", 1)
            text = f"{nick}，你今天的老婆是来自《{source}》的{chara}，请好好珍惜哦~"
        else:
            text = f"{nick}，你今天的老婆是{name}，请好好珍惜哦~"

        if extra_lines:
            text += "\n" + "\n".join(extra_lines)
        
        path = os.path.join(IMG_DIR, img)
        try:
            chain = [
                Plain(text),
                (
                    Image.fromFileSystem(path)
                    if os.path.exists(path)
                    else Image.fromURL(self.image_base_url + img)
                ),
            ]
            return chain
        except Exception:
            return [Plain(text)]

    # ==================== 帮助命令 ====================

    async def wife_help(self, event: AstrMessageEvent):
        """显示帮助信息"""
        help_text = """
【基础命令】
• 抽老婆 - 每天抽取一个二次元老婆
• 老婆背包 - 查看自己的老婆背包列表
• 查老婆 <编号> - 查看自己的背包老婆(带图)
• 查老婆 [@用户] - 查看别人的老婆
• 替换老婆 <编号> - 用“今天的老婆”替换背包指定位置

【牛老婆功能】(概率较低😭)
• 牛老婆 [@用户] - 有概率抢走别人的今日老婆(额外入库到背包，不顶掉今日老婆位)
• 牛老婆 @用户 <编号> - 有概率抢走对方背包指定编号的老婆(额外入库到背包，不顶掉今日老婆位)
• 重置牛 [@用户] - 重置牛的次数(失败会禁言)

【换老婆功能】
• 换老婆 - 丢弃当前老婆换新的
• 重置换 [@用户] - 重置换老婆的次数(失败会禁言)

【交换功能】
• 交换老婆 [@用户] - 向别人发起老婆交换请求
• 同意交换 [@发起者] - 同意交换请求
• 拒绝交换 [@发起者] - 拒绝交换请求
• 查看交换请求 - 查看当前的交换请求

【管理员命令】
• 切换ntr开关状态 - 开启/关闭NTR功能
• 发老婆 @用户 <关键词> - 按关键词发一个老婆给对方(覆盖对方今日老婆，优先入库)

💡 提示：部分命令有每日使用次数限制
"""
        yield event.plain_result(help_text.strip())

    async def search_wife(self, event: AstrMessageEvent):
        """查老婆"""
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())

        # 用法1: 查老婆 <编号> -> 查自己的背包老婆
        msg = normalize_cmd_text(event.message_str)
        arg = msg[len("查老婆"):].strip() if msg.startswith("查老婆") else ""
        if arg:
            try:
                slot = int(arg.split()[0])
            except Exception:
                slot = None
            if slot is not None:
                async for res in self.view_backpack_wife(event, slot):
                    yield res
                return

        # 用法2: 查老婆 [@用户/昵称] -> 查对方今日老婆（兼容旧行为）
        tid = self.parse_target(event) or uid
        today = get_today()
        
        async with get_config_lock(gid):
            cfg = load_group_config(gid)
            wife_data = cfg.get(tid)
            
            if not wife_data or not isinstance(wife_data, list) or wife_data[1] != today:
                yield event.plain_result("没有发现老婆的踪迹，快去抽一个试试吧~")
                return
            
            img, _, owner = wife_data
        
        name = os.path.splitext(img)[0].split("/")[-1]
        
        if "!" in name:
            source, chara = name.split("!", 1)
            text = f"{owner}的老婆是来自《{source}》的{chara}，羡慕吗？"
        else:
            text = f"{owner}的老婆是{name}，羡慕吗？"
        
        path = os.path.join(IMG_DIR, img)
        try:
            chain = [
                Plain(text),
                (
                    Image.fromFileSystem(path)
                    if os.path.exists(path)
                    else Image.fromURL(self.image_base_url + img)
                ),
            ]
            yield event.chain_result(chain)
        except Exception:
            yield event.plain_result(text)

    async def view_backpack_wife(self, event: AstrMessageEvent, slot: int):
        """查自己的背包老婆（编号槽位）。"""
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        size = self.backpack_size

        if slot < 1 or slot > size:
            yield event.plain_result(f"{nick}，编号范围是 1-{size}。用法：/查老婆 <编号>")
            return

        async with get_config_lock(gid):
            cfg = load_group_config(gid)
            backpacks = cfg.get(BACKPACKS_KEY, {})
            if not isinstance(backpacks, dict):
                backpacks = {}

            items = normalize_backpack(backpacks.get(uid), size)
            entry = items[slot - 1] if slot - 1 < len(items) else None
            img, note = backpack_entry_to_img_note(entry)

        if not img:
            yield event.plain_result(f"{nick}，你的{slot}号老婆位还是空的哦~")
            return

        extra = f"（{note}）" if note else ""
        text = f"{nick}，你的{slot}号老婆是({format_wife_name(img)}{extra})，想起她了么~"
        path = os.path.join(IMG_DIR, img)
        try:
            chain = [
                Plain(text),
                (
                    Image.fromFileSystem(path)
                    if os.path.exists(path)
                    else Image.fromURL(self.image_base_url + img)
                ),
            ]
            yield event.chain_result(chain)
        except Exception:
            yield event.plain_result(text)

    async def replace_wife(self, event: AstrMessageEvent):
        """用今天的老婆替换背包指定槽位。"""
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        today = get_today()
        size = self.backpack_size

        msg = event.message_str.strip()
        msg = normalize_cmd_text(msg)
        arg = msg[len("替换老婆"):].strip() if msg.startswith("替换老婆") else ""
        if not arg:
            yield event.plain_result(f"{nick}，用法：/替换老婆 <1-{size}>")
            return

        try:
            slot = int(arg.split()[0])
        except Exception:
            yield event.plain_result(f"{nick}，用法：/替换老婆 <1-{size}>")
            return

        if slot < 1 or slot > size:
            yield event.plain_result(f"{nick}，编号范围是 1-{size}。")
            return

        err: str | None = None
        img: str | None = None
        async with get_config_lock(gid):
            cfg = load_group_config(gid)
            wife_data = cfg.get(uid)
            if not wife_data or not isinstance(wife_data, list) or wife_data[1] != today:
                err = f"{nick}，你今天还没有老婆，先 /抽老婆 再来替换吧~"
            else:
                img = wife_data[0]

            if img:
                backpacks = cfg.get(BACKPACKS_KEY, {})
                if not isinstance(backpacks, dict):
                    backpacks = {}

                items = normalize_backpack(backpacks.get(uid), size)
                items[slot - 1] = img
                backpacks[uid] = items
                cfg[BACKPACKS_KEY] = backpacks
                save_group_config(gid, cfg)

        if err:
            yield event.plain_result(err)
            return
        if not img:
            yield event.plain_result(f"{nick}，替换失败：未找到今天的老婆记录。")
            return

        yield event.plain_result(f"{nick}，已将今天的老婆存入{slot}号背包位：{format_wife_name(img)}")

    async def show_backpack(self, event: AstrMessageEvent):
        """显示自己的老婆背包列表。"""
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        size = self.backpack_size

        async with get_config_lock(gid):
            cfg = load_group_config(gid)
            backpacks = cfg.get(BACKPACKS_KEY, {})
            if not isinstance(backpacks, dict):
                backpacks = {}
            items = normalize_backpack(backpacks.get(uid), size)

        used = sum(1 for x in items if backpack_entry_to_img_note(x)[0])
        lines = [f"{i}. {format_backpack_item(x)}" for i, x in enumerate(items, start=1)]
        text = (
            f"{nick}，你的老婆背包（{used}/{size}）：\n"
            + "\n".join(lines)
            + "\n\n用 /查老婆 <编号> 查看对应老婆(带图)。"
        )
        yield event.plain_result(text)

    async def send_wife(self, event: AstrMessageEvent):
        """按关键词给指定用户发老婆（覆盖今日老婆，并尝试优先入库）。"""
        gid = str(event.message_obj.group_id)
        sender_uid = str(event.get_sender_id())
        sender_nick = event.get_sender_name()
        today = get_today()
        size = self.backpack_size

        if sender_uid not in self.admins:
            yield event.plain_result(f"{sender_nick}，该命令仅管理员可用哦~")
            return

        tid = self.parse_at_target(event)
        if not tid:
            yield event.plain_result(f"{sender_nick}，用法：/发老婆 @用户 <关键词>")
            return

        msg = normalize_cmd_text(event.message_str)
        rest = msg[len("发老婆"):].strip() if msg.startswith("发老婆") else ""
        if rest.startswith("@"):
            parts = rest.split()
            keyword = " ".join(parts[1:]).strip() if len(parts) > 1 else ""
        else:
            keyword = rest.strip()

        # 兜底：从消息链 Plain 中提取关键词
        if not keyword and event.message_obj and hasattr(event.message_obj, "message"):
            plain_text = "".join(
                seg.text for seg in event.message_obj.message if isinstance(seg, Plain)
            ).strip()
            if plain_text.startswith("发老婆"):
                keyword = plain_text[len("发老婆"):].strip()

        if not keyword:
            yield event.plain_result(f"{sender_nick}，请在命令后提供关键词。例如：/发老婆 @用户 澪")
            return

        all_imgs = await self._list_wife_images()
        kw = keyword.lower()
        matches = [
            img
            for img in all_imgs
            if kw in img.lower() or kw in format_wife_name(img).lower()
        ]
        if not matches:
            yield event.plain_result(f"{sender_nick}，没有找到包含“{keyword}”的老婆图片。")
            return

        img = random.choice(matches)

        target_name = None
        try:
            info = await event.bot.get_group_member_info(
                group_id=int(gid), user_id=int(tid)
            )
            target_name = info.get("card") or info.get("nickname")
        except Exception:
            target_name = None

        stored_slot: int | None = None
        is_full = False

        async with get_config_lock(gid):
            cfg = load_group_config(gid)
            if not target_name:
                prev = cfg.get(tid)
                if isinstance(prev, list) and len(prev) > 2 and prev[2]:
                    target_name = prev[2]
                else:
                    target_name = str(tid)

            # 覆盖今日老婆
            cfg[tid] = [img, today, target_name]

            # 优先入库：有空位就自动写入；满了则仅覆盖今日老婆位
            backpacks = cfg.get(BACKPACKS_KEY, {})
            if not isinstance(backpacks, dict):
                backpacks = {}
            items = normalize_backpack(backpacks.get(tid), size)
            slot = first_empty_slot(items)
            if slot is not None:
                items[slot - 1] = img
                backpacks[tid] = items
                cfg[BACKPACKS_KEY] = backpacks
                stored_slot = slot
            else:
                is_full = True

            save_group_config(gid, cfg)

        cancel_msg = await self.cancel_swap_on_wife_change(gid, [tid])

        name = format_wife_name(img)
        extra = (
            f"已存入对方背包 {stored_slot} 号位。"
            if stored_slot is not None
            else (
                f"对方背包已满，本次未自动保存；可让对方用 /替换老婆 <1-{size}> 保存。"
                if is_full
                else ""
            )
        )
        text = f"{sender_nick} 给 {target_name} 发了一位老婆：{name}。{extra}"
        path = os.path.join(IMG_DIR, img)
        try:
            chain = [
                Plain(text),
                (
                    Image.fromFileSystem(path)
                    if os.path.exists(path)
                    else Image.fromURL(self.image_base_url + img)
                ),
            ]
            yield event.chain_result(chain)
        except Exception:
            yield event.plain_result(text)

        if cancel_msg:
            yield event.plain_result(cancel_msg)

    # ==================== 牛老婆相关 ====================

    async def ntr_wife(self, event: AstrMessageEvent):
        """牛老婆"""
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        
        # 检查 NTR 功能是否启用
        if not ntr_statuses.get(gid, True):
            yield event.plain_result("牛老婆功能还没开启哦，请联系管理员开启~")
            return
        
        today = get_today()
        size = self.backpack_size

        # 解析可选背包编号：
        # - /牛老婆 @用户 <编号> : 牛走对方背包指定槽位
        # - /牛老婆 @用户       : 保持旧行为（牛走对方今日老婆）
        msg = normalize_cmd_text(event.message_str)
        rest = msg[len("牛老婆"):].strip() if msg.startswith("牛老婆") else ""
        slot: int | None = None
        if rest:
            parts = rest.split()
            if parts and parts[-1].isdigit():
                try:
                    slot_arg = int(parts[-1])
                except Exception:
                    slot_arg = None
                if slot_arg is not None:
                    if 1 <= slot_arg <= size:
                        slot = slot_arg
                    else:
                        yield event.plain_result(
                            f"{nick}，编号范围是 1-{size}。用法：/牛老婆 @用户 <编号>（不带编号则默认牛今天的老婆）"
                        )
                        return

        # 获取目标用户
        tid = self.parse_target(event)
        if not tid or tid == uid:
            if not tid:
                tip = "请@你想牛的对象，或输入完整的昵称哦~"
                if slot is not None or (rest.strip().isdigit() if rest else False):
                    tip = f"请@你想牛的对象。用法：/牛老婆 @用户 <1-{size}>（不带编号默认牛今天的老婆）"
                yield event.plain_result(f"{nick}，{tip}")
            else:
                yield event.plain_result(f"{nick}，不能牛自己呀，换个人试试吧~")
            return

        # 目标存在性检查（不消耗次数）
        async with get_config_lock(gid):
            cfg = load_group_config(gid)
            if slot is None:
                target_data = cfg.get(tid)
                if not target_data or not isinstance(target_data, list) or target_data[1] != today:
                    yield event.plain_result("对方今天还没有老婆可牛哦~")
                    return
            else:
                backpacks = cfg.get(BACKPACKS_KEY, {})
                if not isinstance(backpacks, dict):
                    backpacks = {}
                items = normalize_backpack(backpacks.get(tid), size)
                entry = items[slot - 1] if 0 <= slot - 1 < len(items) else None
                img, _ = backpack_entry_to_img_note(entry)
                if not img:
                    yield event.plain_result(f"对方背包的{slot}号位还是空的哦~")
                    return

        # 消耗一次牛老婆次数
        async with records_lock:
            grp = records["ntr"].setdefault(gid, {})
            rec = grp.get(uid, {"date": today, "count": 0})
            if rec.get("date") != today:
                rec = {"date": today, "count": 0}
            if rec["count"] >= self.ntr_max:
                yield event.plain_result(f"{nick}，你今天已经牛了{self.ntr_max}次啦，明天再来吧~")
                return
            rec["count"] += 1
            grp[uid] = rec
            save_records()
            rem = self.ntr_max - rec["count"]

        # 判断牛老婆是否成功
        if random.random() >= self.ntr_possibility:
            yield event.plain_result(f"{nick}，很遗憾，牛失败了！你今天还可以再试{rem}次~")
            return

        stolen_img: str | None = None
        stolen_from: str | None = None
        stored_slot: int | None = None
        is_full = False
        cancel_ids: list[str] = []

        async with get_config_lock(gid):
            cfg = load_group_config(gid)
            if slot is None:
                target_data = cfg.get(tid)
                if not target_data or not isinstance(target_data, list) or target_data[1] != today:
                    stolen_img = None
                else:
                    stolen_img = target_data[0]
                    stolen_from = target_data[2] if len(target_data) > 2 else str(tid)

                    # 目标用户失去今日老婆（保持“牛”语义）
                    del cfg[tid]
                    cancel_ids.append(tid)
            else:
                backpacks = cfg.get(BACKPACKS_KEY, {})
                if not isinstance(backpacks, dict):
                    backpacks = {}
                titems = normalize_backpack(backpacks.get(tid), size)
                entry = titems[slot - 1] if 0 <= slot - 1 < len(titems) else None
                img, _ = backpack_entry_to_img_note(entry)
                if not img:
                    stolen_img = None
                else:
                    stolen_img = img
                    tdata = cfg.get(tid)
                    if isinstance(tdata, list) and len(tdata) > 2 and tdata[2]:
                        stolen_from = tdata[2]
                    else:
                        stolen_from = str(tid)

                    # 目标用户失去背包指定槽位老婆
                    titems[slot - 1] = None
                    backpacks[tid] = titems
                    cfg[BACKPACKS_KEY] = backpacks

            if stolen_img:
                # 额外入库到背包，并带备注（不顶掉自己的今日老婆位）
                backpacks = cfg.get(BACKPACKS_KEY, {})
                if not isinstance(backpacks, dict):
                    backpacks = {}
                items = normalize_backpack(backpacks.get(uid), size)
                empty_slot = first_empty_slot(items)
                note = f"牛自用户 {stolen_from}" if stolen_from else "牛自用户"
                if empty_slot is not None:
                    items[empty_slot - 1] = make_backpack_entry(stolen_img, note)
                    backpacks[uid] = items
                    cfg[BACKPACKS_KEY] = backpacks
                    stored_slot = empty_slot
                else:
                    is_full = True

                save_group_config(gid, cfg)

        # 目标在二次校验中消失：退还次数
        if not stolen_img:
            async with records_lock:
                grp = records["ntr"].setdefault(gid, {})
                rec = grp.get(uid, {"date": today, "count": 0})
                if rec.get("date") == today and rec.get("count", 0) > 0:
                    rec["count"] = max(0, rec["count"] - 1)
                    grp[uid] = rec
                    save_records()
            yield event.plain_result(f"{nick}，对方的老婆刚刚溜走了，这次不算次数，再试试吧~")
            return

        cancel_msg = await self.cancel_swap_on_wife_change(gid, cancel_ids) if cancel_ids else None

        name = format_wife_name(stolen_img)
        note_suffix = f"（牛自用户 {stolen_from}）" if stolen_from else ""
        src_suffix = f"（来自对方背包{slot}号位）" if slot is not None else ""
        keep_suffix = "不会顶掉你今天抽到的老婆位。"
        if stored_slot is not None:
            text = f"{nick}，牛老婆成功！你牛到了 {name}{note_suffix}{src_suffix}，已存入背包{stored_slot}号位~{keep_suffix}"
        else:
            text = f"{nick}，牛老婆成功！你牛到了 {name}{note_suffix}{src_suffix}，但你的背包已满，本次未保存~{keep_suffix}"

        path = os.path.join(IMG_DIR, stolen_img)
        try:
            chain = [
                Plain(text),
                (
                    Image.fromFileSystem(path)
                    if os.path.exists(path)
                    else Image.fromURL(self.image_base_url + stolen_img)
                ),
            ]
            yield event.chain_result(chain)
        except Exception:
            yield event.plain_result(text)

        if cancel_msg:
            yield event.plain_result(cancel_msg)

    async def switch_ntr(self, event: AstrMessageEvent):
        """切换 NTR 开关（仅管理员）"""
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        
        if uid not in self.admins:
            yield event.plain_result(f"{nick}，你没有权限操作哦~")
            return
        
        gid = str(event.message_obj.group_id)
        async with ntr_lock:
            current_status = ntr_statuses.get(gid, True)
            ntr_statuses[gid] = not current_status
            save_ntr_statuses()
        
        state = "开启" if not current_status else "关闭"
        yield event.plain_result(f"{nick}，NTR已{state}")

    # ==================== 换老婆相关 ====================

    async def change_wife(self, event: AstrMessageEvent):
        """换老婆"""
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        today = get_today()
        
        async with records_lock:
            # 检查每日换老婆次数
            recs = records["change"].setdefault(gid, {})
            rec = recs.get(uid, {"date": "", "count": 0})
            
            if rec["date"] == today and rec["count"] >= self.change_max_per_day:
                yield event.plain_result(f"{nick}，你今天已经换了{self.change_max_per_day}次老婆啦，明天再来吧~")
                return
        
        # 检查是否有老婆并删除
        async with get_config_lock(gid):
            cfg = load_group_config(gid)
            if uid not in cfg or cfg[uid][1] != today:
                yield event.plain_result(f"{nick}，你今天还没有老婆，先去抽一个再来换吧~")
                return
            
            # 删除老婆
            del cfg[uid]
            save_group_config(gid, cfg)
        
        # 更新记录
        async with records_lock:
            if rec["date"] != today:
                rec = {"date": today, "count": 1}
            else:
                rec["count"] += 1
            recs[uid] = rec
            save_records()
        
        # 取消相关交换请求
        cancel_msg = await self.cancel_swap_on_wife_change(gid, [uid])
        if cancel_msg:
            yield event.plain_result(cancel_msg)
        
        # 立即展示新老婆
        async for res in self.animewife(event, record_to_backpack=False):
            yield res

    # ==================== 重置相关 ====================

    async def reset_ntr(self, event: AstrMessageEvent):
        """重置牛老婆次数"""
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        today = get_today()
        
        # 管理员可直接重置他人
        if uid in self.admins:
            tid = self.parse_at_target(event) or uid
            async with records_lock:
                if gid in records["ntr"] and tid in records["ntr"][gid]:
                    del records["ntr"][gid][tid]
                    save_records()
            yield event.chain_result([
                Plain("管理员操作：已重置"), At(qq=int(tid)), Plain("的牛老婆次数。")
            ])
            return
        
        # 普通用户使用重置机会
        async with records_lock:
            grp = records["reset"].setdefault(gid, {})
            rec = grp.get(uid, {"date": today, "count": 0})
            
            if rec.get("date") != today:
                rec = {"date": today, "count": 0}
            
            if rec["count"] >= self.reset_max_uses_per_day:
                yield event.plain_result(f"{nick}，你今天已经用完{self.reset_max_uses_per_day}次重置机会啦，明天再来吧~")
                return
            
            rec["count"] += 1
            grp[uid] = rec
            save_records()
        
        tid = self.parse_at_target(event) or uid
        
        if random.random() < self.reset_success_rate:
            async with records_lock:
                if gid in records["ntr"] and tid in records["ntr"][gid]:
                    del records["ntr"][gid][tid]
                    save_records()
            yield event.chain_result([
                Plain("已重置"), At(qq=int(tid)), Plain("的牛老婆次数。")
            ])
        else:
            try:
                await event.bot.set_group_ban(group_id=int(gid), user_id=int(uid), duration=self.reset_mute_duration)
            except Exception:
                pass
            yield event.plain_result(f"{nick}，重置牛失败，被禁言{self.reset_mute_duration}秒，下次记得再接再厉哦~")

    async def reset_change_wife(self, event: AstrMessageEvent):
        """重置换老婆次数"""
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        today = get_today()
        
        # 管理员可直接重置他人
        if uid in self.admins:
            tid = self.parse_at_target(event) or uid
            async with records_lock:
                grp = records["change"].setdefault(gid, {})
                if tid in grp:
                    del grp[tid]
                    save_records()
            yield event.chain_result([
                Plain("管理员操作：已重置"), At(qq=int(tid)), Plain("的换老婆次数。")
            ])
            return
        
        # 普通用户使用重置机会
        async with records_lock:
            grp = records["reset"].setdefault(gid, {})
            rec = grp.get(uid, {"date": today, "count": 0})
            
            if rec.get("date") != today:
                rec = {"date": today, "count": 0}
            
            if rec["count"] >= self.reset_max_uses_per_day:
                yield event.plain_result(f"{nick}，你今天已经用完{self.reset_max_uses_per_day}次重置机会啦，明天再来吧~")
                return
            
            rec["count"] += 1
            grp[uid] = rec
            save_records()
        
        tid = self.parse_at_target(event) or uid
        
        if random.random() < self.reset_success_rate:
            async with records_lock:
                grp2 = records["change"].setdefault(gid, {})
                if tid in grp2:
                    del grp2[tid]
                    save_records()
            yield event.chain_result([
                Plain("已重置"), At(qq=int(tid)), Plain("的换老婆次数。")
            ])
        else:
            try:
                await event.bot.set_group_ban(group_id=int(gid), user_id=int(uid), duration=self.reset_mute_duration)
            except Exception:
                pass
            yield event.plain_result(f"{nick}，重置换失败，被禁言{self.reset_mute_duration}秒，下次记得再接再厉哦~")

    # ==================== 交换老婆相关 ====================

    async def swap_wife(self, event: AstrMessageEvent):
        """发起交换老婆请求"""
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        tid = self.parse_at_target(event)
        nick = event.get_sender_name()
        today = get_today()
        
        async with records_lock:
            # 检查每日交换请求次数
            grp_limit = records["swap"].setdefault(gid, {})
            rec_lim = grp_limit.get(uid, {"date": "", "count": 0})
            
            if rec_lim["date"] != today:
                rec_lim = {"date": today, "count": 0}
            
            if rec_lim["count"] >= self.swap_max_per_day:
                yield event.plain_result(f"{nick}，你今天已经发起了{self.swap_max_per_day}次交换请求啦，明天再来吧~")
                return
        
        if not tid or tid == uid:
            yield event.plain_result(f"{nick}，请在命令后@你想交换的对象哦~")
            return
        
        # 检查双方是否都有老婆
        async with get_config_lock(gid):
            cfg = load_group_config(gid)
            for x in (uid, tid):
                if x not in cfg or cfg[x][1] != today:
                    who = nick if x == uid else "对方"
                    yield event.plain_result(f"{who}，今天还没有老婆，无法进行交换哦~")
                    return
        
        # 记录交换请求
        async with records_lock:
            rec_lim["count"] += 1
            grp_limit[uid] = rec_lim
            save_records()
        
        async with swap_lock:
            grp = swap_requests.setdefault(gid, {})
            grp[uid] = {"target": tid, "date": today}
            save_swap_requests()
        
        yield event.chain_result([
            Plain(f"{nick} 想和 "), At(qq=int(tid)),
            Plain(" 交换老婆啦！请对方用\"同意交换 @发起者\"或\"拒绝交换 @发起者\"来回应~")
        ])

    async def agree_swap_wife(self, event: AstrMessageEvent):
        """同意交换老婆"""
        gid = str(event.message_obj.group_id)
        tid = str(event.get_sender_id())
        uid = self.parse_at_target(event)
        nick = event.get_sender_name()
        
        # 检查和删除交换请求（原子操作）
        async with swap_lock:
            grp = swap_requests.get(gid, {})
            rec = grp.get(uid)
            
            if not rec or rec.get("target") != tid:
                yield event.plain_result(f"{nick}，请在命令后@发起者，或用\"查看交换请求\"命令查看当前请求哦~")
                return
            
            # 删除请求
            del grp[uid]
        
        # 执行交换
        async with get_config_lock(gid):
            cfg = load_group_config(gid)
            cfg[uid][0], cfg[tid][0] = cfg[tid][0], cfg[uid][0]
            save_group_config(gid, cfg)
        
        # 保存交换请求删除
        save_swap_requests()
        
        # 取消相关交换请求
        cancel_msg = await self.cancel_swap_on_wife_change(gid, [uid, tid])
        
        yield event.plain_result("交换成功！你们的老婆已经互换啦，祝幸福~")
        if cancel_msg:
            yield event.plain_result(cancel_msg)

    async def reject_swap_wife(self, event: AstrMessageEvent):
        """拒绝交换老婆"""
        gid = str(event.message_obj.group_id)
        tid = str(event.get_sender_id())
        uid = self.parse_at_target(event)
        nick = event.get_sender_name()
        
        async with swap_lock:
            grp = swap_requests.get(gid, {})
            rec = grp.get(uid)
            
            if not rec or rec.get("target") != tid:
                yield event.plain_result(f"{nick}，请在命令后@发起者，或用\"查看交换请求\"命令查看当前请求哦~")
                return
            
            del grp[uid]
            save_swap_requests()
        
        yield event.chain_result([
            At(qq=int(uid)), Plain("，对方婉拒了你的交换请求，下次加油吧~")
        ])

    async def view_swap_requests(self, event: AstrMessageEvent):
        """查看当前交换请求"""
        gid = str(event.message_obj.group_id)
        me = str(event.get_sender_id())
        
        grp = swap_requests.get(gid, {})
        cfg = load_group_config(gid)
        
        # 获取发起的和收到的请求
        sent_targets = [rec["target"] for uid, rec in grp.items() if uid == me]
        received_from = [uid for uid, rec in grp.items() if rec.get("target") == me]
        
        if not sent_targets and not received_from:
            yield event.plain_result("你当前没有任何交换请求哦~")
            return
        
        parts = []
        for tid in sent_targets:
            name = cfg.get(tid, [None, None, "未知用户"])[2]
            parts.append(f"→ 你发起给 {name} 的交换请求")
        
        for uid in received_from:
            name = cfg.get(uid, [None, None, "未知用户"])[2]
            parts.append(f"→ {name} 发起给你的交换请求")
        
        text = "当前交换请求如下：\n" + "\n".join(parts) + "\n请在\"同意交换\"或\"拒绝交换\"命令后@发起者进行操作~"
        yield event.plain_result(text)

    # ==================== 辅助方法 ====================

    async def cancel_swap_on_wife_change(self, gid: str, user_ids: list) -> str | None:
        """检查并取消与指定用户相关的交换请求"""
        today = get_today()
        grp = swap_requests.get(gid, {})
        grp_limit = records["swap"].setdefault(gid, {})
        
        # 找出需要取消的交换请求
        to_cancel = [
            req_uid for req_uid, req in grp.items()
            if req_uid in user_ids or req.get("target") in user_ids
        ]
        
        if not to_cancel:
            return None
        
        # 取消请求并返还次数
        for req_uid in to_cancel:
            rec_lim = grp_limit.get(req_uid, {"date": "", "count": 0})
            if rec_lim.get("date") == today and rec_lim.get("count", 0) > 0:
                rec_lim["count"] = max(0, rec_lim["count"] - 1)
                grp_limit[req_uid] = rec_lim
            del grp[req_uid]
        
        save_swap_requests()
        save_records()
        
        return f"已自动取消 {len(to_cancel)} 条相关的交换请求并返还次数~"

    async def terminate(self):
        """插件卸载时清理资源"""
        global config_locks, records, swap_requests, ntr_statuses
        
        # 清理群组配置锁
        config_locks.clear()
        
        # 清理全局数据
        records.clear()
        swap_requests.clear()
        ntr_statuses.clear()
