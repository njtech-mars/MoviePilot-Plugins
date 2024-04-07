import datetime
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import db
from app.core.config import settings
from app.core.event import Event, eventmanager
from app.db.models.transferhistory import TransferHistory
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.transfer import TransferInfo
from app.schemas.types import EventType


class RSoftlinking(_PluginBase):
    plugin_name = "反向软链接"
    plugin_desc = "转移完成后，在原处留下一个软链接，指向新的位置"
    # 插件版本
    plugin_version = "0.1"
    # 插件作者
    plugin_author = "mars studio"
    # 作者主页
    author_url = "https://github.com/njtech-mars/"
    # 插件配置项ID前缀
    plugin_config_prefix = "rsoftlinking_"
    # 加载顺序
    plugin_order = 5
    # 可使用的用户级别
    auth_level = 1

    _scheduler = None

    _enabled = False
    _onlyonce = False
    _enforced = False
    _enabled_dirs = []
    _cron = None

    def init_plugin(self, config: Dict = None):
        # 读取配置
        if config:
            self._enabled = config.get("enabled")
            self._enforced = config.get("enforced")
            self._onlyonce = config.get("onlyonce")
            self._enabled_dirs = list(
                map(lambda x: Path(x), config.get("enabled_dirs").split("\n"))
            )
            self._cron = config.get("cron")
        self.stop_service()

        if self._enabled or self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
        if self._onlyonce:
            logger.info("立即运行一次")
            self._scheduler.add_job(
                func=self._active_probe,
                trigger="date",
                run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ))
                + datetime.timedelta(seconds=3),
            )
            self._onlyonce = False
            self.__update_config()
        if self._scheduler and self._scheduler.get_jobs():
            self._scheduler.print_jobs()
            self._scheduler.start()

    def __update_config(self):
        """
        更新配置
        """
        self.update_config(
            {
                "enabled": self._enabled,
                "enforced": self._enforced,
                "onlyonce": self._onlyonce,
                "enabled_dirs": "\n".join(map(str, self._enabled_dirs)),
                "cron": self._cron,
            }
        )

    def _is_valid_link(self, src: str, dst: str) -> bool:
        """
        检查软链接是否有效
        """
        srcp = Path(src)
        dstp = Path(dst)
        return (
            srcp.exists()
            and srcp.is_symlink()
            and srcp.resolve() == dstp
            and dstp.exists()
        )

    def _rsoftlink(self, src, dst):
        src_dir = os.path.dirname(src)
        if self._enabled_dirs and not any(
            map(
                lambda x: Path(os.path.commonprefix((x, src_dir))) == x,
                self._enabled_dirs,
            )
        ):
            return
        dst = Path(dst)
        if not (dst.exists() and dst.is_file()):
            logger.warn(f"目标文件不存在：{dst}，跳过...")
            return
        if dst.is_symlink() and dst.resolve() == src:
            logger.warn(
                f"目标文件`{dst}`为软链接并指向源文件`{src}`，可能产生循环引用，跳过..."
            )
            return
        os.makedirs(src_dir, exist_ok=True)
        if os.path.exists(src):
            logger.warn(f"源文件已存在：{src}")
            if self._is_valid_link(src, dst):
                logger.info(f"软链接已存在：{src} -> {dst}")
                return
            if not self._enforced:
                logger.warn(
                    f"源文件`{src}`已存在，并不符合链接，未启用强制执行，跳过..."
                )
                return
            logger.info(f"强制执行：{src}")
            os.remove(src)
        os.symlink(dst, src)
        logger.info(f"反向软链接：{src} -> {dst}")

    def _active_probe(self):
        """
        主动扫描迁移记录，并设置遗漏的软链接
        """
        session = db.get_db().__next__()
        counts = TransferHistory.count(session, True)
        page = 1
        while counts:
            for transfer in TransferHistory.list_by_page(
                session, status=True, page=page
            ):
                files: List[str] = json.loads(transfer.files)
                if len(files) == 1 and files[0] == transfer.src:
                    self._rsoftlink(transfer.src, transfer.dest)
                else:
                    src = Path(transfer.src)
                    dst = Path(transfer.dest)
                    if src.is_dir() and dst.is_dir():
                        for f in files:
                            filename = Path(f).name
                            self._rsoftlink(f, dst / filename)
                    else:
                        logger.warn(f"历史记录数据无法识别:{transfer}，跳过...")
            page += 1
            counts -= 1

    @eventmanager.register(EventType.TransferComplete)
    def transfer_complete_event_handler(self, event: Event):
        if not self._enabled:
            return
        transfer_info: TransferInfo = event.event_data["transferinfo"]
        if not transfer_info.success:
            logger.warn("转移失败，跳过...")
            return
        if len(transfer_info.file_list) != len(transfer_info.file_list_new):
            logger.error("file_list and file_list_new not match\t事件格式错误")
            return
        for src, dst in zip(transfer_info.file_list, transfer_info.file_list_new):
            self._rsoftlink(src, dst)

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return (
            [
                {
                    "component": "VForm",
                    "content": [
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 4},
                                    "content": [
                                        {
                                            "component": "VSwitch",
                                            "props": {
                                                "model": "enabled",
                                                "label": "启用插件",
                                            },
                                        }
                                    ],
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 4},
                                    "content": [
                                        {
                                            "component": "VSwitch",
                                            "props": {
                                                "model": "enforced",
                                                "label": "强制执行",
                                            },
                                        }
                                    ],
                                },
                            ],
                        },
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 4},
                                    "content": [
                                        {
                                            "component": "VTextField",
                                            "props": {
                                                "model": "cron",
                                                "label": "定时主动扫描周期",
                                                "placeholder": "5位cron表达式，留空关闭",
                                            },
                                        }
                                    ],
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 4},
                                    "content": [
                                        {
                                            "component": "VSwitch",
                                            "props": {
                                                "model": "onlyonce",
                                                "label": "立即运行一次",
                                            },
                                        }
                                    ],
                                },
                            ],
                        },
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12},
                                    "content": [
                                        {
                                            "component": "VTextarea",
                                            "props": {
                                                "model": "enabled_dirs",
                                                "label": "启用目录",
                                                "rows": 5,
                                                "placeholder": "每一行一个目录，该目录应当是 *转移前的目录*\n"
                                                "仅会对此处列出的目录执行操作，若为空，则默认全部文件",
                                            },
                                        }
                                    ],
                                }
                            ],
                        },
                    ],
                },
            ],
            {
                "enabled": False,
                "enforced": False,
                "onlyonce": False,
                "enabled_dirs": "",
                "cron": "",
            },
        )

    def get_page(self) -> List[Dict]:
        pass

    def get_state(self) -> bool:
        return self._enabled

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            return [
                {
                    "id": "RSoftlinking",
                    "name": "反向软链接-扫描迁移记录",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self._active_probe,
                    "kwargs": {},
                }
            ]
        return []

    def stop_service(self):
        if self._scheduler:
            self._scheduler.remove_all_jobs()
            if self._scheduler.running:
                self._scheduler.shutdown()
            self._scheduler = None

    def get_api(self) -> List[Dict[str, Any]]:
        return super().get_api()

    def get_command(self) -> List[str]:
        return super().get_command()
