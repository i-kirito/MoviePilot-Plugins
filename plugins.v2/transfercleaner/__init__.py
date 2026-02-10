import os
import threading
import time
from pathlib import Path
from queue import Queue, Empty
from typing import List, Tuple, Dict, Any, Optional

from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import desc
from sqlalchemy.exc import SQLAlchemyError

from app.db import SessionFactory
from app.db.models.transferhistory import TransferHistory
from app.db.transferhistory_oper import TransferHistoryOper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType

try:
    from app.chain.transfer import TransferChain
except ImportError:
    TransferChain = None


class FileMonitorHandler(FileSystemEventHandler):
    """
    目录监控事件处理器
    """

    def __init__(self, monpath: str, plugin: Any, **kwargs):
        super(FileMonitorHandler, self).__init__(**kwargs)
        self._watch_path = monpath
        self.plugin = plugin

    def on_deleted(self, event):
        """文件删除事件"""
        logger.debug(f"TransferCleaner: [Watchdog] on_deleted 触发, is_dir={event.is_directory}, path={event.src_path}")
        if event.is_directory:
            return
        self.plugin.handle_file_event("deleted", event.src_path)

    def on_moved(self, event):
        """文件移动事件"""
        logger.debug(f"TransferCleaner: [Watchdog] on_moved 触发, is_dir={event.is_directory}, src={event.src_path}, dest={event.dest_path}")
        if event.is_directory:
            return
        # 移动事件使用 src_path（旧路径）来匹配历史记录
        self.plugin.handle_file_event("moved", event.src_path, event.dest_path)


class TransferCleaner(_PluginBase):
    # 插件名称
    plugin_name = "转移记录清理"
    # 插件描述
    plugin_desc = "监控目录文件变化，自动删除对应的转移历史记录。支持路径映射，适用于115网盘等场景。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/Ombi_A.png"
    # 插件版本
    plugin_version = "2.3.0"
    # 插件作者
    plugin_author = "i-kirito"
    # 作者主页
    author_url = "https://github.com/i-kirito"
    # 插件配置项ID前缀
    plugin_config_prefix = "transfercleaner_"
    # 加载顺序
    plugin_order = 0
    # 可使用的用户级别
    auth_level = 1

    # 私有属性（类级别默认值，实例属性在 init_plugin 中初始化）
    _enabled: bool = False
    _notify: bool = True
    _dry_run: bool = True
    _delay_enabled: bool = False
    _delay_seconds: int = 10
    _monitor_dirs: str = ""
    _path_mappings: str = ""
    _exclude_dirs: str = ""
    _exclude_keywords: str = ""
    _clean_dirs: str = ""
    _run_once: bool = False
    _retransfer_once: bool = False
    _retransfer_dirs: str = ""
    _retransfer_cron: str = ""
    _clean_failed: bool = False
    _observers: List[PollingObserver] = None
    _transferhistory: Optional[TransferHistoryOper] = None
    # 事件去重缓存 {path: timestamp}
    _event_cache: Dict[str, float] = None
    _event_cache_lock: threading.Lock = None
    # 预编译的排除关键词列表
    _exclude_keywords_list: List[str] = None
    # 预编译的不删除目录列表
    _exclude_dirs_list: List[str] = None
    # 预编译的路径映射 {本地路径前缀: 存储路径前缀}
    _path_mappings_dict: Dict[str, str] = None
    # 预编译的清理目录列表（反向映射：存储路径前缀 -> 本地路径前缀）
    _reverse_mappings_dict: Dict[str, str] = None
    # 去重时间窗口（秒）
    _dedupe_ttl: int = 3
    # 事件缓存清理周期（秒）
    _event_cache_cleanup_interval: int = 30
    _last_event_cache_cleanup: float = 0
    # 临时文件后缀（全部小写）
    _temp_suffixes: List[str] = [".!qb", ".part", ".mp", ".tmp"]
    # 延迟删除队列
    _delay_queue: Queue = None
    _delay_thread: threading.Thread = None
    _stop_event: threading.Event = None
    # 通知聚合
    _notify_buffer: List[str] = None
    _notify_buffer_lock: threading.Lock = None
    _notify_timer: threading.Timer = None
    _notify_delay: int = 10  # 通知延迟秒数

    def init_plugin(self, config: dict = None):
        """初始化插件"""
        # 初始化实例属性（避免类级可变状态共享）
        self._observers = []
        self._event_cache = {}
        self._event_cache_lock = threading.Lock()
        self._last_event_cache_cleanup = time.time()
        self._exclude_keywords_list = []
        self._exclude_dirs_list = []
        self._path_mappings_dict = {}
        self._reverse_mappings_dict = {}
        self._delay_queue = Queue()
        self._stop_event = threading.Event()
        # 通知聚合初始化
        self._notify_buffer = []
        self._notify_buffer_lock = threading.Lock()
        self._notify_timer = None

        self._transferhistory = TransferHistoryOper()

        if config:
            self._enabled = config.get("enabled", False)
            self._notify = config.get("notify", True)
            self._dry_run = config.get("dry_run", True)
            self._delay_enabled = config.get("delay_enabled", False)
            self._delay_seconds = int(config.get("delay_seconds", 10) or 10)
            self._monitor_dirs = config.get("monitor_dirs", "")
            self._path_mappings = config.get("path_mappings", "")
            self._exclude_dirs = config.get("exclude_dirs", "")
            self._exclude_keywords = config.get("exclude_keywords", "")
            self._clean_dirs = config.get("clean_dirs", "")
            self._run_once = config.get("run_once", False)
            self._retransfer_once = config.get("retransfer_once", False)
            self._retransfer_dirs = config.get("retransfer_dirs", "")
            self._retransfer_cron = config.get("retransfer_cron", "")
            self._clean_failed = config.get("clean_failed", False)
            # 预编译排除关键词列表
            self._exclude_keywords_list = [
                k.strip() for k in self._exclude_keywords.split("\n") if k.strip()
            ]
            # 预编译不删除目录列表
            self._exclude_dirs_list = [
                d.strip() for d in self._exclude_dirs.split("\n") if d.strip()
            ]
            # 预编译路径映射
            self._path_mappings_dict = self._parse_path_mappings()
            # 预编译反向路径映射（用于清理任务）
            self._reverse_mappings_dict = {v: k for k, v in self._path_mappings_dict.items()}

        logger.info(
            f"TransferCleaner 插件初始化，"
            f"enabled={self._enabled}, dry_run={self._dry_run}, "
            f"delay_enabled={self._delay_enabled}, delay_seconds={self._delay_seconds}, "
            f"path_mappings={len(self._path_mappings_dict)}个"
        )

        # 停止现有监控
        self.stop_service()

        if self._enabled:
            self._start_monitoring()
            # 启动延迟删除线程
            if self._delay_enabled:
                self._start_delay_worker()

        # 检查是否需要立即运行清理任务
        if self._run_once:
            # 启动清理任务（在任务完成后重置开关）
            threading.Thread(
                target=self._run_cleanup_task_wrapper,
                daemon=True,
                name="TransferCleaner-Cleanup"
            ).start()

        # 检查是否需要立即运行重新整理任务
        if self._retransfer_once:
            # 启动重新整理任务（在任务完成后重置开关）
            threading.Thread(
                target=self._run_retransfer_task_wrapper,
                daemon=True,
                name="TransferCleaner-Retransfer"
            ).start()

    def _run_cleanup_task_wrapper(self):
        """清理任务包装器，完成后重置开关"""
        try:
            self._run_cleanup_task()
        finally:
            # 重置开关
            self._run_once = False
            self.__update_config()

    def _run_retransfer_task_wrapper(self):
        """重新整理任务包装器（立即执行，不重置开关）"""
        self._run_retransfer_task()

    def __update_config(self):
        """更新配置（用于重置 run_once 开关）"""
        self.update_config({
            "enabled": self._enabled,
            "notify": self._notify,
            "dry_run": self._dry_run,
            "delay_enabled": self._delay_enabled,
            "delay_seconds": self._delay_seconds,
            "monitor_dirs": self._monitor_dirs,
            "path_mappings": self._path_mappings,
            "exclude_dirs": self._exclude_dirs,
            "exclude_keywords": self._exclude_keywords,
            "clean_dirs": self._clean_dirs,
            "run_once": False,
            "retransfer_once": self._retransfer_once,
            "retransfer_dirs": self._retransfer_dirs,
            "retransfer_cron": self._retransfer_cron,
            "clean_failed": self._clean_failed,
        })

    def _parse_path_mappings(self) -> Dict[str, str]:
        """
        解析路径映射配置
        格式: 本地目录:存储路径
        例如: /media/115/转存:/115/转存
        返回: {本地路径前缀: 存储路径前缀}
        """
        mappings = {}
        if not self._path_mappings:
            return mappings

        for line in self._path_mappings.split("\n"):
            line = line.strip()
            if not line or ":" not in line:
                continue
            try:
                # 格式: 本地目录:存储类型:存储路径 或 本地目录:存储路径
                parts = line.split(":", 2)
                if len(parts) == 2:
                    # 本地目录:存储路径（存储路径可能包含存储类型前缀如 u115:）
                    local_path = parts[0].strip()
                    storage_path = parts[1].strip()
                    mappings[local_path] = storage_path
                elif len(parts) == 3:
                    # 本地目录:存储类型:存储路径
                    local_path = parts[0].strip()
                    storage_type = parts[1].strip()
                    storage_path = parts[2].strip()
                    # 组合成完整的存储路径
                    mappings[local_path] = f"{storage_type}:{storage_path}"
                else:
                    logger.warning(f"TransferCleaner: 无效的路径映射配置: {line}")
                    continue

                logger.info(f"TransferCleaner: 路径映射 {local_path} -> {mappings[local_path]}")

            except (AttributeError, TypeError, ValueError) as e:
                logger.warning(f"TransferCleaner: 解析路径映射失败 {line}: {e}")

        return mappings

    def _convert_storage_to_local(self, storage_path: str) -> str:
        """
        将存储路径转换为本地路径（用于检查文件是否存在）

        :param storage_path: 数据库中的存储路径
        :return: 转换后的本地路径，如果没有匹配的映射则返回原路径
        """
        for storage_prefix, local_prefix in self._reverse_mappings_dict.items():
            if storage_path.startswith(storage_prefix):
                # 计算相对路径
                relative_path = storage_path[len(storage_prefix):].lstrip("/")
                # 构建本地路径
                local_path = local_prefix.rstrip("/") + "/" + relative_path
                return local_path

        # 没有匹配的映射，返回原路径
        return storage_path

    @staticmethod
    def _build_record_info(record) -> Dict[str, Any]:
        """构建统一的记录展示结构"""
        return {
            "id": record.id,
            "src": record.src,
            "title": getattr(record, "title", "")
        }

    def _create_transfer_chain(self) -> Optional[Any]:
        """创建整理链实例（统一处理导入/初始化异常）"""
        transfer_chain_cls = TransferChain
        # 模块级导入失败时，运行时再尝试一次（避免因加载时序导致功能永久不可用）
        if transfer_chain_cls is None:
            try:
                from app.chain.transfer import TransferChain as transfer_chain_cls
            except ImportError:
                logger.error("TransferCleaner: TransferChain 模块未加载")
                return None
        try:
            return transfer_chain_cls()
        except (TypeError, RuntimeError, ValueError):
            logger.exception("TransferCleaner: 初始化 TransferChain 失败")
            return None

    @staticmethod
    def _trigger_retransfer(transfer_chain, src_path: str) -> bool:
        """触发单个文件重新整理"""
        if not transfer_chain:
            return False
        try:
            transfer_chain.process(Path(src_path))
            return True
        except (OSError, RuntimeError, ValueError):
            logger.exception(f"TransferCleaner: 重新整理失败 {src_path}")
            return False

    def _run_cleanup_task(self):
        """
        运行清理任务：扫描数据库中的转移记录，检查源文件是否存在，
        如果不存在则删除对应的记录
        """
        logger.info("TransferCleaner: 开始运行清理任务...")

        # 解析清理目录
        clean_dirs = [d.strip() for d in self._clean_dirs.split("\n") if d.strip()]
        if not clean_dirs:
            # 如果没有配置清理目录，使用监控目录
            clean_dirs = [d.strip() for d in self._monitor_dirs.split("\n") if d.strip()]

        if not clean_dirs:
            logger.warning("TransferCleaner: 未配置清理目录，跳过清理任务")
            self.systemmessage.put("未配置清理目录，请先配置监控目录或清理目录", title="转移记录清理")
            return

        # 将本地目录转换为存储路径前缀（用于数据库查询）
        storage_prefixes = []
        for local_dir in clean_dirs:
            storage_path = self._convert_path_to_storage(local_dir)
            storage_prefixes.append(storage_path)
            logger.info(f"TransferCleaner: 清理目录映射 {local_dir} -> {storage_path}")

        # 统计
        total_checked = 0
        total_deleted = 0
        deleted_records = []
        pending_delete_records = []

        try:
            with SessionFactory() as db:
                # 遍历每个存储路径前缀
                for storage_prefix in storage_prefixes:
                    logger.info(f"TransferCleaner: 扫描存储路径前缀 {storage_prefix}")

                    # 流式查询匹配记录
                    query = db.query(TransferHistory).filter(
                        TransferHistory.src.like(f"{storage_prefix}%")
                    ).order_by(desc(TransferHistory.id))

                    for record in query.yield_per(200):
                        if self._stop_event.is_set():
                            logger.info("TransferCleaner: 清理任务收到停止信号，退出")
                            return
                        total_checked += 1

                        # 将存储路径转换为本地路径
                        local_path = self._convert_storage_to_local(record.src)

                        # 检查文件是否存在
                        if os.path.exists(local_path):
                            continue

                        pending_delete_records.append(self._build_record_info(record))

                        # 防止删除过多
                        if len(pending_delete_records) >= 1000:
                            logger.warning("TransferCleaner: 达到单次清理上限 1000 条")
                            break

                    if len(pending_delete_records) >= 1000:
                        break

        except (SQLAlchemyError, OSError) as e:
            logger.exception("TransferCleaner: 清理任务异常")
            self.systemmessage.put(f"清理任务异常: {str(e)}", title="转移记录清理")
            return

        # 在读取游标关闭后执行删除，避免 DB 锁竞争
        if self._dry_run:
            total_deleted = len(pending_delete_records)
            deleted_records = pending_delete_records
            for item in pending_delete_records:
                logger.info(
                    f"[DryRun] TransferCleaner: 将删除记录 "
                    f"ID={item['id']}, src={item['src']}"
                )
        else:
            for item in pending_delete_records:
                try:
                    self._transferhistory.delete(item["id"])
                    total_deleted += 1
                    deleted_records.append(item)
                    logger.info(
                        f"TransferCleaner: 已删除记录 "
                        f"ID={item['id']}, src={item['src']}"
                    )
                except SQLAlchemyError:
                    logger.exception(f"TransferCleaner: 删除记录失败 ID={item['id']}")

        # 发送通知
        dry_run_tag = "[模拟] " if self._dry_run else ""
        summary = f"{dry_run_tag}清理任务完成\n"
        summary += f"扫描记录: {total_checked} 条\n"
        summary += f"{'将删除' if self._dry_run else '已删除'}: {total_deleted} 条\n"

        if deleted_records and len(deleted_records) <= 10:
            summary += "\n详情:\n"
            for r in deleted_records[:10]:
                title = r.get('title', '')
                if title:
                    summary += f"- {title}\n"
                else:
                    summary += f"- ID:{r['id']}\n"

        logger.info(f"TransferCleaner: {summary}")

        if self._notify:
            self.post_message(
                mtype=NotificationType.SiteMessage,
                title=f"【转移记录清理】{dry_run_tag}",
                text=summary
            )

        # 如果开启了清理失败记录，继续执行
        if self._clean_failed:
            self._run_clean_failed_task()

    def _run_clean_failed_task(self):
        """
        清理/重试失败记录：
        - 源文件不存在（说明已上传成功）：删除失败记录
        - 源文件仍存在（说明确实失败了）：删除记录并重新整理
        """
        logger.info("TransferCleaner: 开始处理失败记录...")

        total_checked = 0
        deleted_count = 0
        retry_count = 0
        pending_delete_ids = []
        pending_retry_items = []

        try:
            with SessionFactory() as db:
                # 流式查询所有失败的记录
                query = db.query(TransferHistory).filter(
                    TransferHistory.status == False
                ).order_by(desc(TransferHistory.id)).limit(500)

                for record in query.yield_per(100):
                    if self._stop_event.is_set():
                        logger.info("TransferCleaner: 失败记录处理收到停止信号，退出")
                        return
                    total_checked += 1

                    # 将存储路径转换为本地路径
                    local_path = self._convert_storage_to_local(record.src)

                    if os.path.exists(local_path):
                        # 源文件存在，说明确实失败了，需要重试
                        if self._dry_run:
                            logger.info(
                                f"[DryRun] TransferCleaner: 将重试整理 "
                                f"ID={record.id}, src={record.src}"
                            )
                            retry_count += 1
                        else:
                            pending_retry_items.append({
                                "id": record.id,
                                "src": record.src,
                                "local_path": local_path,
                            })
                    else:
                        # 源文件不存在，说明实际已上传成功，删除错误记录
                        if self._dry_run:
                            logger.info(
                                f"[DryRun] TransferCleaner: 将删除假失败记录 "
                                f"ID={record.id}, src={record.src}"
                            )
                            deleted_count += 1
                        else:
                            pending_delete_ids.append(record.id)

                    current_count = (
                        deleted_count + retry_count
                        if self._dry_run
                        else len(pending_delete_ids) + len(pending_retry_items)
                    )
                    if current_count >= 100:
                        logger.warning("TransferCleaner: 达到单次处理上限 100 条")
                        break

        except (SQLAlchemyError, OSError) as e:
            logger.exception("TransferCleaner: 处理失败记录异常")
            return

        # 在读取游标关闭后执行写入操作，避免 DB 锁竞争
        if not self._dry_run:
            transfer_chain = self._create_transfer_chain()

            # 删除假失败记录
            for record_id in pending_delete_ids:
                try:
                    self._transferhistory.delete(record_id)
                    deleted_count += 1
                    logger.info(f"TransferCleaner: 已删除假失败记录 ID={record_id}")
                except SQLAlchemyError:
                    logger.exception(f"TransferCleaner: 删除假失败记录异常 ID={record_id}")

            # 先整理后删除（确保整理成功才删记录，避免记录丢失）
            for item in pending_retry_items:
                if self._trigger_retransfer(transfer_chain, item["local_path"]):
                    try:
                        self._transferhistory.delete(item["id"])
                        retry_count += 1
                        logger.info(f"TransferCleaner: 已触发重新整理并删除记录 ID={item['id']}")
                    except SQLAlchemyError:
                        logger.exception(f"TransferCleaner: 删除记录异常 ID={item['id']}")
                else:
                    logger.warning(f"TransferCleaner: 重新整理失败，保留记录 ID={item['id']}")

        if deleted_count > 0 or retry_count > 0:
            dry_run_tag = "[模拟] " if self._dry_run else ""
            summary = f"{dry_run_tag}处理失败记录完成\n"
            summary += f"检查失败记录: {total_checked} 条\n"
            if deleted_count > 0:
                summary += f"{'将删除' if self._dry_run else '已删除'}假失败记录: {deleted_count} 条\n"
            if retry_count > 0:
                summary += f"{'将重试' if self._dry_run else '已重试'}整理: {retry_count} 条\n"

            logger.info(f"TransferCleaner: {summary}")

            if self._notify:
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title=f"【转移记录清理】{dry_run_tag}失败记录处理",
                    text=summary
                )

    def _run_retransfer_task(self):
        """
        运行重新整理任务：扫描已有转移记录但源文件仍存在的情况，
        说明文件没有成功上传，需要重新整理
        """
        logger.info("TransferCleaner: 开始运行重新整理检测任务...")

        # 解析检测目录
        retransfer_dirs = [d.strip() for d in self._retransfer_dirs.split("\n") if d.strip()]
        if not retransfer_dirs:
            # 默认使用 /media/待上传
            retransfer_dirs = ["/media/待上传"]

        # 统计
        total_checked = 0
        need_retransfer = []

        try:
            with SessionFactory() as db:
                for check_dir in retransfer_dirs:
                    logger.info(f"TransferCleaner: 检测目录 {check_dir}")

                    # 流式查询源路径在该目录下的记录
                    query = db.query(TransferHistory).filter(
                        TransferHistory.src.like(f"{check_dir}%")
                    ).order_by(desc(TransferHistory.id)).limit(500)

                    for record in query.yield_per(100):
                        if self._stop_event.is_set():
                            logger.info("TransferCleaner: 重新整理检测收到停止信号，退出")
                            return
                        total_checked += 1
                        src_path = record.src

                        # 检查源文件是否仍然存在
                        if os.path.exists(src_path):
                            # 源文件仍存在，说明可能没有成功上传
                            need_retransfer.append({
                                "id": record.id,
                                "src": src_path,
                                "dest": record.dest,
                                "title": getattr(record, 'title', ''),
                            })
                            logger.info(
                                f"TransferCleaner: 发现未上传文件 "
                                f"ID={record.id}, src={src_path}"
                            )

                        if len(need_retransfer) >= 100:
                            logger.warning("TransferCleaner: 达到单次检测上限 100 条")
                            break

                    if len(need_retransfer) >= 100:
                        break

        except (SQLAlchemyError, OSError) as e:
            logger.exception("TransferCleaner: 重新整理检测任务异常")
            self.systemmessage.put(f"重新整理检测异常: {str(e)}", title="转移记录清理")
            return

        # 处理需要重新整理的文件
        retransfer_count = 0
        if need_retransfer and not self._dry_run:
            transfer_chain = self._create_transfer_chain()

            for item in need_retransfer:
                if self._stop_event.is_set():
                    break
                src_path = item["src"]

                # 先整理后删除（确保整理成功才删记录）
                if self._trigger_retransfer(transfer_chain, src_path):
                    try:
                        self._transferhistory.delete(item["id"])
                        retransfer_count += 1
                        logger.info(f"TransferCleaner: 已触发重新整理并删除旧记录 ID={item['id']}")
                    except SQLAlchemyError:
                        logger.exception(f"TransferCleaner: 删除旧记录失败 ID={item['id']}")
                else:
                    logger.warning(f"TransferCleaner: 重新整理失败，保留记录 ID={item['id']}")

        # 发送通知
        dry_run_tag = "[模拟] " if self._dry_run else ""
        summary = f"{dry_run_tag}重新整理检测完成\n"
        summary += f"扫描记录: {total_checked} 条\n"
        summary += f"发现未上传: {len(need_retransfer)} 条\n"
        if not self._dry_run:
            summary += f"已重新整理: {retransfer_count} 条\n"

        if need_retransfer and len(need_retransfer) <= 10:
            summary += "\n详情:\n"
            for r in need_retransfer[:10]:
                title = r.get('title', '')
                if title:
                    summary += f"- {title}\n"
                else:
                    src = r.get('src', '')
                    summary += f"- {Path(src).name}\n"

        logger.info(f"TransferCleaner: {summary}")

        if self._notify:
            self.post_message(
                mtype=NotificationType.SiteMessage,
                title=f"【转移记录清理】{dry_run_tag}重新整理",
                text=summary
            )

    def _convert_path_to_storage(self, local_path: str) -> str:
        """
        将本地路径转换为存储路径（用于匹配 TransferHistory.src）

        :param local_path: 本地文件路径
        :return: 转换后的存储路径，如果没有匹配的映射则返回原路径
        """
        for local_prefix, storage_prefix in self._path_mappings_dict.items():
            if local_path.startswith(local_prefix):
                # 计算相对路径
                relative_path = local_path[len(local_prefix):].lstrip("/")
                # 构建存储路径
                storage_path = storage_prefix.rstrip("/") + "/" + relative_path
                logger.debug(f"TransferCleaner: 路径转换 {local_path} -> {storage_path}")
                return storage_path

        # 没有匹配的映射，返回原路径
        return local_path

    def _start_monitoring(self):
        """启动目录监控"""
        monitor_dirs = [d.strip() for d in self._monitor_dirs.split("\n") if d.strip()]

        if not monitor_dirs:
            logger.warning("TransferCleaner: 未配置监控目录")
            return

        logger.info(f"TransferCleaner: 监控目录列表 {monitor_dirs}")

        for mon_path in monitor_dirs:
            if not os.path.isdir(mon_path):
                logger.warning(f"TransferCleaner: 监控目录不存在 {mon_path}")
                continue

            try:
                # 使用兼容模式（轮询），适用于网络挂载目录
                observer = PollingObserver(timeout=10)

                self._observers.append(observer)
                observer.schedule(
                    FileMonitorHandler(mon_path, self),
                    mon_path,
                    recursive=True
                )
                observer.daemon = True
                observer.start()

                logger.info(f"TransferCleaner: {mon_path} 目录监控启动 [兼容模式], observer.is_alive={observer.is_alive()}")

            except (OSError, RuntimeError) as e:
                logger.exception(f"TransferCleaner: 启动目录监控失败 {mon_path}")
                self.systemmessage.put(
                    f"启动目录监控失败：{mon_path}\n{str(e)}",
                    title="转移记录清理"
                )

    def _start_delay_worker(self):
        """启动延迟删除工作线程"""
        self._delay_thread = threading.Thread(
            target=self._delay_worker_loop,
            daemon=True,
            name="TransferCleaner-DelayWorker"
        )
        self._delay_thread.start()
        logger.info(f"TransferCleaner: 延迟删除线程启动，延迟 {self._delay_seconds} 秒")

    def _delay_worker_loop(self):
        """延迟删除工作线程主循环"""
        while not self._stop_event.is_set():
            try:
                # 从队列获取事件，超时1秒
                event_data = self._delay_queue.get(timeout=1)
            except Empty:
                continue

            event_type = event_data["event_type"]
            src_path = event_data["src_path"]
            dest_path = event_data.get("dest_path")
            event_time = event_data["event_time"]

            # 计算需要等待的时间
            elapsed = time.time() - event_time
            wait_time = self._delay_seconds - elapsed

            if wait_time > 0:
                # 等待剩余时间，但要检查停止信号
                if self._stop_event.wait(wait_time):
                    break

            # 检查文件是否仍然不存在（确认删除）
            if os.path.exists(src_path):
                logger.info(
                    f"TransferCleaner: 延迟检查发现文件已恢复，跳过 {src_path}"
                )
                continue

            # 执行删除历史记录
            self._process_delete(event_type, src_path, dest_path)

    def handle_file_event(self, event_type: str, src_path: str, dest_path: str = None):
        """
        处理文件事件

        :param event_type: 事件类型 (deleted/moved)
        :param src_path: 源路径（用于匹配历史记录）
        :param dest_path: 目标路径（仅移动事件有）
        """
        try:
            file_path = Path(src_path)

            # 过滤临时文件
            if file_path.suffix.lower() in self._temp_suffixes:
                return

            # 检查是否在不删除目录中
            if self._is_in_exclude_dirs(src_path):
                logger.debug(f"TransferCleaner: 路径在不删除目录中，跳过 {src_path}")
                return

            # 过滤排除关键词
            if self._should_exclude(src_path):
                logger.debug(f"TransferCleaner: 路径命中排除关键词，跳过 {src_path}")
                return

            # 事件去重
            if self._is_duplicate_event(src_path):
                logger.info(f"TransferCleaner: 重复事件，跳过 {src_path}")
                return

            logger.info(f"TransferCleaner: 检测到文件{event_type}事件 - {src_path}")

            if self._delay_enabled:
                # 加入延迟队列
                self._delay_queue.put({
                    "event_type": event_type,
                    "src_path": src_path,
                    "dest_path": dest_path,
                    "event_time": time.time()
                })
                logger.debug(f"TransferCleaner: 事件加入延迟队列，{self._delay_seconds}秒后处理")
            else:
                # 立即处理
                self._process_delete(event_type, src_path, dest_path)

        except (OSError, TypeError, ValueError) as e:
            logger.exception(f"TransferCleaner: 处理事件异常 {src_path}")

    def _process_delete(self, event_type: str, src_path: str, dest_path: str = None):
        """实际执行删除历史记录"""
        # 规范化路径
        normalized_path = self._normalize_path(src_path)

        # 应用路径映射转换
        storage_path = self._convert_path_to_storage(normalized_path)

        # 删除历史记录（先尝试存储路径，再尝试原路径）
        result = self._delete_history_by_src(storage_path, event_type)

        # 如果存储路径没有匹配到，且存储路径与原路径不同，再尝试原路径
        if result["deleted_count"] == 0 and storage_path != normalized_path:
            logger.debug(f"TransferCleaner: 存储路径未匹配，尝试原路径 {normalized_path}")
            result = self._delete_history_by_src(normalized_path, event_type)

        # 发送通知
        if result["deleted_count"] > 0 and self._notify:
            self._send_notification(event_type, storage_path, dest_path, result)

    def _normalize_path(self, path: str) -> str:
        """路径规范化"""
        # 转换为绝对路径
        normalized = os.path.abspath(path)
        # 统一分隔符
        normalized = normalized.replace("\\", "/")
        # 去除尾部斜杠
        normalized = normalized.rstrip("/")
        return normalized

    def _is_in_exclude_dirs(self, path: str) -> bool:
        """检查路径是否在不删除目录中"""
        if not self._exclude_dirs_list:
            return False

        for exclude_dir in self._exclude_dirs_list:
            if path.startswith(exclude_dir):
                return True
        return False

    def _should_exclude(self, path: str) -> bool:
        """检查路径是否应该排除（使用预编译的关键词列表）"""
        if not self._exclude_keywords_list:
            return False

        for keyword in self._exclude_keywords_list:
            if keyword in path:
                return True
        return False

    def _cleanup_event_cache(self, current_time: float):
        """按固定周期清理过期缓存，避免每次事件都全量遍历"""
        if current_time - self._last_event_cache_cleanup < self._event_cache_cleanup_interval:
            return
        expired_keys = [
            k for k, v in self._event_cache.items()
            if current_time - v > self._dedupe_ttl
        ]
        for k in expired_keys:
            self._event_cache.pop(k, None)
        self._last_event_cache_cleanup = current_time

    def _is_duplicate_event(self, path: str) -> bool:
        """检查是否为重复事件"""
        current_time = time.time()

        with self._event_cache_lock:
            self._cleanup_event_cache(current_time)

            last_seen = self._event_cache.get(path)
            if last_seen is not None and current_time - last_seen <= self._dedupe_ttl:
                return True

            # 记录事件
            self._event_cache[path] = current_time
            return False

    def _delete_history_by_src(self, src_path: str, reason: str) -> dict:
        """
        根据源路径删除转移历史记录

        :return: {"deleted_count": int, "deleted_ids": list, "dry_run": bool}
        """
        result = {
            "deleted_count": 0,
            "deleted_ids": [],
            "dry_run": self._dry_run
        }

        # 删除上限保护，防止异常数据导致长循环
        max_delete_count = 100

        try:
            # 循环删除直到没有匹配记录（处理重复记录）
            while result["deleted_count"] < max_delete_count:
                history = self._transferhistory.get_by_src(src_path)
                if not history:
                    break

                result["deleted_ids"].append(history.id)
                result["deleted_count"] += 1

                if self._dry_run:
                    logger.info(
                        f"[DryRun] TransferCleaner: 将删除历史记录 "
                        f"ID={history.id}, src={src_path}"
                    )
                    break  # Dry Run 模式只检查一次
                else:
                    self._transferhistory.delete(history.id)
                    logger.info(
                        f"TransferCleaner: 已删除历史记录 "
                        f"ID={history.id}, src={src_path}, reason={reason}"
                    )

            if result["deleted_count"] >= max_delete_count:
                logger.warning(
                    f"TransferCleaner: 达到删除上限 {max_delete_count}，"
                    f"src={src_path} 可能存在异常数据"
                )

        except SQLAlchemyError:
            logger.exception(f"TransferCleaner: 删除历史记录异常 {src_path}")

        return result

    def _send_notification(self, event_type: str, src_path: str,
                          dest_path: str, result: dict):
        """发送通知"""
        dry_run_tag = "[模拟] " if result["dry_run"] else ""

        # 提取文件名并加入通知缓冲区
        file_name = Path(src_path).name
        self._add_to_notify_buffer(file_name, result["dry_run"])

    def _add_to_notify_buffer(self, file_name: str, dry_run: bool):
        """将文件名加入通知缓冲区，延迟聚合发送"""
        with self._notify_buffer_lock:
            self._notify_buffer.append(file_name)

            # 取消现有定时器
            if self._notify_timer:
                self._notify_timer.cancel()

            # 设置新的定时器
            self._notify_timer = threading.Timer(
                self._notify_delay,
                self._flush_notify_buffer,
                args=[dry_run]
            )
            self._notify_timer.daemon = True
            self._notify_timer.start()

    def _flush_notify_buffer(self, dry_run: bool = False):
        """发送聚合通知"""
        with self._notify_buffer_lock:
            if not self._notify_buffer:
                return

            files = self._notify_buffer.copy()
            self._notify_buffer.clear()

        dry_run_tag = "[模拟] " if dry_run else ""
        count = len(files)

        title = f"【转移记录清理】{dry_run_tag}已删除 {count} 条记录"

        # 按剧集名分组
        if count <= 5:
            text = "\n".join(files)
        else:
            # 超过5个文件，只显示前5个 + 省略提示
            text = "\n".join(files[:5]) + f"\n... 等共 {count} 个文件"

        self.post_message(
            mtype=NotificationType.SiteMessage,
            title=title,
            text=text
        )

    def get_state(self) -> bool:
        """获取插件状态"""
        return self._enabled

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册定时任务
        """
        if not self._enabled:
            return []

        cron_exp = (self._retransfer_cron or "").strip()
        if not cron_exp:
            return []

        try:
            trigger = CronTrigger.from_crontab(cron_exp)
        except ValueError as e:
            logger.error(f"TransferCleaner: 无效的定时任务表达式 `{cron_exp}`: {e}")
            return []

        return [{
            "id": "TransferCleanerScheduled",
            "name": "检测未上传文件/清理假失败记录",
            "trigger": trigger,
            "func": self._run_scheduled_task,
            "kwargs": {}
        }]

    def _run_scheduled_task(self):
        """
        定时任务：执行检测未上传和清理假失败
        """
        self._run_retransfer_task()
        if self._clean_failed:
            self._run_clean_failed_task()

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """配置页面"""
        return [
            {
                "component": "VForm",
                "content": [
                    # 插件说明
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "title": "转移记录清理插件",
                                            "text": "监控本地目录，当文件被移动或删除时，自动删除对应的转移历史记录。支持路径映射，适用于115网盘等场景。",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    # 第一行：开关
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
                                            "model": "notify",
                                            "label": "发送通知",
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
                                            "model": "dry_run",
                                            "label": "模拟运行",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    # 立即运行
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
                                            "model": "run_once",
                                            "label": "立即运行一次",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "density": "compact",
                                            "text": "开启后保存配置，将立即扫描清理目录下的转移记录，删除源文件已不存在的记录。",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    # 重新整理检测
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
                                            "model": "retransfer_once",
                                            "label": "检测未上传文件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "warning",
                                            "variant": "tonal",
                                            "density": "compact",
                                            "text": "检测源文件仍存在但有转移记录的情况，删除记录并重新整理。",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    # 清理假失败记录
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
                                            "model": "clean_failed",
                                            "label": "清理假失败记录",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "density": "compact",
                                            "text": "清理失败记录中源文件已不存在的（实际已上传成功）。",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    # 定时执行周期（两个功能共用）
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
                                            "model": "retransfer_cron",
                                            "label": "定时执行周期",
                                            "placeholder": "0 */6 * * *",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "density": "compact",
                                            "text": "定时执行上方两个功能（检测未上传、清理假失败），留空则不定时执行。",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    # 第二行：延迟删除
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
                                            "model": "delay_enabled",
                                            "label": "启用延迟删除",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "delay_seconds",
                                            "label": "延迟时间(秒)",
                                            "type": "number",
                                            "placeholder": "10",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    # 第三行：监控目录
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
                                            "model": "monitor_dirs",
                                            "label": "监控目录（仅本地目录支持实时监控，网盘挂载目录请用定时任务）",
                                            "rows": 4,
                                            "placeholder": "/media/待上传",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    # 第四行：路径映射
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
                                            "model": "path_mappings",
                                            "label": "路径映射",
                                            "rows": 4,
                                            "placeholder": "/media/115/转存:u115:/115/转存\n/media/待上传:u115:/待上传",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    # 不删除目录
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
                                            "model": "exclude_dirs",
                                            "label": "不删除目录（命中路径前缀将跳过，每行一个）",
                                            "rows": 3,
                                            "placeholder": "/media/待上传/保留目录",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    # 排除关键词
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
                                            "model": "exclude_keywords",
                                            "label": "排除关键词（路径中包含关键词则跳过，每行一个）",
                                            "rows": 3,
                                            "placeholder": "sample\n@Recycle",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    # 清理目录（立即运行使用）
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
                                            "model": "clean_dirs",
                                            "label": "定时清理目录（网盘挂载目录放这里，通过定时任务清理）",
                                            "rows": 3,
                                            "placeholder": "/media/115/转存",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    # 重新整理目录
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
                                            "model": "retransfer_dirs",
                                            "label": "重新整理检测目录（留空默认 /media/待上传）",
                                            "rows": 2,
                                            "placeholder": "/media/待上传",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": True,
            "dry_run": True,
            "delay_enabled": False,
            "delay_seconds": 10,
            "monitor_dirs": "",
            "path_mappings": "",
            "clean_dirs": "",
            "exclude_dirs": "",
            "exclude_keywords": "",
            "run_once": False,
            "retransfer_once": True,
            "retransfer_dirs": "",
            "retransfer_cron": "",
            "clean_failed": True,
        }

    def get_page(self) -> List[dict]:
        """插件页面"""
        pass

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """远程命令"""
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        """API接口"""
        return [
            {
                "path": "/status",
                "endpoint": self._api_status,
                "methods": ["GET"],
                "summary": "获取插件状态",
            },
            {
                "path": "/run_cleanup",
                "endpoint": self._api_run_cleanup,
                "methods": ["POST"],
                "summary": "手动触发清理任务",
            },
            {
                "path": "/run_retransfer",
                "endpoint": self._api_run_retransfer,
                "methods": ["POST"],
                "summary": "手动触发重新整理检测",
            },
        ]

    def _api_status(self, *_args, **_kwargs) -> Dict[str, Any]:
        """返回插件状态"""
        if self._event_cache_lock:
            with self._event_cache_lock:
                event_cache_size = len(self._event_cache or {})
        else:
            event_cache_size = len(self._event_cache or {})

        if self._notify_buffer_lock:
            with self._notify_buffer_lock:
                notify_buffer_size = len(self._notify_buffer or [])
        else:
            notify_buffer_size = len(self._notify_buffer or [])

        return {
            "success": True,
            "data": {
                "enabled": self._enabled,
                "dry_run": self._dry_run,
                "delay_enabled": self._delay_enabled,
                "delay_seconds": self._delay_seconds,
                "observers": len(self._observers or []),
                "event_cache_size": event_cache_size,
                "notify_buffer_size": notify_buffer_size,
                "clean_failed": self._clean_failed,
                "retransfer_cron": self._retransfer_cron,
            }
        }

    def _api_run_cleanup(self, *_args, **_kwargs) -> Dict[str, Any]:
        """手动触发清理任务"""
        try:
            threading.Thread(
                target=self._run_cleanup_task,
                daemon=True,
                name="TransferCleaner-ApiCleanup"
            ).start()
        except RuntimeError as e:
            logger.exception("TransferCleaner: 启动清理任务失败")
            return {"success": False, "message": f"启动清理任务失败: {str(e)}"}
        return {"success": True, "message": "清理任务已启动"}

    def _api_run_retransfer(self, *_args, **_kwargs) -> Dict[str, Any]:
        """手动触发重新整理检测"""
        try:
            threading.Thread(
                target=self._run_retransfer_task,
                daemon=True,
                name="TransferCleaner-ApiRetransfer"
            ).start()
        except RuntimeError as e:
            logger.exception("TransferCleaner: 启动重新整理任务失败")
            return {"success": False, "message": f"启动重新整理任务失败: {str(e)}"}
        return {"success": True, "message": "重新整理检测任务已启动"}

    def stop_service(self):
        """停止服务"""
        # 停止延迟删除线程
        if self._stop_event:
            self._stop_event.set()
        if self._delay_thread and self._delay_thread.is_alive():
            self._delay_thread.join(timeout=5)
            logger.info("TransferCleaner: 延迟删除线程已停止")

        # 停止通知聚合定时器
        if self._notify_timer:
            try:
                self._notify_timer.cancel()
                if self._notify_timer.is_alive():
                    self._notify_timer.join(timeout=2)
            except RuntimeError:
                logger.warning("TransferCleaner: 通知聚合定时器停止时状态异常")
            self._notify_timer = None
            logger.info("TransferCleaner: 通知聚合定时器已停止")

        # 清空通知缓冲区
        if self._notify_buffer_lock:
            with self._notify_buffer_lock:
                if self._notify_buffer is not None:
                    self._notify_buffer.clear()

        # 停止目录监控
        if self._observers:
            for observer in self._observers:
                try:
                    observer.stop()
                    observer.join(timeout=5)
                except (RuntimeError, OSError):
                    logger.exception("TransferCleaner: 停止监控异常")
            self._observers = []
            logger.info("TransferCleaner: 目录监控已停止")
