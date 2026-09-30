# THP 群投稿插件

将指定 QQ 群的 `/投稿 内容` 可靠送入 THP 主持管理端。仅接 NapCat / OneBot v11 的群消息；不调用 LLM，不审核投稿，不控制大屏，不下载头像。

版本 **1.0.0**。开发验证基线为 **AstrBot 4.28.2 / Python 3.12.13**。元数据允许 `>=4.28.2,<5`；该范围内其他版本仍需现场验证。尚未连接真实 QQ 账号或最终云服务器。

## 安装与配置

使用 `dist/astrbot_plugin_thp_post-1.0.0.zip`（大项目根目录），在 AstrBot 插件管理中上传本地 ZIP。包内 `main.py` 直接位于根层，不要再套一层目录。离线传输 ZIP 不等于已包含 Python 依赖。

安装后填写下列配置并保存、重载插件：

| 配置 | 填写内容 |
| --- | --- |
| `backend_url` | 网站源地址，如 `http://服务器IP:18080`；不带 `/host`、`/screen` 或 `/api` |
| `ingest_token` | 后端 `INGEST_TOKEN`，不能使用 HOST_TOKEN / DISPLAY_TOKEN |
| `event_id` | 与后端 `EVENT_ID` 完全一致 |
| `allowed_group_ids` | QQ 群号字符串列表，同时加入后端白名单 |
| `enabled` | 填好上述内容后显式打开，默认 false |

完整中文步骤：[ASTRBOT_SETUP.md](docs/ASTRBOT_SETUP.md)。源码目录下这些文档在大项目 `../docs/`；打包时会装入本插件的 `docs/`。

## 用法与回执

```text
/投稿 今天很开心 🎉

/投稿 第一行
第二行  保留内部空格
```

无需 @ 机器人，正文去首尾空白后 1–500 个 Unicode 码点。只支持文字及 Unicode emoji；QQ 原生表情、引用、图片、文件等会提示重新发送纯文本。`/投稿箱`、普通聊天、私聊、未授权群和机器人自发消息不会上传。

- 入库成功：`投稿成功，编号 #123，已送达主持端。`
- 暂时不可达：落盘后提示暂存，后台有限重试，成功再补发编号。
- 明确拒绝或最终未确认：给出对应失败提示，保留诊断记录。

**入库成功不代表已上屏。** 只有主持的选择会改变屏幕；新稿不能抢占画面。

## 持久化与恢复

数据位于 AstrBot 的 `data/plugin_data/astrbot_plugin_thp_post/outbox.db`，具体根目录由 AstrBot 官方路径函数决定（支持 ASTRBOT_ROOT）。升级插件不会覆盖该目录。SQLite WAL、事务幂等、单调度器和进程锁保护发送。

默认重试间隔 2/5/15/30/60 秒，带少量抖动；默认最多 20 次（包含首发）、10 分钟到期。429 遵守 Retry-After。已失败稿件不自动复活，过期稿件不再上传。

更换后端、活动或移除群时，旧待投稿进入 held；恢复原配置也不会自动释放。令牌错误暂停网络发送；修正配置并重载恢复其余 pending，失败稿件仍保留终态。维护方式见 [PLUGIN_HANDOFF.md](docs/PLUGIN_HANDOFF.md)。

## 开发、测试与打包

在插件目录创建隔离环境，安装 `requirements-dev.txt`。Windows 使用 `.venv/Scripts/python.exe`，Linux 使用 `.venv/bin/python`：

本次基础开发环境的完整依赖版本快照在 `requirements-dev.lock`；可在 Python 3.12 的隔离环境按该文件复现基础测试。生产 bot 只使用 `requirements.txt`。

```bash
python -m pytest -o addopts='' -q --timeout=20
python -m ruff check main.py services tests
```

基础测试无需 AstrBot 或后端。集成测试需要另外安装官方 AstrBot 4.28.2 及本项目后端依赖，缺少时明确 skip；不得把 skip 写成已验证。

在大项目根目录执行：

```bash
python scripts/package_astrbot_plugin.py
```

只打包白名单代码、运行依赖和插件文档，排除配置、令牌、数据库、虚拟环境、缓存及测试夹具。输出 ZIP、SHA256 文件及包内文件清单。实际测试记录见 [PLUGIN_ACCEPTANCE.md](docs/PLUGIN_ACCEPTANCE.md)。

实现依据为 [AstrBot 官方源码 v4.28.2](https://github.com/AstrBotDevs/AstrBot/tree/v4.28.2) 和本项目 `contracts/openapi.json`。未复制参考日报插件代码或资源。
