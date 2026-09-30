"""THP 投稿插件的纯逻辑层（不依赖 AstrBot，可独立测试）。

模块划分：

* :mod:`~astrbot_plugin_thp_post.services.settings` —— 配置解析与校验；
* :mod:`~astrbot_plugin_thp_post.services.parser` —— 消息段与命令边界解析；
* :mod:`~astrbot_plugin_thp_post.services.identity` —— 事件身份归一化；
* :mod:`~astrbot_plugin_thp_post.services.backend_client` —— HTTP 调用与结果分类；
* :mod:`~astrbot_plugin_thp_post.services.outbox` —— SQLite 持久队列；
* :mod:`~astrbot_plugin_thp_post.services.delivery` —— 调度、退避、崩溃恢复、回执；
* :mod:`~astrbot_plugin_thp_post.services.receipts` —— 群回执文案。

这里不导入任何子模块，避免 ``main.py`` 的相对导入触发连锁导入。
"""
