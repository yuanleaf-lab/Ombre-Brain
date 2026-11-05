# Memory 权限基础与扩展只读能力验收报告

日期：2026-11-04。本轮完成代码及独立测试验收，未启用新的运行配置，
未重启既有服务，未切换正式 Vault，未开放写入权限或公网连接。

## 现有成果保护

- 开工时 11 个已修改文件、17 个未跟踪文件，共 28 个。
- 原文件副本、SHA-256 清单和已有 Git 差异已保存于工作区独立验收目录
  `<LOCAL_ACCEPTANCE_DIRECTORY>`，未执行 reset/checkout/clean、commit 或 push。
- 原 28 个文件全部保留；其中 memory_stdio.py、memory_http.py、
  tools/breath/surface.py 在既有实现上作了本轮增量修改，其余 25 个逐字节一致。
- 原生 server.py、server_app.py、EmbeddingEngine、Cyberboss 配置未作本轮修改。

## 权限矩阵

| 工具 | 固定允许操作 | 允许记忆范围 | 默认 |
| --- | --- | --- | --- |
| memory_bootstrap | 重要记忆浮现 | 活跃 dynamic/permanent | 开放 |
| memory_search | 关键词及语义检索 | 活跃 dynamic/permanent | 开放 |
| memory_read | 完整 ID 读取 | 活跃 dynamic/permanent | 开放 |
| memory_catalog | 目录元数据读取 | 活跃 dynamic/permanent | 需逐项启用 |
| memory_status | 白名单聚合统计 | 活跃 dynamic/permanent | 需逐项启用 |
| memory_feel_search | Feel 检索 | 活跃 feel | 需逐项启用 |
| memory_plan_list | 读取活跃计划 | type=plan 且 status=active | 需逐项启用 |
| memory_self_read | 正式 I 及明确标识的候选读取 | i、合法 self_candidate | 需逐项启用 |

发现和执行共用冻结的授权对象，执行时再次检查。存储视图同时校验逻辑类型、
系统标记和物理目录，向量候选在排序前限制。空授权不开放工具。
不认识、重复或非法权限字段直接失败。没有通用 trace 转发或任意字段更新入口。

本版 write_enabled 仅接受 false，true 会在打开 Vault 前被拒绝。
没有审核通过工具或可授予权限的请求参数；approved、permissions、delete、
promote 等不属于对应读取 schema 的字段均被拒绝。
工具描述及 readOnlyHint 只是声明，实际权限由注册、调用检查和读取视图执行。

当前是单个已认证服务实例的固定管理员授权，不宣称实现了多账号 OAuth/RBAC。
原有启动配置不新增权限项时仍严格只有三个工具。本轮没有修改这些启动配置。

## 完整调用链与副作用清单

| 路径 | 复用方式 | 副作用隔离与异常行为 |
| --- | --- | --- |
| bootstrap | 原生 breath dispatcher → surface_default | 原有只读隔离；不衰减、不强化、不记录事件 |
| search | 原生 surface_search → SQLite/BucketManager | 只读向量连接；候选前置过滤；不构建 BM25 |
| exact read | 原有完整 ID 快照路径 | 不存在/越权为错误，不进行语义替代 |
| catalog | 原生 catalog 分支及渲染 | 仅普通范围；只读列举异常明确抛出 |
| status | 原生 BucketManager 读取结果的聚合投影 | 不调用会启动衰减和列出私人元数据的完整 pulse |
| Feel | 原生 Feel 通道及渲染 | 只读语义异常拒绝回退；显式关闭 embedding 才允许标明的字面模式 |
| Plan | 原生 surface_plans | 只返回 active；异常不会伪装成空列表或成功结果 |
| I | 原生 _read_i | 跳过 i_core 的 mark_op/ensure_started；不写见证、不晋升、不取代 |

status 仅返回 read_only、普通记忆分类型数量、embedding enabled/model/dim、
maintenance；不返回正文、标题、ID、主题列表、文件路径、服务 URL 或凭据。
I 保留原生当前/被质疑/已取代/候选分区；limit 是原生各正式分区上限，
不是全部候选的分页上限。超过 token 预算时失败，不静默截断候选。

所有路径继续使用既有 SQLite mode=ro/query_only，不使用 immutable=1。
没有启动 decay、embedding queue、dream、同步、Ollama 子进程、Webhook、
删除请求对账、持久事件或磁盘缓存。允许进程内查询向量 LRU 和读取缓存更新。
stdio 子进程保留既有写入审计拦截；该机制不是操作系统权限沙箱。

Letter（包括伪装成普通类型但带 Letter 标记的记录）、归档/删除记录及错位目录
均不能借普通读取通道返回。测试包含已经到期的锁信，没有调用到期解锁逻辑。

## 情绪主体设计

未改动任何正式数据结构，也未启用 Feel 写入。
本次只读结果把缺乏可信主体字段的 Feel 标为 legacy_unspecified / unspecified，
不根据正文自动判断成用户自述、助手感受或推测事实。

待另行批准的最小扩展方案见 memory-permissions.md：

- 用户自述：emotion_subject=user，emotion_basis=self_report。
- 助手第一人称感受：emotion_subject=assistant，emotion_basis=self_report。
- 上下文推测：明确主体，emotion_basis=inference，保留来源及不确定性。
- 旧记录主体不清：保持 unspecified，不自动回填或升级为事实。

用户情绪记录不能直接套用原生 hold(feel=true) 修改源记忆 digested/model_valence
的副作用，未来需要独立的可信写入分支。本轮没有实现或启用该分支。

## 自动化结果

63 项标准库 unittest 全部通过：原有 43 项，加本轮 20 项。
完整输出位于验收目录 unittest-final-63.log。没有安装 pytest 或其他依赖。

覆盖：

- 原有三个工具、旧配置、认证、默认生命周期的全部只读回归。
- 按权限发现、直接调用拒绝、空授权、未知/重复授权、伪造审核及请求越权。
- 八条真实原生业务读取链；完整文件列表、目录列表、SHA-256 对比。
- SQLite 表结构、向量数据、元数据、journal_mode、integrity_check 对比。
- 写入尝试拦截：即使原生代码吞掉异常，也不能把写入尝试算作通过。
- Feel 供应方/数据库异常、意外禁用引擎、合法无命中、各扩展链读取异常。
- 向量候选提前过滤：80 条更相似但无授权的向量不能挤掉已授权普通记忆。
- 真实 MCP stdio 子进程 initialize、八工具发现与调用、禁止工具调用拒绝。
- HTTP 内存 ASGI 握手、受限七工具配置、业务调用、无/错 Token 拒绝。
- 独立正常 OB 写入进程与 stdio 只读进程共享合成 WAL Vault：写事务持有期间
  八工具读取正常；提交后能读取新 ID；新记录的 1024 维向量和正文哈希正确；
  两进程退出后 integrity_check=ok。只读请求前后应用文件保持不变。

语义单测及并发写入使用确定性测试模型输出，SQLite 解码、过滤、余弦排序、
原生持久化和渲染是真实实现。不能把该结果当成本轮重新验证真实 Ollama。
HTTP 测试没有打开监听端口；未重启、连接或重测实际 Cyberboss 产品会话。
原生默认行为的证据是原有回归、默认 Feel 回退测试、正常 OB 写入兼容测试，
以及原生服务入口和生命周期文件与阶段前副本一致。

## 失败案例与修复

1. 初始受限测试进程不能访问其新建临时目录；使用经自动审批允许的本机测试
   权限重新执行，未改系统 ACL 或原有临时目录。
2. 新测试夹具遗漏 embeddings.updated_at 的必填值，且连接未显式关闭；
   仅修复合成夹具，保留此前失败日志，未修改正式数据库。
3. SDK 的直接调用返回为文本与结构化结果的 tuple；修正测试解包。
   I 原生候选提示和 pinned 排序也按实际业务契约核对，未为断言改业务语义。
4. 并发验收最初在过滤前读取 WAL/SHM 全文件哈希，撞上 Windows 正常共享内存
   范围锁。改为打开前排除协调文件；应用文件仍逐项比对，最终另做数据库完整性检查。
5. 独立复核复现 type=i 同时带候选标记时，被原生渲染同时列为正式认知和候选。
   新回归先失败，再在适配层拒绝冲突；Feel/候选混合标记也拒绝。不修改或修复原记录。
6. 自动审批曾因账户额度中断一次测试请求，该请求未执行；用户继续后重新审批
   并完成该回归及全套测试，没有绕过审批、升级套餐或购买额度。

独立复审只读检查了阶段增量和相关原生辅助链路，最终无未解决的阻断/重要问题。
MCP SDK 在故意调用未注册工具时会输出“not listed”警告；实际响应均为 isError，
这不是绕过授权，也未执行对应工具。

## 正式环境复核与下一步

正式 Vault 仍为 218 个文件，阶段前后完整清单及内容组合哈希一致：
`<REDACTED_ENVIRONMENT_FINGERPRINT>`。
正式 OB 配置哈希一致；没有修改 Cyberboss 配置或复制正式正文作为测试数据。

等待验收后，再由用户决定是否在独立本地测试启动配置中逐项启用新增工具。
本报告不授权切换正式 Vault、写入、迁移、远程连接、Render 部署或公网发布。
