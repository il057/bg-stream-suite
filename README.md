# SillyTavern 后台流式生成 + 楼层兜底修复（bg-stream-suite）

由两个扩展合并而成的单一 SillyTavern UI 扩展：

- **Part A — 后台流式生成**：锁屏 / 切后台 / 断网不再让生成中断，回复在服务端继续跑，回来接着流式显示或自动恢复；
- **Part B — 楼层兜底修复**：自动修复恢复消息缺失的思维链拆分、MVU 变量、前端代码块渲染。

> **来源声明**：Part A 的代码改编自 [alani-fan-club (Dong Duong)](https://github.com/alani-fan-club) 的
> [SillyTavern-BackgroundStreaming](https://github.com/alani-fan-club/SillyTavern-BackgroundStreaming)，
> 其配套服务端插件同样出自该作者
> （[SillyTavern-BackgroundStreaming-Server](https://github.com/alani-fan-club/SillyTavern-BackgroundStreaming-Server)）。
> 本仓库是其衍生作品，遵循 **AGPL-3.0** 许可证以同协议发布，详见 [LICENSE](./LICENSE)。

## 功能

### Part A：后台流式生成

把流式生成请求劫持转发到服务端 relay 上缓冲：

- 手机锁屏、切后台、断网，生成**照常在服务端进行**，不再被取消——不浪费已付费的 token
- 回到页面时流式接着显示，就像从没离开过
- 页面被系统整个杀掉（iOS 常见）后重新打开，能**收养**还在跑的生成继续写入，或把**已完成**的回复自动插入聊天
- 生成中 UI 完整还原：停止按钮（可点，真正取消服务端生成）、发送键隐藏、楼层计时从生成真正开始时刻计算
- 恢复单飞：同一时刻最多恢复一条回复，绝不并行插入两层

### Part B：楼层兜底修复

恢复路径的消息绕过了 ST 的实时流解析，会留下三类残缺，Part B 自动补齐：

- **思维链**：推理内容没被拆出，原样混在正文里 → 按 ST 推理设置的前后缀拆开
- **MVU 变量**：`<UpdateVariable>` 没被处理，状态栏空白或显示上一层旧状态 → 重放事件让 MVU 处理，失败则按「上一层 + 本层 JSONPatch」重算，并带**陈旧检测**（本层状态与上一层完全一致而补丁明明有改动时判定未生效）
- **前端代码块**：该渲染成 iframe 的代码块没渲染 → 请求酒馆助手重绘并补齐高度

可在设置面板单独关闭 Part B（「楼层兜底修复」开关）。

### 内置修复（相对上游原版）

1. **虚假生成请求不再真的生成**：「查看提示词」类按钮的原理是发起生成、拿到拼好的 prompt 后立刻 abort。直连时 abort 会取消生成，但经过 relay 劫持后会漏过两个窗口导致服务端真的跑完一整条回复。本版堵住了两个窗口：
   - signal 在 fetch 之前就已 aborted → 直接透传给原生 fetch，立即以 AbortError 拒绝
   - abort 落在等 relay `/start` 返回的窗口 → 流启动时检查 `signal.aborted`，命中则立即取消 job
2. **MVU 变量陈旧**（见上）：变量结算只在正文写完的最终态做一次，并轮询等待 MVU 异步写完
3. **iOS PWA 崩溃防护**：
   - iOS 上页面打开后前 8 秒为安静期，不做恢复（躲开聊天渲染 + 状态栏 iframe 初始化的内存高峰），8 秒一到立刻触发恢复
   - 崩溃循环保险丝：每个 job 在 sessionStorage 里记录本会话的自动恢复次数，达到 2 次降级为手动确认，保证页面一定能打开

## 安装

**需要两个部分：服务端插件 + 本 UI 扩展。**

### 1. 服务端插件（必装）

本扩展依赖 [SillyTavern-BackgroundStreaming-Server](https://github.com/alani-fan-club/SillyTavern-BackgroundStreaming-Server) 做实际缓冲。**先装它**：

1. `config.yaml` 中设置 `enableServerPlugins: true`
2. 把该仓库 clone / 解压到 SillyTavern 的 `plugins/` 目录
3. 重启 SillyTavern

### 2. 本扩展

方法一：SillyTavern 扩展管理器 → Install Extension → 粘贴本仓库地址。

方法二：clone 到扩展目录：

```bash
git clone https://github.com/il057/bg-stream-suite \
  data/default-user/extensions/bg-stream-suite
```

### 3. 验证

刷新页面后打开 **扩展 → 后台流式生成 + 楼层兜底修复**，面板显示
`Relay vX.X.X connected` 即为连通。

## 使用

装好后无需任何操作，正常聊天即可：

- 发消息后锁屏 / 切后台 → 生成在服务端继续；回来后流式续上
- 页面被杀后重开 → 已完成的回复自动插入（可在设置中改为手动确认），还在跑的生成被收养续写
- 设置面板可开关：后台流式总开关、恢复功能、自动插入、楼层兜底修复

## 兼容性

- SillyTavern ≥ 1.13.3
- 桌面浏览器 / Android / iOS Safari（含 PWA）
- 不依赖任何 ST 内部模块（唯一集成点是 `globalThis.fetch`），对上游更新有较好的抗性

## 许可证

本项目基于 [AGPL-3.0](./LICENSE) 发布。

- Part A 衍生自 [alani-fan-club/SillyTavern-BackgroundStreaming](https://github.com/alani-fan-club/SillyTavern-BackgroundStreaming)（AGPL-3.0），作者 Dong Duong
- 服务端插件为 [alani-fan-club/SillyTavern-BackgroundStreaming-Server](https://github.com/alani-fan-club/SillyTavern-BackgroundStreaming-Server)，本仓库不包含也不修改它
- 合并、去重及上述「内置修复」为本仓库新增内容
