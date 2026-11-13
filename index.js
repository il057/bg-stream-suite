/**
 * Background Streaming Suite — 后台流式生成 + 楼层兜底修复（合并版）
 *
 * 由两个扩展合并而来，行为保持一致：
 *
 *   Part A — Background Streaming（后台流式）
 *     依赖服务端插件 st-relay（plugins/SillyTavern-BackgroundStreaming-Server）。
 *     把流式生成请求劫持到 relay 上，锁屏 / 切后台 / 断网不再取消生成；
 *     页面被销毁后，回来还能把跑到一半或已完成的回复接回来。
 *
 *   Part B — 楼层兜底修复（原「后台生成兜底修复」st-bg-fix）
 *     补上 recovery / 后台消息绕过 ST 实时流解析留下的三类残缺：
 *     思维链没拆开、MVU 变量缺失导致状态栏空白、前端代码块没渲染成 iframe。
 *
 * ============================================================
 * Part A 的核心机制（abort 区分，整个设计的关键）：
 * ST 把自己的 AbortSignal 传进 fetch，按 Stop 时 abort 它；而挂起的标签页
 * 永远不会触发这个 signal。所以「signal aborted」= 用户想停，「socket 断了」
 * = 页面不在了——只有前者会让 relay 取消生成。
 *
 * v2.0.0 修复：虚假生成请求（卡片里的「查看提示词」等按钮：发一个生成请求
 * 只为拿到拼好的 prompt，随后立刻 abort）原来会漏过两个窗口——
 *   1. signal 在 fetch 调用前就已 aborted：shim 照样把它转发给 relay，
 *      服务端真的开始生成；
 *   2. signal 在等 relay /start 返回期间 abort：此时 abort 监听还没挂上，
 *      job 无人取消，跑完后被 recovery 当成孤儿回复插进聊天。
 * 两者都会表现为「点了查看提示词，真的开始生成回复」。现在两个窗口都堵上了。
 *
 * 与 ST 的唯一集成点是 globalThis.fetch；不 import 任何 ST 内部模块，这也是
 * 它能在上游更新后继续活着的原因。
 *
 * 来源与许可：
 *   Part A 改编自 alani-fan-club (Dong Duong) 的 SillyTavern-BackgroundStreaming
 *   （https://github.com/alani-fan-club/SillyTavern-BackgroundStreaming），
 *   服务端配合插件 st-relay 同样出自该作者
 *   （https://github.com/alani-fan-club/SillyTavern-BackgroundStreaming-Server）。
 *   本合并版遵循其 AGPL-3.0 许可证继续以同协议发布。
 */

/* ================================================================ 共享部分 */

const RELAY_BASE = '/api/plugins/st-relay';
const MODULE_NAME = 'st_background_stream';
const LOG = '[bg-stream]';
const LOG_FIX = '[bg-fix]';

/** 必须与服务端插件 st-relay 的 ALLOWED_TARGETS 保持同步。 */
const RELAY_TARGETS = new Set([
    '/api/backends/chat-completions/generate',
    '/api/backends/text-completions/generate',
    '/api/backends/kobold/generate',
    '/api/backends/koboldhorde/generate',
    '/api/novelai/generate',
]);

const DEFAULT_SETTINGS = {
    enabled: true,
    recover: true,
    autoInsert: true,
    fix: true, // Part B：楼层兜底修复总开关
};

/* ---------------------------------------------------------------- 共享工具 */

function getContextSafe() {
    try {
        return globalThis.SillyTavern?.getContext?.() ?? null;
    } catch {
        return null;
    }
}

function requestHeaders() {
    const ctx = getContextSafe();
    return ctx?.getRequestHeaders?.() ?? { 'Content-Type': 'application/json' };
}

function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function safeJson(text) {
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
}

/** 容忍缺少事件目标的环境（测试、奇怪的 webview）。 */
function listen(target, type, handler, options) {
    target?.addEventListener?.(type, handler, options);
    return () => target?.removeEventListener?.(type, handler);
}

function base64ToBytes(base64) {
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
        bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
}

/**
 * 极简 SSE 帧解析器。故意内联而不复用 ST 的 EventSourceStream，让这个文件
 * 不依赖任何内部模块——而且这条流的两端都是我们自己的，只有 \n\n 分帧。
 */
function parseFrame(frame) {
    let type = 'message';
    let id;
    const dataLines = [];

    for (const line of frame.split('\n')) {
        if (!line || line.startsWith(':')) {
            continue;
        }
        const colon = line.indexOf(':');
        const field = colon === -1 ? line : line.slice(0, colon);
        const value = colon === -1 ? '' : line.slice(colon + 1).replace(/^ /, '');

        if (field === 'event') {
            type = value;
        } else if (field === 'data') {
            dataLines.push(value);
        } else if (field === 'id') {
            id = value;
        }
    }

    return dataLines.length > 0 ? { type, id, data: dataLines.join('\n') } : null;
}

async function* readServerEvents(body) {
    const reader = body.getReader();
    const decoder = new TextDecoder('utf-8');
    let buffer = '';

    while (true) {
        const { done, value } = await reader.read();
        if (done) {
            return;
        }

        buffer += decoder.decode(value, { stream: true });

        let boundary = buffer.indexOf('\n\n');
        while (boundary !== -1) {
            const event = parseFrame(buffer.slice(0, boundary));
            buffer = buffer.slice(boundary + 2);
            if (event) {
                yield event;
            }
            boundary = buffer.indexOf('\n\n');
        }
    }
}

/* =========================================================== Part A：后台流式 */

/** 一轮不断线的失败重连里，尝试多少次后放弃。成功一次就清零。 */
const MAX_RECONNECT_ATTEMPTS = 120;

/** 页面 reload 后收养（adopt）的 job 的重连上限，低于上面的值。 */
const REATTACH_MAX_ATTEMPTS = 60;

/** 页面可见时，寻找待收养 job 的周期。 */
const DISCOVERY_INTERVAL_MS = 20000;

/** 仍在填充中的消息的重绘节流。 */
const RENDER_INTERVAL_MS = 120;

/** relay job 里仍在产出字节的状态。 */
const RUNNING_STATES = new Set(['starting', 'running']);

/** 本页脚本加载完成的时刻。 */
const BOOT_AT = Date.now();

/**
 * iOS（含 PWA）的 WebContent 进程内存余量小：页面刚打开的那几秒，聊天渲染
 * 和状态栏 iframe 全在初始化，这时候再叠一套恢复流程（追加消息、发事件、
 * 重建 iframe）容易把进程顶爆——iOS 直接杀页面，用户看到「重复出现问题」。
 * iOS 上开机先安静几秒（8 秒，覆盖状态栏 iframe 的初始化高峰），时间一到
 * 立刻触发 discovery，不等轮询周期；桌面端内存宽裕，保持即时恢复。
 */
const IS_IOS = /iP(hone|ad|od)/.test(globalThis.navigator?.userAgent ?? '')
    || (globalThis.navigator?.platform === 'MacIntel' && globalThis.navigator?.maxTouchPoints > 1);
const BOOT_QUIET_MS = IS_IOS ? 8000 : 0;

/**
 * 崩溃循环保险丝：自动恢复只有「插入成功并消费」才会让 job 从 relay 上消失，
 * 所以恢复路径里任何一次崩溃都会在下次打开时原样复现——偶发崩溃被放大成
 * 必现崩溃循环。这里按 job 记录本会话（sessionStorage，崩溃重载不丢）的
 * 自动恢复次数，超过上限就降级为手动 toast：页面能正常打开，由用户决定
 * 要不要这条回复。
 */
const RECOVERY_ATTEMPT_LIMIT = 2;

function recoveryAttempts(jobId) {
    try {
        const raw = globalThis.sessionStorage?.getItem(`bg-stream:attempt:${jobId}`);
        return raw ? (Number(raw) || 0) : 0;
    } catch {
        return 0;
    }
}

function noteRecoveryAttempt(jobId) {
    try {
        const count = recoveryAttempts(jobId) + 1;
        globalThis.sessionStorage?.setItem(`bg-stream:attempt:${jobId}`, String(count));
    } catch {
        // sessionStorage 不可用（隐私模式等）就没有保险丝，行为退回原样。
    }
}

/**
 * 写进被恢复消息的 extra 里。这是收养能扛过第二次 reload 的原因：标记随
 * 聊天一起保存，中途回来的页面据此找到自己正在填充的那条消息，接着写，
 * 而不是再追加一条重复的。
 */
const JOB_MARKER = 'st_background_stream_jobId';

function newId() {
    return globalThis.crypto?.randomUUID?.()
        ?? `bg-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

/**
 * 标识本次页面加载。随每个 job 一起发送，用于区分「这个页面要的」和「前任
 * 页面留下的」。
 */
const CLIENT_ID = newId();

/**
 * 所有还在途中的 /start 请求的 meta.requestId。
 *
 * job 在 relay 上先于 /start 返回而注册，它的 id 只有在 /start 返回后才进入
 * activeJobIds——这个窗口里 discovery 扫到它会误收养一个页面正在发起的生成。
 * 推理模型光等响应头就能占住这个窗口好几秒。
 */
const pendingStarts = new Set();

let settings = { ...DEFAULT_SETTINGS };

/**
 * 本页正在流式接收的 job。用 Set 而不是单个 id：群聊和 swipe 可以同时有多个
 * 在途。
 */
const activeJobIds = new Set();

/** reload 后被本页收养、正在流式写进聊天的 job。 */
const liveReattachments = new Map();

/**
 * 本页已经处理完的 job：实时流完了、被用户停了、或已被恢复插入。
 * discovery 一律跳过。没有它，一次正常完成的生成会以「完成且未消费」的
 * 状态留在 relay 上，下一轮 discovery 会误判为孤儿，往已经显示的回复下面
 * 再插一份。
 */
const handledJobIds = new Set();

/**
 * 正在进行的恢复数（收养中转 job 或插入已完成 job）。全局同时最多恢复一条：
 * 恢复是往聊天里写回复，两条一起跑会插出两层楼；误触产生的多个孤儿 job 也
 * 绝不该一口气全收。取最新的优先，其余留给下一轮 discovery。
 */
let activeRecoveries = 0;

/** relay job 尚未建立时抛出，让调用方安全回退直连。 */
class RelayUnavailableError extends Error {}

/** 重试也无济于事时抛出（job 过期或被驱逐）。 */
class FatalRelayError extends Error {}

/* ---------------------------------------------------- Part A：fetch 劫持 */

const originalFetch = globalThis.fetch.bind(globalThis);

function resolveRelayTarget(input, init) {
    if (typeof input !== 'string' && !(input instanceof URL)) {
        return null;
    }
    if (String(init?.method ?? 'GET').toUpperCase() !== 'POST') {
        return null;
    }
    if (typeof init?.body !== 'string') {
        return null;
    }

    let url;
    try {
        url = new URL(String(input), globalThis.location.origin);
    } catch {
        return null;
    }

    if (url.origin !== globalThis.location.origin) {
        return null;
    }

    return RELAY_TARGETS.has(url.pathname) ? url.pathname : null;
}

function buildMeta(ctx) {
    return {
        chatId: ctx?.getCurrentChatId?.() ?? null,
        characterName: ctx?.name2 ?? null,
        startedAt: new Date().toISOString(),
        clientId: CLIENT_ID,
        requestId: newId(),
    };
}

async function relayRequest(target, init) {
    const payload = safeJson(init.body);
    if (!payload || typeof payload !== 'object') {
        throw new RelayUnavailableError('Request body is not a JSON object');
    }

    const headers = requestHeaders();
    const meta = buildMeta(getContextSafe());

    // 只在调用在途期间持有。job 在 relay 上先于本调用返回而注册，注册到返回
    // 之间得有东西替它说话；返回之后 activeJobIds 接管；若一直没返回，job
    // 是真孤儿，应该交给恢复流程。
    pendingStarts.add(meta.requestId);

    try {
        const startResponse = await originalFetch(`${RELAY_BASE}/start`, {
            method: 'POST',
            headers,
            body: JSON.stringify({ target, payload, meta }),
        });

        if (!startResponse.ok) {
            const detail = await startResponse.text().catch(() => '');
            throw new RelayUnavailableError(`Relay /start returned ${startResponse.status} ${detail}`);
        }

        const job = await startResponse.json();
        activeJobIds.add(job.jobId);
        console.info(`${LOG} relaying ${target} as job ${job.jobId} (upstream ${job.statusCode})`);

        return buildRelayedResponse(job, init, headers);
    } finally {
        // 在 activeJobIds 拿到 id 之后，两者之间不留空隙。
        pendingStarts.delete(meta.requestId);
    }
}

/**
 * 把 relay job 包成一个 Response，body 是一条会透明重连的流。ST 眼里这是一
 * 条偶尔停顿的连续响应；它永远不知道底下换过socket。
 */
function buildRelayedResponse(job, init, headers) {
    let finished = false;
    let offset = 0;
    let attempt = 0;
    let liveController = null;
    let releaseHooks = () => {};
    let abortSent = false;

    /**
     * 记下本页已处理过这个 job，discovery 不能再把它当待恢复的。
     * 调用时 job 必然还在 activeJobIds 里；两个集合刻意在这一瞬间重叠，
     * 否则一竿子已在途的 /jobs 响应会掉进缝隙里，回复被插第二次。
     */
    function claimJob() {
        handledJobIds.add(job.jobId);
    }

    /**
     * 让 relay 取消生成。幂等：Stop 的 signal 和消费方取消流都会走到这里，
     * 有时同一个 job 两者都来。
     */
    function abortJob() {
        if (abortSent) {
            return;
        }
        abortSent = true;
        // /abort 是发完就算，下一轮 discovery 时 job 可能还显示 running。
        // 在这里 claim 掉，用户刚停掉的生成不会被收养、再流回聊天里。
        claimJob();
        void originalFetch(`${RELAY_BASE}/abort`, {
            method: 'POST',
            headers,
            body: JSON.stringify({ jobId: job.jobId }),
            keepalive: true,
        }).catch(() => {});
    }

    async function pump(controller) {
        while (!finished) {
            liveController = new AbortController();

            try {
                const url = `${RELAY_BASE}/stream?jobId=${encodeURIComponent(job.jobId)}&from=${offset}`;
                const response = await originalFetch(url, {
                    headers: { 'Accept': 'text/event-stream' },
                    signal: liveController.signal,
                    cache: 'no-store',
                });

                if (response.status === 404) {
                    throw new FatalRelayError('Relay job expired before it could be delivered');
                }
                if (!response.ok || !response.body) {
                    throw new Error(`Relay /stream returned ${response.status}`);
                }

                attempt = 0;

                for await (const event of readServerEvents(response.body)) {
                    if (event.type === 'chunk') {
                        const bytes = base64ToBytes(event.data);
                        offset = Number(event.id) + bytes.length;
                        controller.enqueue(bytes);
                        continue;
                    }

                    if (event.type === 'end') {
                        finished = true;
                        claimJob();

                        const end = safeJson(event.data);
                        if (end?.status === 'error' && offset === 0) {
                            releaseHooks();
                            controller.error(new Error(end.error || 'Relayed generation failed'));
                            return;
                        }

                        // ST 已拿到完整回复。也告诉 relay 一声，同一聊天的其他
                        // 标签页就不会再提供一遍。
                        void markConsumed(job.jobId);
                        break;
                    }
                }

                if (!finished) {
                    // 流没等到 end 事件就断了：是连接死了而不是 job。重连续传。
                    throw new Error('Relay stream closed before completion');
                }
            } catch (error) {
                if (init.signal?.aborted || finished) {
                    break;
                }

                // 对一个已经挂掉的 relay 或永远回不来的 job 无限重连是错的：
                // 调用方会干等一条安静且不关闭的流，毫无回退手段。
                const exhausted = attempt >= MAX_RECONNECT_ATTEMPTS;

                if (error instanceof FatalRelayError || exhausted) {
                    finished = true;
                    if (exhausted) {
                        console.warn(`${LOG} giving up on job ${job.jobId} after ${attempt} failed reconnects: ${error.message}`);
                    }
                    if (offset === 0) {
                        releaseHooks();
                        controller.error(error);
                        return;
                    }
                    // 至少来了一部分。交出截断的回复好过整个丢弃；剩下的部分
                    // 如果 job 能跑完，恢复流程还会提供。
                    break;
                }

                const backoff = Math.min(500 * 2 ** attempt, 5000);
                attempt += 1;
                console.debug(`${LOG} resuming job ${job.jobId} from byte ${offset} in ${backoff}ms: ${error.message}`);
                await delay(backoff);
            }
        }

        releaseHooks();
        try {
            controller.close();
        } catch {
            // 已经关闭或出错。
        }
    }

    const stream = new ReadableStream({
        start(controller) {
            const signal = init.signal;

            // ST 在按 Stop 时 abort 这个 signal。挂起的标签页不会——这正是
            // 取消只从这里驱动的全部理由。
            const onAbort = () => {
                finished = true;
                liveController?.abort();
                abortJob();
            };

            // 从挂起中醒来的标签页常握着一条早已死掉、却要过几分钟才报告的
            // socket。拆掉它，循环立刻重连并重放积压。
            const onWake = () => {
                if (!finished && globalThis.document?.visibilityState !== 'hidden') {
                    liveController?.abort();
                }
            };

            const release = [
                listen(signal, 'abort', onAbort, { once: true }),
                listen(globalThis.document, 'visibilitychange', onWake),
                listen(globalThis, 'online', onWake),
            ];

            releaseHooks = () => {
                for (const off of release) {
                    off();
                }
                activeJobIds.delete(job.jobId);
            };

            /**
             * 修复（虚假生成请求，窗口 2）：signal 可能在等 /start 返回的期间
             * 就被 abort 了——「查看提示词」这类按钮拿到拼好的 prompt 后立刻
             * 停掉请求，正好落在这个窗口。已经 aborted 的 signal 永远不会再
             * 触发自己的监听器，不在这里补一刀的话，job 会无人取消地跑完，
             * 然后被恢复流程当成没人要的回复插进聊天。
             *
             * 处理方式与直连 fetch 的语义对齐：向上游转发取消，并把流置为
             * AbortError——ST 会把它当用户主动停止处理。
             */
            if (signal?.aborted) {
                finished = true;
                abortJob();
                releaseHooks();
                controller.error(signal.reason
                    ?? new DOMException('The user aborted a request.', 'AbortError'));
                return;
            }

            void pump(controller);
        },
        /**
         * 消费方丢弃了余下的响应。这就是一个停止的决定，与按 Stop 等价，
         * 同样要向上游取消——否则生成会在无人观看的状态下跑完，之后变成
         * 一个用户从没要过的恢复提示。
         */
        cancel() {
            const wasRunning = !finished;
            finished = true;
            liveController?.abort();
            if (wasRunning) {
                abortJob();
            }
            releaseHooks();
        },
    });

    return new Response(stream, {
        status: job.statusCode || 200,
        headers: { 'Content-Type': job.contentType || 'application/octet-stream' },
    });
}

function installFetchShim() {
    if (globalThis.fetch.__stBackgroundStream) {
        return;
    }

    const shim = async function stBackgroundStreamFetch(input, init) {
        /**
         * 修复（虚假生成请求，窗口 1）：signal 在调用 fetch 之前就已 aborted。
         * 「查看提示词」这类按钮就是这个模式——发一个生成请求只为从
         * CHAT_COMPLETION_SETTINGS_READY 里拿拼好的 prompt，随后立刻 abort。
         * 直接透传：原生 fetch 对 aborted signal 会立刻以 AbortError 拒绝，
         * 与没装本扩展时完全一致；而转发给 relay 会在服务端真开出一次
         * 谁也不会取消的生成。
         */
        if (init?.signal?.aborted) {
            return originalFetch(input, init);
        }

        if (!settings.enabled) {
            return originalFetch(input, init);
        }

        const target = resolveRelayTarget(input, init);
        if (!target) {
            return originalFetch(input, init);
        }

        try {
            return await relayRequest(target, init);
        } catch (error) {
            if (error instanceof RelayUnavailableError) {
                // 没建出 job，直接重试不会双重生成。
                console.warn(`${LOG} relay unavailable, falling back to a direct request:`, error.message);
                return originalFetch(input, init);
            }
            throw error;
        }
    };

    shim.__stBackgroundStream = true;
    globalThis.fetch = shim;
    console.info(`${LOG} fetch shim installed for ${RELAY_TARGETS.size} generation endpoints`);
}

/* ---------------------------------------------------- Part A：恢复（recovery） */

/**
 * 从缓冲的响应里尽力提取文本。这是整个扩展里唯一认得各家 provider 格式的
 * 一段，而且只在恢复路径上跑——页面被彻底销毁、没有活流可喂的时候。
 */
function pickDelta(json) {
    const choice = Array.isArray(json.choices) ? json.choices[0] : null;
    if (choice) {
        if (typeof choice.delta?.content === 'string') return choice.delta.content;
        if (typeof choice.delta?.text === 'string') return choice.delta.text;
        if (typeof choice.text === 'string') return choice.text;
        if (typeof choice.message?.content === 'string') return choice.message.content;
    }
    if (typeof json.delta?.text === 'string') return json.delta.text;
    if (Array.isArray(json.candidates)) {
        const parts = json.candidates[0]?.content?.parts ?? [];
        return parts.filter(p => typeof p.text === 'string' && !p.thought).map(p => p.text).join('');
    }
    if (typeof json.token === 'string') return json.token;
    if (typeof json.content === 'string' && json.object !== 'chat.completion.chunk') return json.content;
    return '';
}

function extractText(raw) {
    let text = '';

    for (const frame of raw.split(/\r\n\r\n|\n\n/)) {
        for (const line of frame.split(/\r\n|\n/)) {
            if (!line.startsWith('data:')) {
                continue;
            }
            const data = line.slice(5).trim();
            if (!data || data === '[DONE]') {
                continue;
            }
            const json = safeJson(data);
            if (json) {
                text += pickDelta(json);
            }
        }
    }

    if (text) {
        return text;
    }

    // 不是 SSE——非流式请求的普通 JSON 响应体。
    const json = safeJson(raw);
    return json ? pickDelta(json) : '';
}

/** 某个 job 的恢复消息当前所在的楼层，不在则 -1。 */
function messageIndexForJob(ctx, jobId) {
    const chat = ctx.chat ?? [];
    for (let i = chat.length - 1; i >= 0; i--) {
        if (chat[i]?.extra?.[JOB_MARKER] === jobId) {
            return i;
        }
    }
    return -1;
}

/**
 * 恢复的回复写回之前，把思维链和正文拆开。
 * 被 adopt 的生成不经过 ST 流式的 ReasoningHandler，原始思维链标签会原样
 * 落进 mes、extra.reasoning 空着。镜像 ST 自己的拆法：前后缀取自推理设置，
 * 未闭合的块视作进行中的思维链；前缀之前的内容保留为正文。
 * @returns {string} 消息正文（已剥去思维链）。
 */
function splitRecoveredReasoning(message, text) {
    const reasoningSettings = globalThis.SillyTavern?.getContext?.()?.powerUserSettings?.reasoning;
    const prefix = reasoningSettings?.prefix;
    const suffix = reasoningSettings?.suffix;

    if (!reasoningSettings?.auto_parse || !prefix || !suffix) {
        return text;
    }

    const start = text.indexOf(prefix);
    if (start === -1) {
        return text;
    }

    const end = text.indexOf(suffix, start + prefix.length);
    const reasoning = (end === -1
        ? text.slice(start + prefix.length)
        : text.slice(start + prefix.length, end)).trim();
    const content = ((start > 0 ? text.slice(0, start) : '')
        + (end === -1 ? '' : text.slice(end + suffix.length))).trim();

    message.extra = message.extra ?? {};
    message.extra.reasoning = reasoning;
    message.extra.reasoning_type = 'parsed';

    return content;
}

/**
 * 消息携带的变量不在它的正文里：有东西把 <UpdateVariable> 块从 mes 里解析
 * 出来、写进 chat[i].variables。在这张卡里那是酒馆助手的脚本，挂在 ST 为
 * 自己生成的回复触发的一串回调上。被恢复的回复不经过那些回调，变量池是
 * 空的——读它的每个状态栏都走各自的「无数据」分支把自己藏起来。
 * 先重放回调；都没接住的话，再把 JSON patch 套到上一层楼的变量上。
 */

/** 消息当前 swipe 用的变量对象，没有则 null。 */
function swipeVariables(message) {
    const pool = message?.variables;
    if (!Array.isArray(pool) || pool.length === 0) {
        return null;
    }
    return pool[message.swipe_id ?? 0] ?? pool[0] ?? null;
}

/** 这条消息是否已有可用的世界/用户状态。 */
function hasStatData(message) {
    const vars = swipeVariables(message);
    return Boolean(vars?.stat_data?.world && vars?.stat_data?.user);
}

/** 按出现顺序抽出正文里的每一段 JSON patch。 */
function collectPatches(text) {
    const patches = [];
    const block = /<JSONPatch>([\s\S]*?)<\/JSONPatch>/g;
    let match;
    while ((match = block.exec(text)) !== null) {
        const parsed = safeJson(match[1].trim());
        if (Array.isArray(parsed)) {
            patches.push(...parsed);
        }
    }
    return patches;
}

/** 把 JSON pointer 解析成父容器和末级键。 */
function patchTarget(root, pointer) {
    const parts = String(pointer)
        .split('/')
        .slice(1)
        .map(part => part.replace(/~1/g, '/').replace(/~0/g, '~'));

    let node = root;
    for (let i = 0; i < parts.length - 1; i++) {
        const key = parts[i];
        if (node[key] === undefined || node[key] === null || typeof node[key] !== 'object') {
            node[key] = {};
        }
        node = node[key];
    }
    return { parent: node, key: parts[parts.length - 1] };
}

/**
 * 应用这张卡会发出的那部分 JSON patch。其他的一律不碰而不去猜，
 * 一条坏 patch 不能毁掉一整层楼。
 */
function applyPatches(target, patches) {
    for (const patch of patches) {
        if (!patch || typeof patch.path !== 'string' || patch.path === '') {
            continue;
        }
        const { parent, key } = patchTarget(target, patch.path);
        if (patch.op === 'add' || patch.op === 'replace') {
            parent[key] = patch.value;
        } else if (patch.op === 'remove') {
            if (Array.isArray(parent)) {
                parent.splice(Number(key), 1);
            } else {
                delete parent[key];
            }
        }
    }
}

/** 最后手段：把算好的「上一层 + 本层 patch」写进本层。 */
function seedVariablesFromPreviousFloor(ctx, index) {
    const message = ctx.chat?.[index];
    if (!message || typeof message.mes !== 'string') {
        return false;
    }

    const computed = computedStatData(ctx.chat ?? [], index, message);
    if (!computed) {
        return false;
    }

    const swipe = message.swipe_id ?? 0;
    if (!Array.isArray(message.variables)) {
        message.variables = [];
    }
    message.variables[swipe] = computed;

    console.info(`${LOG} variable hooks did not fire; seeded floor ${index} from a previous floor + patches`);
    return true;
}

/** 这层楼还欠不欠一次变量结算：没有 stat_data，或只是原样继承了上一层。 */
function needsVariableSettle(ctx, index) {
    const message = ctx.chat?.[index];
    if (!message) {
        return false;
    }
    if (typeof message.mes !== 'string' || !message.mes.includes('<UpdateVariable>')) {
        return false;
    }
    if (!hasStatData(message)) {
        return true;
    }
    return statDataIsStale(ctx, index, message);
}

/** 已结算过的楼层（key 含正文长度），防止重复触发 MVU 的处理链。 */
const varSettledIndices = new Set();

/**
 * 结算恢复消息的变量。
 *
 * 只在正文写完的最终态调用——MVU 拿到残缺文本会算不出补丁、把上一层状态
 * 原样写回去，之后「已有 stat_data」的守卫反而会让正确数据永远进不来。
 * 这里是那个 bug 的修复点：结算一次性、只在结尾做。
 *
 * MVU 的处理链是异步的（事件经酒馆助手转发进脚本再写回来），emit 返回
 * 不等于它写完，所以每个事件之后轮询着等。
 */
async function settleVariables(ctx, index) {
    const message = ctx.chat?.[index];
    if (!message || typeof message.mes !== 'string' || !message.mes.includes('<UpdateVariable>')) {
        return;
    }

    const settleKey = `${ctx.getCurrentChatId?.() ?? ''}:${index}:${message.mes.length}`;
    if (varSettledIndices.has(settleKey) || !needsVariableSettle(ctx, index)) {
        return;
    }
    varSettledIndices.add(settleKey);

    const events = ctx.eventTypes ?? {};
    const source = ctx.eventSource;
    const attempts = [
        ['MESSAGE_RECEIVED', [index, 'normal'], 3000],
        ['GENERATION_ENDED', [index + 1], 2000],
    ];

    for (const [name, args, waitMs] of attempts) {
        const event = events[name];
        if (!event) {
            continue;
        }
        try {
            await source?.emit?.(event, ...args);
        } catch (error) {
            console.warn(`${LOG} ${name} listener threw while settling variables`, error);
        }

        // 轮询窗口：MVU 写好了（或本层补丁确实无改动）就算结算完成。
        for (let waited = 0; waited < waitMs; waited += 200) {
            if (!needsVariableSettle(ctx, index)) {
                console.info(`${LOG} variables for floor ${index} settled by ${name}`);
                return;
            }
            await delay(200);
        }
    }

    // MVU 不在场或没接住：按「上一层 + 本层 patch」自己兜底。
    seedVariablesFromPreviousFloor(ctx, index);
}

/**
 * 把 job 的文本写进聊天：首次见到就追加，之后原地更新。
 *
 * 每次调用都重新查索引而不是缓存，因为恢复可能正在填充一条消息的同时用户
 * 又发了新的；记住的索引会指到用户那条消息上把它覆盖掉。聊天已经翻篇时
 * 返回 false，调用方应当收手。
 *
 * final=true 表示这条回复到此写完：补上 gen_finished，重绘后楼层会像原生
 * 生成一样常驻显示生成耗时。
 */
async function writeRecoveredText(ctx, job, text, { final = false } = {}) {
    if ((ctx.getCurrentChatId?.() ?? null) !== (job.meta?.chatId ?? null)) {
        return false;
    }

    const index = messageIndexForJob(ctx, job.jobId);
    if (index !== -1) {
        ctx.chat[index].mes = splitRecoveredReasoning(ctx.chat[index], text);
        if (final && !ctx.chat[index].gen_finished) {
            ctx.chat[index].gen_finished = new Date().toISOString();
        }
        // 变量结算只在正文写完的最终态做：MVU 拿到残缺文本会算不出补丁、
        // 把上一层状态原样写回去，流式过程中绝不触发。
        if (final) {
            await settleVariables(ctx, index);
        }
        ctx.updateMessageBlock?.(index, ctx.chat[index]);
        return true;
    }

    const message = {
        name: job.meta?.characterName ?? ctx.name2,
        is_user: false,
        is_system: false,
        send_date: new Date().toISOString(),
        // 计时从生成真正开始算（meta 里带的是 relay /start 的时刻），不是从
        // 恢复开始算——中间挂起的那段时间也是生成时间。
        gen_started: job.meta?.startedAt ?? new Date().toISOString(),
        mes: text,
        extra: {
            st_background_stream_recovered: true,
            [JOB_MARKER]: job.jobId,
        },
    };

    message.mes = splitRecoveredReasoning(message, text);

    ctx.chat.push(message);
    await ctx.addOneMessage(message);

    // 重新定位而不是相信我们 push 时的下标：addOneMessage 会跑一些可能自己
    // 往聊天里追加内容的扩展。
    const fresh = messageIndexForJob(ctx, job.jobId);
    if (fresh !== -1) {
        // 首次创建一定发生在最终写（流式的第一块也走这条），此时正文可能
        // 还不完整——非 final 的调用不做变量结算。
        if (final) {
            await settleVariables(ctx, fresh);
        }
        // 重绘，让状态栏 iframe 对着刚 settle 好的变量而不是它们第一次见到的
        // 空池子重建。
        ctx.updateMessageBlock?.(fresh, ctx.chat[fresh]);
    }
    return true;
}

/**
 * 告诉 ST 其余部分：一条恢复的消息被（重）渲染了。
 * updateMessageBlock 只重画 DOM 不出声；在渲染之后才填内容的扩展——提示词
 * 模板、变量状态栏、一切 CHARACTER_MESSAGE_RENDERED 监听者——只认这个事件。
 */
function announceRendered(ctx, jobId) {
    const index = messageIndexForJob(ctx, jobId);
    if (index === -1) {
        return;
    }

    const types = ctx.eventTypes ?? {};
    ctx.eventSource?.emit?.(types.CHARACTER_MESSAGE_RENDERED, index, 'normal');
    ctx.eventSource?.emit?.(types.MESSAGE_UPDATED, index);
}

async function insertRecovered(ctx, job) {
    // 手动点击插入时也可能已有恢复在进行（收养还没跑完）。退回认领，让
    // discovery 下一轮重新提供这条，绝不并行插两条回复。
    if (activeRecoveries > 0) {
        handledJobIds.delete(job.jobId);
        toastr.info('Another recovered reply is still coming in — this one will be offered again shortly.', '后台流式生成');
        return;
    }

    activeRecoveries += 1;
    try {
        const response = await originalFetch(`${RELAY_BASE}/result?jobId=${encodeURIComponent(job.jobId)}`, {
            headers: requestHeaders(),
        });

        if (!response.ok) {
            toastr.error('Could not fetch the recovered generation.', '后台流式生成');
            return;
        }

        const text = extractText(await response.text()).trim();
        if (!text) {
            // 无论如何消费掉：解码不了的 job 留着不消费，每轮 discovery 都会被
            // 再提供一遍。字节仍在 /result。
            toastr.warning('The recovered generation could not be decoded.', '后台流式生成');
            await markConsumed(job.jobId);
            return;
        }

        if (!(await writeRecoveredText(ctx, job, text, { final: true }))) {
            return;
        }

        await ctx.saveChat();
        await markConsumed(job.jobId);
        announceRendered(ctx, job.jobId);
        // 合并自兜底修复扩展：插入完成后立刻做一轮楼层检查（思维链拆分收尾、
        // 前端代码块补渲染），不等 4 秒轮询。
        sweepSoon('recover');

        toastr.success('Recovered generation added to the chat.', '后台流式生成');
    } finally {
        activeRecoveries -= 1;
    }
}

/**
 * 收养一个仍在 relay 上跑的生成，把它流式写进聊天。这是页面被整个销毁时
 * 原本必然丢失的情况：没有活流可续，而生成也还没跑完。
 *
 * 故意不和 pump 共用循环。pump 喂的是 ST 正阻塞等待的 Response，必须区分
 * 致命与可续并交还部分字节；这条循环没人等它，停下来把 job 留给下一轮即可。
 */
/* ---------------- 生成中 UI（恢复期间模拟 ST 自己的生成状态） */

/**
 * ST 的「生成中」由三样东西组成：#mes_stop 显示、body[data-generating]
 * （style.css 借它藏起发送/继续/impersonate 按钮）、正在生成的楼层上的
 * .mes_timer 计时。正常生成时这些由 StreamingProcessor 驱动；页面被 iOS
 * 整个销毁后走恢复路径时没有 StreamingProcessor，这里按同样的机制补上，
 * 让回到页面的瞬间看起来和没离开过一样——包括会走的计时和可点的停止。
 *
 * 刻意不碰 ST 自己的状态键（dataset.generating / #mes_stop 的内联样式），
 * 用独立的一套：恢复和 ST 自己的生成同时发生时，两边互不拆台。
 */
const generatingUI = {
    active: false,
    timerId: null,
    startedAt: 0,
    jobId: null,
    release: [],
};

function ensureGeneratingUIStyle() {
    const doc = globalThis.document;
    if (!doc?.head || doc.getElementById('bg-stream-generating-style')) {
        return;
    }
    const style = doc.createElement('style');
    style.id = 'bg-stream-generating-style';
    // 第一条镜像 ST 自己的 body[data-generating] 规则，键换成我们的。
    style.textContent = [
        'body[data-bg-generating="true"] :is(#send_but, #mes_continue, #mes_impersonate) { display: none; }',
        '#mes_stop.bg-stream-visible { display: flex !important; }',
    ].join('\n');
    doc.head.append(style);
}

function beginGeneratingUI(job) {
    const doc = globalThis.document;
    const stop = doc?.getElementById?.('mes_stop');
    if (!doc?.body || !stop || generatingUI.active) {
        return;
    }

    ensureGeneratingUIStyle();
    generatingUI.active = true;
    generatingUI.jobId = job.jobId;
    // 计时从生成真正开始算，不是从恢复开始算。
    generatingUI.startedAt = Date.parse(job.meta?.startedAt ?? '') || Date.now();

    // ST 自己也在生成时，停止按钮本来就是它的，别碰显示；只拦点击没用——
    // 它点击停的是 ST 的生成，这里不装拦截。
    const stGenerating = stop.style.display === 'flex';
    if (!stGenerating) {
        stop.classList.add('bg-stream-visible');
        doc.body.dataset.bgGenerating = 'true';

        // 拦下 ST 的 stopGeneration（document 上的委托监听在冒泡阶段）：
        // 此刻该停的是恢复，不是（ST 眼里并不存在的）生成。
        const onStopClick = (event) => {
            event.stopImmediatePropagation();
            event.preventDefault();
            handledJobIds.add(generatingUI.jobId);
            void abortRelayJob(generatingUI.jobId);
            liveReattachments.get(generatingUI.jobId)?.stop();
            endGeneratingUI();
        };
        stop.addEventListener('click', onStopClick, { capture: true });
        generatingUI.release = [() => stop.removeEventListener('click', onStopClick, { capture: true })];
    }

    const tickTimer = () => {
        const ctx = getContextSafe();
        const index = messageIndexForJob(ctx, generatingUI.jobId);
        if (index === -1) {
            return;
        }
        const node = doc.querySelector(`#chat .mes[mesid="${index}"] .mes_timer`);
        if (!node) {
            return;
        }
        const seconds = (Date.now() - generatingUI.startedAt) / 1000;
        node.textContent = `${seconds.toFixed(1)}s`;
        node.title = `Recovered generation still running — ${seconds.toFixed(0)} s elapsed`;
    };

    generatingUI.timerId = globalThis.setInterval?.(tickTimer, 250);
    tickTimer();
}

function endGeneratingUI() {
    if (!generatingUI.active) {
        return;
    }
    generatingUI.active = false;
    globalThis.clearInterval?.(generatingUI.timerId);
    generatingUI.timerId = null;
    for (const off of generatingUI.release) {
        try {
            off();
        } catch {
            // 元素可能已经没了。
        }
    }
    generatingUI.release = [];

    const doc = globalThis.document;
    if (!doc?.body) {
        return;
    }
    doc.getElementById('mes_stop')?.classList.remove('bg-stream-visible');
    delete doc.body.dataset.bgGenerating;
}

/**
 * 收养一个仍在 relay 上跑的生成，把它流式写进聊天。这是页面被整个销毁时
 * 原本必然丢失的情况：没有活流可续，而生成也还没跑完。
 *
 * 故意不和 pump 共用循环。pump 喂的是 ST 正阻塞等待的 Response，必须区分
 * 致命与可续并交还部分字节；这条循环没人等它，停下来把 job 留给下一轮即可。
 */
async function reattachRunningJob(ctx, job) {
    const controller = new AbortController();
    const decoder = new TextDecoder('utf-8');

    let socket = null;
    let raw = '';
    let rendered = '';
    let lastRender = 0;
    let offset = 0;
    let attempt = 0;
    let finished = false;
    let detached = false;

    // 刚醒来的标签页常握着一条早已死掉却不肯承认的 socket。与活流同样处理。
    const onWake = () => {
        if (globalThis.document?.visibilityState !== 'hidden') {
            socket?.abort();
        }
    };

    const release = [
        listen(globalThis.document, 'visibilitychange', onWake),
        listen(globalThis, 'online', onWake),
    ];

    liveReattachments.set(job.jobId, {
        chatId: job.meta?.chatId ?? null,
        stop: () => controller.abort(),
    });

    /**
     * 重新提取并渲染。每次全量重解 buffer 原则上是 O(n²)，但回复只有几 KB，
     * 且最多每 RENDER_INTERVAL_MS 一次——换来的是复用同一个文本提取器，
     * 不必维护一个可能和它不一致的第二套增量解析。
     */
    async function paint(force) {
        const now = Date.now();
        if (!force && now - lastRender < RENDER_INTERVAL_MS) {
            return;
        }
        lastRender = now;

        const text = extractText(raw).trim();
        if (!text || text === rendered) {
            return;
        }
        rendered = text;

        if (!(await writeRecoveredText(ctx, job, text, { final: force }))) {
            detached = true;
            controller.abort();
        }
    }

    announceReattachment(job);
    // 页面回来时把「生成中」的样子摆出来：停止按钮、隐藏发送键、会走的
    // 计时。iOS 整页销毁后走恢复路径时，没有它发送键就是一副闲着的样子。
    beginGeneratingUI(job);

    try {
        while (!finished && !controller.signal.aborted) {
            socket = new AbortController();
            const cascade = () => socket.abort();
            controller.signal.addEventListener('abort', cascade, { once: true });

            try {
                const url = `${RELAY_BASE}/stream?jobId=${encodeURIComponent(job.jobId)}&from=${offset}`;
                const response = await originalFetch(url, {
                    headers: { 'Accept': 'text/event-stream' },
                    signal: socket.signal,
                    cache: 'no-store',
                });

                if (response.status === 404) {
                    throw new FatalRelayError('Job expired before it could be recovered');
                }
                if (!response.ok || !response.body) {
                    throw new Error(`Relay /stream returned ${response.status}`);
                }

                attempt = 0;

                for await (const event of readServerEvents(response.body)) {
                    if (event.type === 'chunk') {
                        const bytes = base64ToBytes(event.data);
                        offset = Number(event.id) + bytes.length;
                        // 整个 job 用一个解码器：续传按字节精确，各段必然连续，
                        // 跨块劈开的多字节字符也能正确解码。
                        raw += decoder.decode(bytes, { stream: true });
                        await paint(false);
                        continue;
                    }

                    if (event.type === 'end') {
                        finished = true;
                        break;
                    }
                }

                if (!finished) {
                    throw new Error('Relay stream closed before completion');
                }
            } catch (error) {
                if (controller.signal.aborted) {
                    break;
                }
                if (error instanceof FatalRelayError || attempt >= REATTACH_MAX_ATTEMPTS) {
                    console.warn(`${LOG} stopped recovering job ${job.jobId}: ${error.message}`);
                    break;
                }

                const backoff = Math.min(500 * 2 ** attempt, 5000);
                attempt += 1;
                console.debug(`${LOG} resuming recovery of ${job.jobId} from byte ${offset} in ${backoff}ms`);
                await delay(backoff);
            } finally {
                controller.signal.removeEventListener('abort', cascade);
            }
        }

        if (!detached) {
            await paint(true);
            if (rendered) {
                const settledIndex = messageIndexForJob(ctx, job.jobId);
                if (settledIndex !== -1) {
                    await settleVariables(ctx, settledIndex);
                }
                await ctx.saveChat();
                handledJobIds.add(job.jobId);
                await markConsumed(job.jobId);
                // 变量就位之后再重绘，状态栏 iframe 对着真实状态而不是空池子
                // 重建。
                const repaintIndex = messageIndexForJob(ctx, job.jobId);
                if (repaintIndex !== -1) {
                    ctx.updateMessageBlock?.(repaintIndex, ctx.chat[repaintIndex]);
                }
                announceRendered(ctx, job.jobId);
                sweepSoon('recover');
                console.info(`${LOG} recovered job ${job.jobId} into the chat (${rendered.length} chars)`);
            } else if (finished) {
                // 跑完了但解不出任何东西。照样消费，否则往后每轮 discovery
                // 都会收养它一遍。
                handledJobIds.add(job.jobId);
                await markConsumed(job.jobId);
            }
        }
    } finally {
        endGeneratingUI();
        for (const off of release) {
            off();
        }
        liveReattachments.delete(job.jobId);
    }
}

/**
 * 宣告一条消息正在自己出现，并给一条退路。
 * ST 不知道这个生成的存在——它没发起过——所以它自己的 Stop 按钮根本不在
 * 屏幕上。没有这一下，用户只能看着凭空出现的文字干瞪眼。
 */
function announceReattachment(job) {
    toastr.info('Picking up a generation that was still running. Click to stop it.', '后台流式生成', {
        timeOut: 8000,
        closeButton: true,
        onclick: () => {
            // 先认领再停：/abort 万一没送到，下一轮 discovery 也不能把它
            // 当孤儿重新收养回来。
            handledJobIds.add(job.jobId);
            liveReattachments.get(job.jobId)?.stop();
            void abortRelayJob(job.jobId);
        },
    });
}

function abortRelayJob(jobId) {
    return originalFetch(`${RELAY_BASE}/abort`, {
        method: 'POST',
        headers: requestHeaders(),
        body: JSON.stringify({ jobId }),
    }).catch(() => {});
}

/**
 * 告诉 relay 这个 job 已处理，之后加载的页面不再提供它。
 * 用 keepalive 的理由同 abortJob：它在回复落地的瞬间触发，正是人们锁屏的
 * 瞬间。丢掉的 consume 会让 job 以「完成且未认领」留在 relay 上，下一个
 * 页面加载——新的 CLIENT_ID、不记得处理过它——会把 ST 已经存好的回复再插一份。
 */
async function markConsumed(jobId) {
    await originalFetch(`${RELAY_BASE}/consume`, {
        method: 'POST',
        headers: requestHeaders(),
        body: JSON.stringify({ jobId }),
        keepalive: true,
    }).catch(() => {});
}

function offerRecovery(ctx, job) {
    const preview = `A generation finished while this tab was away (${job.byteLength} bytes). Click to add it to the chat.`;

    toastr.info(preview, '后台流式生成', {
        timeOut: 0,
        extendedTimeOut: 0,
        closeButton: true,
        tapToDismiss: false,
        onclick: () => void insertRecovered(ctx, job).catch(error => {
            console.error(`${LOG} recovery failed`, error);
            toastr.error('Recovery failed — see the console.', '后台流式生成');
        }),
        onCloseClick: () => void markConsumed(job.jobId),
    });
}

/**
 * 覆盖最坏情况：iOS 把页面整个丢弃而非仅仅挂起，没有活流可续。
 * 两种状态都要管：已跑完的 job 整取插入；还在跑的收养进来流式续写。
 */
async function checkForRecoverableJobs() {
    const ctx = getContextSafe();
    if (!ctx || !settings.recover) {
        return;
    }

    // iOS 开机安静期：等页面安定，job 在 relay 上跑不了。
    if (Date.now() - BOOT_AT < BOOT_QUIET_MS) {
        return;
    }

    const chatId = ctx.getCurrentChatId?.() ?? null;
    if (!chatId) {
        return;
    }

    try {
        const response = await originalFetch(`${RELAY_BASE}/jobs`, { headers: requestHeaders() });
        if (!response.ok) {
            return;
        }

        const { jobs = [] } = await response.json();

        // 一次只恢复一条，最新的优先。上一条还没恢复完时，本轮到此为止。
        jobs.sort((a, b) => (b.createdAt ?? 0) - (a.createdAt ?? 0));

        for (const job of jobs) {
            const mine = job.meta?.chatId === chatId;
            const spokenFor = activeJobIds.has(job.jobId)
                || liveReattachments.has(job.jobId)
                || handledJobIds.has(job.jobId);
            if (!mine || job.consumedAt || spokenFor) {
                continue;
            }

            // 同时只恢复一条回复：已经有一条在收养/插入时，剩下的全部留给
            // 下一轮 discovery，绝不并行开第二条。
            if (activeRecoveries > 0) {
                break;
            }

            if (RUNNING_STATES.has(job.status)) {
                // 本页还在等着它：活流马上就要接管。这是 activeJobIds 盖不住
                // 的空隙——/start 还没交出 id 放进去。
                if (job.meta?.clientId === CLIENT_ID && pendingStarts.has(job.meta?.requestId)) {
                    continue;
                }

                // 保险丝：这个 job 已经拖垮过本会话两次，不再自动收养。
                // 等它跑完转成 done，走插入路径的保险丝（降级为手动）。
                if (recoveryAttempts(job.jobId) >= RECOVERY_ATTEMPT_LIMIT) {
                    continue;
                }
                noteRecoveryAttempt(job.jobId);

                activeRecoveries += 1;
                void reattachRunningJob(ctx, job)
                    .catch(error =>
                        console.warn(`${LOG} recovery of job ${job.jobId} failed:`, error))
                    .finally(() => {
                        activeRecoveries -= 1;
                    });
                // 本轮已经收养了一条，不再看其余的。
                break;
            }

            if (job.status !== 'done' || job.byteLength === 0) {
                continue;
            }

            // 插入之前先认领：/consume 要等文本落地才标记，而 discovery 在那
            // 之前就会再转一圈。
            handledJobIds.add(job.jobId);

            if (settings.autoInsert) {
                // 保险丝：自动插入已经失败/崩溃两次的 job 改为手动提供。
                // 保持认领，避免每轮 discovery 都弹一条持久 toast。
                if (recoveryAttempts(job.jobId) >= RECOVERY_ATTEMPT_LIMIT) {
                    offerRecovery(ctx, job);
                    break;
                }
                noteRecoveryAttempt(job.jobId);
                void insertRecovered(ctx, job)
                    .catch(error =>
                        console.warn(`${LOG} recovery of job ${job.jobId} failed:`, error));
                break;
            } else {
                offerRecovery(ctx, job);
            }
        }
    } catch (error) {
        console.debug(`${LOG} recovery check skipped:`, error);
    }
}

/** 持续观察，而不是只在启动时查一次。 */
function watchForRecoverableJobs(ctx) {
    const tick = () => {
        if (globalThis.document?.visibilityState === 'hidden') {
            return;
        }
        void checkForRecoverableJobs();
    };

    setInterval(tick, DISCOVERY_INTERVAL_MS);
    // 开机安静期不是干等轮询：时间一到立刻查一次，iOS 上恢复的等待上限
    // 就是安静期本身（8 秒），而不是「安静期 + 一个轮询周期」。
    if (BOOT_QUIET_MS > 0) {
        setTimeout(tick, BOOT_QUIET_MS);
    } else {
        tick();
    }
    listen(globalThis.document, 'visibilitychange', tick);
    listen(globalThis, 'online', tick);

    // job 按聊天划分，切聊天会退役属于旧聊天的恢复，也可能揭开新聊天里
    // 等着的 job。
    ctx?.eventSource?.on?.(ctx?.eventTypes?.CHAT_CHANGED, () => {
        const chatId = ctx.getCurrentChatId?.() ?? null;
        for (const live of liveReattachments.values()) {
            if (live.chatId !== chatId) {
                live.stop();
            }
        }
        tick();
    });

    // 兜底。ST 对自己发起的生成也会触发这个事件（那些经由 fetch signal 取消）；
    // 万一按钮够得着，对收养的 job 也照办。
    ctx?.eventSource?.on?.(ctx?.eventTypes?.GENERATION_STOPPED, () => {
        for (const [jobId, live] of liveReattachments) {
            live.stop();
            void abortRelayJob(jobId);
        }
    });

    tick();
}

/* ==================================================== Part B：楼层兜底修复 */

/** 扫描最近多少层楼。够了，多了只是浪费。 */
const SCAN_DEPTH = 6;

/** 兜底轮询间隔（只在页面可见、且没有在生成时跑）。 */
const POLL_MS = 4000;

/** iframe 低于这个高度就当作「没渲染出来」。 */
const MIN_IFRAME_HEIGHT = 3;

/** 同一层楼最多请求酒馆助手重绘几次。 */
const MAX_RENDER_TRIES = 3;

/** 修完消息等多久再去看渲染结果（酒馆助手是异步挂 iframe 的）。 */
const SETTLE_DELAY = 400;

/** 页面刚加载 / 刚回到前台时，隔多久补扫一次。 */
const WAKE_DELAYS = [800, 2500, 6000];

let repairing = false;
let healing = false;

/** 已尝试过思维链修复的消息（key 含正文长度，正文变了会重新试）。 */
const reasoningTried = new Set();

/** 已尝试过变量修复的消息。 */
const varsTried = new Set();

/** 已请求重绘的次数，key -> count。 */
const renderTries = new Map();

function isVisible() {
    return (globalThis.document?.visibilityState ?? 'visible') === 'visible';
}

/** 生成中不动手：消息还在长，任何拆解都可能拆到一半的内容。 */
function isGenerating() {
    const stop = globalThis.document?.querySelector?.('#mes_stop');
    if (!stop) {
        return false;
    }
    return globalThis.getComputedStyle?.(stop)?.display !== 'none';
}

function messageKey(ctx, index, message) {
    return `${ctx.getCurrentChatId?.() ?? ''}:${index}:${message?.mes?.length ?? 0}`;
}

/** 通知 ST / 酒馆助手这一层重画了。updateMessageBlock 本身不发事件，必须补这一下。 */
function notifyRendered(ctx, index) {
    try {
        ctx.eventSource?.emit?.(ctx.eventTypes?.CHARACTER_MESSAGE_RENDERED, index, 'normal');
    } catch (error) {
        console.warn(`${LOG_FIX} 广播渲染事件失败`, error);
    }
}

function recentIndexes(chat) {
    const start = Math.max(0, chat.length - SCAN_DEPTH);
    const list = [];
    for (let i = start; i < chat.length; i++) {
        list.push(i);
    }
    return list;
}

/* -------------------------------------------------- *
 * B-1. 思维链
 * -------------------------------------------------- */

/**
 * 按前缀/后缀拆成「思维链」和「正文」，前缀之前的内容算正文保留。
 * 没有成对标签时返回 null（生成到一半的消息就属于这种）。
 * 与 splitRecoveredReasoning 的区别：那条给恢复路径用，未闭合的块按进行中
 * 处理；这条给扫描修复用，只碰已经写完的楼层。
 */
function splitReasoning(text, prefix, suffix) {
    const start = text.indexOf(prefix);
    if (start === -1) {
        return null;
    }
    const end = text.indexOf(suffix, start + prefix.length);
    if (end === -1) {
        return null;
    }
    return {
        reasoning: text.slice(start + prefix.length, end).trim(),
        content: ((start > 0 ? text.slice(0, start) : '') + text.slice(end + suffix.length)).trim(),
    };
}

function repairReasoning(ctx) {
    const reasoningSettings = ctx.powerUserSettings?.reasoning;
    if (!reasoningSettings?.auto_parse || !reasoningSettings.prefix || !reasoningSettings.suffix) {
        return 0;
    }

    const chat = ctx.chat ?? [];
    let fixed = 0;

    for (const index of recentIndexes(chat)) {
        const message = chat[index];
        if (!message || message.is_user || message.is_system) {
            continue;
        }

        const key = messageKey(ctx, index, message);
        if (reasoningTried.has(key)) {
            continue;
        }
        reasoningTried.add(key);

        const text = message.mes;
        if (typeof text !== 'string'
            || !text.includes(reasoningSettings.prefix)
            || !text.includes(reasoningSettings.suffix)) {
            continue;
        }

        // 已经有思维链就是 ST 自己解析好的，绝不覆盖。
        if (message.extra?.reasoning) {
            continue;
        }

        const parts = splitReasoning(text, reasoningSettings.prefix, reasoningSettings.suffix);
        if (!parts?.reasoning) {
            continue;
        }

        message.extra = message.extra ?? {};
        message.extra.reasoning = parts.reasoning;
        message.extra.reasoning_type = 'parsed';
        message.mes = parts.content;

        // display_text 优先于 mes 参与渲染，留着旧值会把刚拆好的正文盖回去。
        delete message.extra.display_text;

        try {
            ctx.updateMessageBlock?.(index, message);
        } catch (error) {
            console.warn(`${LOG_FIX} 第 ${index} 楼重画失败（内容已修好，会随聊天保存）`, error);
        }
        notifyRendered(ctx, index);

        fixed += 1;
        console.info(`${LOG_FIX} 第 ${index} 楼补上思维链：${parts.reasoning.length} 字`);
    }

    return fixed;
}

/* -------------------------------------------------- *
 * B-2. 变量（MVU <UpdateVariable>/<JSONPatch>）
 * -------------------------------------------------- */

/** 同层或之前最近的一条带 stat_data 的消息变量。 */
function previousState(chat, index) {
    for (let i = index - 1; i >= 0; i--) {
        if (hasStatData(chat[i])) {
            return swipeVariables(chat[i]);
        }
    }
    return null;
}

/**
 * 按「上一层的状态 + 本层正文里的 patch」算出本层应有的状态。
 * 算不出来（上一层没状态 / 正文没有 JSON patch）返回 null。
 */
function computedStatData(chat, index, message) {
    const source = previousState(chat, index);
    if (!source) {
        return null;
    }
    const patches = collectPatches(message.mes ?? '');
    if (patches.length === 0) {
        return null;
    }
    const copy = JSON.parse(JSON.stringify(source));
    copy.stat_data = copy.stat_data ?? {};
    applyPatches(copy.stat_data, patches);
    return copy;
}

/**
 * 这层的 stat_data 是否只是原样继承了上一层——补丁还没生效的标志。
 * MVU 拿到残缺文本时就会写出这种「上一层状态」。
 * 判据保守：与上一层完全一致才算陈旧；补丁本来就没有实际改动的楼层不会被误伤。
 */
function statDataIsStale(ctx, index, message) {
    const current = swipeVariables(message);
    if (!current?.stat_data) {
        return false;
    }
    const source = previousState(ctx.chat ?? [], index);
    if (!source?.stat_data) {
        return false;
    }
    return JSON.stringify(current.stat_data) === JSON.stringify(source.stat_data);
}

function repairVariables(ctx) {
    const chat = ctx.chat ?? [];
    let fixed = 0;

    for (const index of recentIndexes(chat)) {
        const message = chat[index];
        if (!message || message.is_user || message.is_system) {
            continue;
        }

        const text = message.mes;
        if (typeof text !== 'string' || !text.includes('<UpdateVariable>')) {
            continue;
        }

        const key = messageKey(ctx, index, message);
        if (varsTried.has(key)) {
            continue;
        }
        varsTried.add(key);

        if (hasStatData(message)) {
            // stat_data 在，但可能只是原样继承了上一层——后台生成时 MVU 没接到
            // 完整文本就会写出这种陈旧状态。只在「与上一层完全一致、而本层
            // 补丁明明会改动」时才判定为陈旧并重算；补丁确实无改动的楼层
            // 不会被误伤，MVU 自己算好的正确状态更不会被碰。
            if (!statDataIsStale(ctx, index, message)) {
                continue;
            }
            const computed = computedStatData(chat, index, message);
            if (!computed) {
                continue;
            }
            message.variables[message.swipe_id ?? 0] = computed;

            try {
                ctx.updateMessageBlock?.(index, message);
            } catch (error) {
                console.warn(`${LOG_FIX} 第 ${index} 楼重画失败（变量已重算）`, error);
            }
            notifyRendered(ctx, index);

            fixed += 1;
            console.info(`${LOG_FIX} 第 ${index} 楼的变量是陈旧的上一层状态，已按本层补丁重算`);
            continue;
        }

        // 变量整个缺失（酒馆助手的变量继承没接住恢复消息）：上一层 + 本层 patch。
        const source = previousState(chat, index);
        if (!source) {
            continue;
        }

        const patches = collectPatches(text);
        if (patches.length === 0) {
            continue;
        }

        const copy = JSON.parse(JSON.stringify(source));
        copy.stat_data = copy.stat_data ?? {};
        applyPatches(copy.stat_data, patches);

        if (!Array.isArray(message.variables)) {
            message.variables = [];
        }
        message.variables[message.swipe_id ?? 0] = copy;

        try {
            ctx.updateMessageBlock?.(index, message);
        } catch (error) {
            console.warn(`${LOG_FIX} 第 ${index} 楼重画失败（变量已写好）`, error);
        }
        notifyRendered(ctx, index);

        fixed += 1;
        console.info(`${LOG_FIX} 第 ${index} 楼补上变量：继承上一层 + ${patches.length} 条 patch`);
    }

    return fixed;
}

/* -------------------------------------------------- *
 * B-3. 渲染 / 折叠
 * -------------------------------------------------- */

/** 酒馆助手的判定方式：含 html>/<head>/<body 的代码块才是「前端界面」。 */
function isFrontendText(text) {
    return ['html>', '<head>', '<body'].some(tag => text.includes(tag));
}

/** 酒馆助手没装 / 还没加载完时，什么都别做。 */
function tavernHelperReady(doc) {
    return Boolean(globalThis.TavernHelper) || Boolean(doc?.querySelector?.('div.TH-render'));
}

/**
 * 读酒馆助手自己的渲染设置（它存在 extension_settings.tavern_helper）。
 * 用户把渲染器关了、或者限制了渲染深度时，别去做无用功。
 */
function renderConfig(ctx) {
    try {
        return ctx?.extensionSettings?.tavern_helper?.settings?.render ?? null;
    } catch {
        return null;
    }
}

/**
 * 同源的 srcdoc，直接透过 contentDocument 量真实高度。
 * 它自己的 adjust_iframe_height.js 只在 rAF 里跑，标签页 hidden 时永远量不到。
 */
function measure(iframe) {
    try {
        const doc = iframe.contentDocument;
        if (!doc || doc.readyState === 'loading') {
            return 0;
        }
        const body = doc.body;
        if (!body) {
            return 0;
        }
        const height = Math.max(
            body.scrollHeight || 0,
            body.offsetHeight || 0,
            doc.documentElement?.scrollHeight || 0,
        );
        if (height > MIN_IFRAME_HEIGHT) {
            iframe.style.height = `${height}px`;
        }
        return height;
    } catch {
        return 0;
    }
}

function requestRender(ctx, mesid, triesKey) {
    const used = renderTries.get(triesKey) ?? 0;

    if (used < MAX_RENDER_TRIES) {
        renderTries.set(triesKey, used + 1);
        notifyRendered(ctx, mesid);
        console.info(`${LOG_FIX} 第 ${mesid} 楼的前端界面没渲染，已请求重绘（第 ${used + 1} 次）`);
        return true;
    }

    // 还是没动，就换一条路：MESSAGE_UPDATED。和「点编辑再退出」走的是同一条
    // 渲染路径，实测能把裸代码块换成前端界面。只升级一次。
    const fallbackKey = `updated:${triesKey}`;
    if (renderTries.has(fallbackKey)) {
        return false;
    }
    renderTries.set(fallbackKey, 1);
    try {
        const message = ctx.chat?.[mesid];
        if (!message) {
            return false;
        }
        // 编辑消息后的那条路径：先用消息内容重画 DOM，再广播 MESSAGE_UPDATED。
        ctx.updateMessageBlock?.(mesid, message);
        ctx.eventSource?.emit?.(ctx.eventTypes?.MESSAGE_UPDATED, mesid, message.mes);
    } catch (error) {
        console.warn(`${LOG_FIX} 升级重绘失败`, error);
        return false;
    }
    console.info(`${LOG_FIX} 第 ${mesid} 楼重绘无效，已改用 MESSAGE_UPDATED`);
    return true;
}

/**
 * 扫最近几层：该渲染成 iframe 的代码块，没有 iframe 就请求重绘，
 * 有 iframe 但高度为 0 就替它量一次。
 */
function heal(ctx) {
    if (healing) {
        return 0;
    }

    const doc = globalThis.document;
    if (!doc || !tavernHelperReady(doc)) {
        return 0;
    }

    const config = renderConfig(ctx);
    if (config && config.enabled === false) {
        return 0;
    }

    const nodes = doc.querySelectorAll('#chat > .mes');
    if (!nodes.length) {
        return 0;
    }

    const chat = ctx.chat ?? [];
    healing = true;
    let acted = 0;
    let requested = 0;

    try {
        // 渲染深度 > 0 时，酒馆助手本来就只渲染最近几层，再往里找没有意义。
        const depth = Number(config?.depth) > 0 ? Number(config.depth) : 0;
        const start = depth > 0 ? Math.max(0, nodes.length - depth) : Math.max(0, nodes.length - SCAN_DEPTH);

        // 开了「允许流式渲染」时，酒馆助手是故意不接管成品消息的（交给流式组件），
        // 这时候请求重绘没有意义，只保留补高度。
        const streamingMode = Boolean(config?.allow_streaming);

        for (let i = start; i < nodes.length; i++) {
            const node = nodes[i];
            const mesid = Number(node.getAttribute('mesid'));
            if (!Number.isFinite(mesid)) {
                continue;
            }

            const triesKey = `${mesid}:${chat[mesid]?.mes?.length ?? 0}`;

            for (const pre of node.querySelectorAll('.mes_text pre')) {
                if (!isFrontendText(pre.textContent ?? '')) {
                    continue;
                }

                // 被酒馆助手包过就是「已接管」，否则连 iframe 都不会有。
                const box = pre.parentElement?.closest?.('div.TH-render');

                if (!box) {
                    if (!streamingMode && requestRender(ctx, mesid, triesKey)) {
                        acted += 1;
                        requested += 1;
                    }
                    continue;
                }

                const iframe = box.querySelector('iframe');
                if (!iframe) {
                    // 包了但还没挂 iframe：可能正在挂，连续几轮都这样才升级成重绘。
                    const waitKey = `wait:${triesKey}`;
                    const waited = (renderTries.get(waitKey) ?? 0) + 1;
                    renderTries.set(waitKey, waited);
                    if (!streamingMode && waited >= 2 && requestRender(ctx, mesid, triesKey)) {
                        acted += 1;
                        requested += 1;
                    }
                    continue;
                }

                if (iframe.offsetHeight > MIN_IFRAME_HEIGHT) {
                    continue;
                }

                if (measure(iframe) > MIN_IFRAME_HEIGHT) {
                    acted += 1;
                    console.info(`${LOG_FIX} 第 ${mesid} 楼前端界面的高度补齐了`);
                }
            }
        }
    } finally {
        healing = false;
    }

    // 刚请求过重绘就尽快回头看一眼，别等 4 秒一次的轮询，那样最多要等十几秒。
    // 次数由 requestRender 里的上限兜底，链条长度有限。
    if (requested > 0) {
        globalThis.setTimeout?.(() => heal(ctx), 900);
    }

    return acted;
}

/* -------------------------------------------------- *
 * B 调度
 * -------------------------------------------------- */

function sweep(reason = 'tick') {
    if (!settings.fix) {
        return 0;
    }
    if (!isVisible()) {
        return 0;
    }

    const ctx = getContextSafe();
    if (!ctx?.chat?.length || isGenerating() || repairing) {
        return 0;
    }

    repairing = true;
    let changed = 0;
    try {
        changed = repairReasoning(ctx) + repairVariables(ctx);
        if (changed > 0) {
            Promise.resolve(ctx.saveChat?.()).catch(error => console.warn(`${LOG_FIX} 保存失败`, error));
        }
    } catch (error) {
        console.warn(`${LOG_FIX} 修复失败（${reason}）`, error);
    } finally {
        repairing = false;
    }

    if (changed > 0) {
        // 消息刚重画过，等酒馆助手把帧挂完再去检查渲染结果。
        globalThis.setTimeout?.(() => heal(ctx), SETTLE_DELAY);
    } else {
        heal(ctx);
    }

    return changed;
}

function sweepSoon(reason) {
    for (const delayMs of WAKE_DELAYS) {
        globalThis.setTimeout?.(() => sweep(reason), delayMs);
    }
}

function initBgFix(ctx) {
    const { eventSource, eventTypes } = ctx;
    const on = (name, handler) => {
        const event = eventTypes?.[name];
        if (event) {
            eventSource?.on?.(event, handler);
        }
    };

    // 一张聊天刚打开：等它渲染完再看。
    on('CHAT_CHANGED', () => sweepSoon('chat'));
    on('CHAT_LOADED', () => sweepSoon('load'));

    // 生成结束 / 收到消息 / 换 swipe：新增或变化的那条最需要检查。
    on('GENERATION_ENDED', () => sweepSoon('end'));
    on('GENERATION_STOPPED', () => sweepSoon('stop'));
    on('MESSAGE_RECEIVED', () => sweepSoon('recv'));
    on('MESSAGE_SWIPED', () => sweepSoon('swipe'));

    // 手机切回前台：后台期间页面的 rAF 是停的，很多事都得在这时候补。
    globalThis.document?.addEventListener?.('visibilitychange', () => {
        if (isVisible()) {
            sweepSoon('wake');
        }
    });

    // 兜底：可见时低频扫一遍。
    globalThis.setInterval?.(() => {
        if (isVisible()) {
            sweep('poll');
        }
    }, POLL_MS);

    sweepSoon('init');
    console.info(`${LOG_FIX} 已启用（随 Background Streaming 合并版加载）`);
}

/* ---------------------------------------------------------------- 设置面板 */

function loadSettings() {
    const ctx = getContextSafe();
    if (!ctx?.extensionSettings) {
        return;
    }
    ctx.extensionSettings[MODULE_NAME] = Object.assign({}, DEFAULT_SETTINGS, ctx.extensionSettings[MODULE_NAME]);
    settings = ctx.extensionSettings[MODULE_NAME];
}

function renderSettingsPanel() {
    const container = document.getElementById('extensions_settings2');
    if (!container || document.getElementById('bg_stream_settings')) {
        return;
    }

    const panel = document.createElement('div');
    panel.id = 'bg_stream_settings';
    panel.classList.add('bg-stream-settings');
    panel.innerHTML = `
        <div class="inline-drawer">
            <div class="inline-drawer-toggle inline-drawer-header">
                <b>后台流式生成</b>
                <div class="inline-drawer-icon fa-solid fa-circle-chevron-down down"></div>
            </div>
            <div class="inline-drawer-content">
                <small class="bg-stream-hint">
                    把流式生成转发到 <code>st-relay</code> 服务端插件，锁屏或切后台不再取消生成；
                    并自动修复恢复消息的思维链、MVU 变量与前端代码块渲染。
                </small>
                <label class="checkbox_label" for="bg_stream_enabled">
                    <input id="bg_stream_enabled" type="checkbox">
                    <span>生成请求经由服务器中转（后台不断流）</span>
                </label>
                <label class="checkbox_label" for="bg_stream_recover">
                    <input id="bg_stream_recover" type="checkbox">
                    <span>页面刷新后找回未送达的生成</span>
                </label>
                <label class="checkbox_label" for="bg_stream_auto_insert">
                    <input id="bg_stream_auto_insert" type="checkbox">
                    <span>找回的回复自动插入聊天，无需确认</span>
                </label>
                <label class="checkbox_label" for="bg_stream_fix">
                    <input id="bg_stream_fix" type="checkbox">
                    <span>楼层兜底修复（思维链 / 变量 / 前端渲染）</span>
                </label>
                <div id="bg_stream_status" class="bg-stream-status">Checking relay…</div>
            </div>
        </div>`;

    container.append(panel);

    const enabled = panel.querySelector('#bg_stream_enabled');
    const recover = panel.querySelector('#bg_stream_recover');
    const autoInsert = panel.querySelector('#bg_stream_auto_insert');
    const fix = panel.querySelector('#bg_stream_fix');
    const ctx = getContextSafe();

    enabled.checked = Boolean(settings.enabled);
    recover.checked = Boolean(settings.recover);
    autoInsert.checked = Boolean(settings.autoInsert);
    autoInsert.disabled = !settings.recover;
    fix.checked = settings.fix !== false;

    enabled.addEventListener('change', () => {
        settings.enabled = enabled.checked;
        ctx?.saveSettingsDebounced?.();
    });

    recover.addEventListener('change', () => {
        settings.recover = recover.checked;
        autoInsert.disabled = !recover.checked;
        ctx?.saveSettingsDebounced?.();
    });

    autoInsert.addEventListener('change', () => {
        settings.autoInsert = autoInsert.checked;
        ctx?.saveSettingsDebounced?.();
    });

    fix.addEventListener('change', () => {
        settings.fix = fix.checked;
        ctx?.saveSettingsDebounced?.();
    });

    void reportRelayStatus();
}

async function reportRelayStatus() {
    const target = document.getElementById('bg_stream_status');
    if (!target) {
        return;
    }

    try {
        const response = await originalFetch(`${RELAY_BASE}/health`, { headers: requestHeaders() });
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        const health = await response.json();
        target.textContent = `Relay v${health.version} connected — ${health.jobs} buffered job(s).`;
        target.classList.remove('bg-stream-status--bad');
    } catch (error) {
        target.textContent = 'Relay plugin not reachable. Enable server plugins and restart SillyTavern.';
        target.classList.add('bg-stream-status--bad');
        console.warn(`${LOG} relay health check failed:`, error);
    }
}

/* ---------------------------------------------------------------------- init */

// 在 import 时安装，任何生成请求都抢不在 shim 前面，哪怕应用自己的启动和
// 扩展加载器赛跑。
installFetchShim();

(function initialize() {
    let started = false;

    // APP_READY 是重放事件，下面的定时器是老版本的兜底，所以要防跑两次、
    // 防恢复提示重复出现。
    const start = () => {
        // ST 还没起来（极慢的加载）时别永久放弃，稍后再试；兜底修复的事件
        // 监听也得等 ctx 才能挂。
        if (!getContextSafe()) {
            setTimeout(start, 2000);
            return;
        }
        if (started) {
            return;
        }
        started = true;
        loadSettings();
        renderSettingsPanel();
        watchForRecoverableJobs(getContextSafe());

        const ctx = getContextSafe();
        if (ctx) {
            initBgFix(ctx);
        }
    };

    const ctx = getContextSafe();
    if (ctx?.eventSource?.on && ctx?.eventTypes?.APP_READY) {
        ctx.eventSource.on(ctx.eventTypes.APP_READY, start);
    }
    setTimeout(start, 3000);
})();

export { installFetchShim };
