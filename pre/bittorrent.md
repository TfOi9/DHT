# BitTorrent

基于 Kademlia DHT 的 BitTorrent P2P 文件下载与传输实现，课程设计项目。

![BitTorrent Logo](./figs/BitTorrent_logo.svg.png)
![BitTorrent Network](./figs/BitTorrent_network.svg.png)

## 技术栈

| 组件 | 语言 | 说明 |
|------|------|------|
| DHT 层 | Go | Kademlia 协议实现，编译为 `dht-sidecar` 独立进程 |
| 后端 | Rust (tokio) | Peer Wire Protocol、Session 调度、存储，编译为 `bittorrent` |
| 通信 | gRPC / Protobuf | Rust 后端与 Go DHT 通过 gRPC 在本机通信，解耦两层 |

## 实现内容

- **Bencode 编解码与 .torrent 解析** — 完整解析器，支持单/多文件，提取 info_hash、pieces 等字段
- **Peer Wire Protocol** — 握手、9 种消息类型编解码（Choke/Unchoke/Interested/Have/Bitfield/Request/Piece/Cancel）、缓冲区分帧
- **网络通信** — TCP 出站连接 / 入站监听，`tokio::select!` 三路并发事件循环，连接管理与去重
- **Session 调度器** — 消息分发、下载管线、SHA1 校验、Have 广播、DHT 动态发现 Peer
- **存储层** — Piece 磁盘读写、SHA1 校验后落盘、单/多文件组装、预分配空间
- **CLI** — `bittorrent download` / `bittorrent seed` 两种模式，支持局域网 IP 检测
- **Seed 模式** — 从已有文件加载并校验后做种，响应 Peer 请求，持续运行

## 核心算法

1. **Kademlia DHT**（Go 侧）— 基于 XOR 度量的分布式哈希表，支持 `get_peers` 和 `announce_peer`，实现结点发现与组网
2. **Rarest-First Piece 选择** — 统计所有 Peer 的 piece 持有频次，优先下载副本最少的 piece，降低稀有 piece 丢失风险
3. **Tit-for-Tat 阻塞算法** — 每 10s 按上传量降序选择 4 个 unchoke slot，每 30s 随机乐观 unchoke 一个被阻塞的 Peer
4. **5-deep Pipeline** — 每个 Peer 同时维护最多 5 个 inflight block 请求，16KB/block，自动计算末尾 block 长度

## 当前效果

- **330+ 单元测试全通过**，覆盖 Bencode、Metainfo、Bitfield、Message、Handshake、Connection、EventLoop、Session、DHT、Storage 等模块
- **双机跨机器测试通过**：一个节点做种，另一个节点下载，文件 SHA256 校验一致
- **三机跨机器测试通过**：A 做种，B 和 C 可以下载；B 和 C 做种，A 可以进行并行下载
- **已知限制**：暂不支持 Tracker 发现；无断点续传；无前端；只能在局域网内部传输；

## 快速开始

```bash
# 1. 编译 Go DHT Sidecar
cd dht && go build -o dht-sidecar .

# 2. 编译 Rust 后端
cd backend && cargo build --release

# 3. 启动 DHT (终端 A)
./dht/dht-sidecar

# 4. 做种 (终端 B)
./backend/target/release/bittorrent seed file.torrent --data ./data --bind <your-ip>

# 5. 下载 (终端 C)
./backend/target/release/bittorrent download file.torrent --output ./downloads --bind <your-ip>
```

### References
Wikipedia, "BitTorrent," Wikipedia, The Free Encyclopedia, 2026. [Online]. Available: https://en.wikipedia.org/wiki/BitTorrent (accessed Jul. 24, 2026).
