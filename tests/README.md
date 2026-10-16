# tests/ —— 容器化真集群 rig

这里放的是**跨进程 / 跨容器**的验证（要真网络、真断网、真多节点），
与 `crates/**/tests/`（进程内单测 / 集成）互补：单测判"逻辑对"，
本目录判"装起来之后仍然对"。

| 文件 | 用途 | 前置 |
|---|---|---|
| `cluster.sh` | **三节点 metanode 容器化真集群**（podman-compose + `debian:trixie-slim`）：真网络命名空间 / 真断网（`podman network disconnect`）/ 接回后**收敛硬断言**。用法见脚本头部（`build / up / probe / partition / heal / smoke / logs / ps / down`） | podman + podman-compose；镜像须 glibc ≥ 2.44（台账 `D-22`） |
| `datanode_cluster.sh` | **3 metanode + 2 datanode** rig：两个数据进程都真的合并过 + 行数守恒 + `gc` 单持有 + 杀掉干活的那台另一台接手 | 同上 |
| `compose.yaml` / `datanode-compose.yaml` | 上述两套 rig 的编排 | — |
| `Containerfile` | rig 镜像定义（宿主 `cargo build` 出的二进制 + 运行底座） | — |

配套的极小 Meta 客户端：`cargo run -p yuntun-meta --example meta_probe`
（`tasks` / `leases` 子命令看任务目录与租约持有者）。

**单机（standalone）不在这里**：它不需要容器，一条命令即可 ——
`scripts/standalone_smoke.py`（见根 `README.md §5`）对着
`./target/release/yuntun --config yuntun.toml` 跑全端口冒烟。
