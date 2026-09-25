# tests/ —— 容器化的多节点测试

这里放**用容器跑真集群**的测试，与 `crates/*/tests/` 的进程内用例互补。

为什么要有它：进程内夹具给不了"**真网络命名空间 + 真断网 + 对端换 IP**"这三样，而它们恰好是
多节点真部署最容易出事的地方 —— `§108` 抓到的第一个真 bug（**对端换 IP 后 leader 再也送不到**）
就是进程内夹具**测不到**的。

## 跑法

```bash
cargo build -p yuntun-meta --bin metanode --example meta_probe   # 宿主编译（容器里不编，见下）
bash tests/cluster.sh smoke     # 起三节点 → 真断网 → 多数派仍写 → 接回**必须收敛**
```

常用子命令：

```bash
bash tests/cluster.sh up                    # 起（每次都是干净的一份，卷一起重建）
bash tests/cluster.sh ps / logs [1|2|3]     # 看状态 / 看某台日志
bash tests/cluster.sh probe status mn1:9311 mn2:9311 mn3:9311
bash tests/cluster.sh probe write k9 mn1:9311 mn2:9311 mn3:9311
bash tests/cluster.sh partition 3           # podman network disconnect —— **真断网**
bash tests/cluster.sh heal 3                # 接回（**必须带 --alias**，否则别的节点解析不到它）
bash tests/cluster.sh down                  # 连卷一起清掉（下次 up 才会重新 --init）
```

出错时容器**不会被清掉**（脚本会把三个容器的日志尾打出来），现场可以继续查。

## 按需开轨迹

默认安静。要逐条消息轨迹 / raft 内部判定：

```bash
YUNTUN_META_TRACE=1 RUST_LOG=raft=debug bash tests/cluster.sh up
```

⚠️ **别默认打开**：轨迹是"每条消息一行 `eprintln!`"，真实集群的 stdout 管道会被塞满、进程被
**写阻塞**，集群反而起不来（`§108.4` 实测踩过）。所以 `trace_on()` 的判据是"非空且不是 `0`"
（编排里常见的 `- VAR=${VAR:-}` 会给出"已设置但为空"，用 `is_ok()` 判会误开）。

## 两个设计选择

- **不在容器里编译**：镜像就用本机已有的 `debian:trixie-slim`，二进制由宿主 cargo 编好挂进去
  （实测宿主 glibc 2.44 编出的二进制在 2.41 的镜像里能直接跑，无缺失符号）。20 个 crate 在容器里
  编一遍又慢又要拉 crates.io，而这里要验的是**运行期**行为。
- **地址用容器名**（`--peer 2@mn2:9311`）：名字在编排时就固定 ⇒ 顺手满足 `§103` 的"对端地址要
  在启动前知道"；而且容器重新接入换 IP 时**只有名字是对的**（这正是 `§108` 那个 bug 的另一半）。

## 依赖与定位

需要 podman + podman-compose + 一个 `debian:trixie-slim` 镜像 ⇒ **需要外部环境，不进 CI**
（与 `scripts/s3_*_smoke.sh` 同属"手动 / 按需"档）。
