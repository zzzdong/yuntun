//! **成员变更的运维面**（`§128`）：把 `Meta.Promote` / `Meta.Remove` / `Meta.Status` 包成
//! "给一组**接触点**、自己找到能受理的那个"的三个动作。
//!
//! # 为什么要"接触点列表"而不是单个地址
//!
//! 只有 leader 受理成员变更，而非 leader 回的是 **`NotLeader` + hint，而 hint 是个 id 不是地址**
//! —— 调用方（运维 / 脚本）手里只有地址。所以接触点是**列表**，逐个试到有人受理为止
//! （与 `metanode --join`、`bench --meta` 同一套做法）。
//!
//! # 一条纪律：**只对"没人受理"换下一个，绝不把真拒绝藏起来**
//!
//! * **没人受理**（非 leader，走约定 3 映射成 `UNAVAILABLE`；或连不上/超时）⇒ 试下一个；
//! * **真拒绝**（如"不能摘掉最后一个 voter"、"节点不在成员表里"）⇒ **原样上抛**。
//!
//! 判据用**状态码**（[`is_retryable`]），**不是错误文本** —— 文本会变，码不会。把这条判据
//! 弄错两个方向都很糟：该换节点的干等，不该换节点的会把真拒绝藏成"再试试别的"。
//!
//! # 为什么错误用 `String`（而不是像服务端那样分类型）
//!
//! 这层的唯一消费者是 CLI 与脚本，它们要的是**给人看的一句话**。服务端那层该类型化，是因为
//! 客户端要按类型**分流重试**；这里没有分流，包个类型只会在 `main` 里立刻 `to_string()`。

use std::time::Duration;

use tonic::Code;
use yuntun_proto::meta as pb;
use yuntun_proto::meta::meta_client::MetaClient;

/// 成员表（`voters` / `learners`）—— 服务端 `Status` / `Promote` / `Remove` 都回它。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberSet {
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    /// 谁是 leader（`0` = 还不知道）。只有 `Status` 答得出来 —— `Promote`/`Remove` 的回包
    /// 里没有这个字段，所以它是 `Option`：**不假装有**。
    ///
    /// 它对运维有两个用处：① `members` 直接报出"现在谁是 leader"（排障第一问）；
    /// ② 脚本可以用它判断"集群是否还没选出 leader"（此时 `Promote`/`Remove` 一定失败）。
    pub leader_id: Option<u64>,
}

impl std::fmt::Display for MemberSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "voters={:?} learners={:?}", self.voters, self.learners)?;
        match self.leader_id {
            Some(0) | None => Ok(()),
            Some(l) => write!(f, " leader={l}"),
        }
    }
}

/// 要做的动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminAction {
    /// 看成员表（**任何**活着的成员都答得出来，不必是 leader）。
    Members,
    /// 把 learner 提升为 voter（只 leader 受理）。
    Promote(u64),
    /// 把一个成员移除（只 leader 受理；幂等）。
    Remove(u64),
}

fn member_set(voters: Vec<u64>, learners: Vec<u64>) -> MemberSet {
    MemberSet {
        voters,
        learners,
        leader_id: None,
    }
}

/// 这条错误是不是"**没人受理**"（⇒ 值得换个接触点再试）。
///
/// 单独抽出来是为了**能测它**（它决定上一条纪律的方向）。
pub fn is_retryable(status: &tonic::Status) -> bool {
    matches!(status.code(), Code::Unavailable)
}

/// 在一组接触点里找到能受理的那个并执行 `action`。
///
/// 返回 `(成员表, 受理的接触点)` —— 把**用了哪个**也返回，因为运维排障第一句要问的就是
/// "这话是谁说的"。
pub async fn run(
    action: AdminAction,
    contacts: &[String],
    timeout: Duration,
) -> Result<(MemberSet, String), String> {
    if contacts.is_empty() {
        return Err("没有接触点：用 --meta <host:port>[,<host:port>…] 给至少一个".into());
    }
    let mut tried: Vec<String> = Vec::new();

    for c in contacts {
        let ep = if c.starts_with("http") {
            c.clone()
        } else {
            format!("http://{c}")
        };
        let mut client = match tokio::time::timeout(timeout, MetaClient::connect(ep)).await {
            Ok(Ok(cli)) => cli,
            Ok(Err(e)) => {
                tried.push(format!("{c}: 连不上（{e}）"));
                continue;
            }
            Err(_) => {
                tried.push(format!("{c}: 连接超时"));
                continue;
            }
        };

        // 一次调用：超时与"没人受理"都算**这个接触点不行**（换下一个），其余是真拒绝。
        let outcome: Result<MemberSet, tonic::Status> = match action {
            AdminAction::Members => {
                match tokio::time::timeout(timeout, client.status(pb::StatusRequest {})).await {
                    Ok(x) => x.map(|r| {
                        let s = r.into_inner();
                        MemberSet {
                            voters: s.voter_ids,
                            learners: s.learner_ids,
                            leader_id: Some(s.leader_id),
                        }
                    }),
                    Err(_) => {
                        tried.push(format!("{c}: Status 超时"));
                        continue;
                    }
                }
            }
            AdminAction::Promote(node_id) => {
                match tokio::time::timeout(
                    timeout,
                    client.promote(pb::PromoteRequest { node_id }),
                )
                .await
                {
                    Ok(x) => x.map(|r| {
                        let s = r.into_inner();
                        member_set(s.voter_ids, s.learner_ids)
                    }),
                    Err(_) => {
                        tried.push(format!("{c}: Promote 超时"));
                        continue;
                    }
                }
            }
            AdminAction::Remove(node_id) => {
                match tokio::time::timeout(timeout, client.remove(pb::RemoveRequest { node_id }))
                    .await
                {
                    Ok(x) => x.map(|r| {
                        let s = r.into_inner();
                        member_set(s.voter_ids, s.learner_ids)
                    }),
                    Err(_) => {
                        tried.push(format!("{c}: Remove 超时"));
                        continue;
                    }
                }
            }
        };

        match outcome {
            Ok(ms) => return Ok((ms, c.clone())),
            Err(st) if is_retryable(&st) => {
                tried.push(format!("{c}: 没人受理（{} {}）", st.code(), st.message()));
            }
            // 真拒绝：原样上抛，别藏
            Err(st) => return Err(format!("服务端拒绝（{c}）：{}", st.message())),
        }
    }

    Err(format!(
        "没有任何接触点受理（都答 NotLeader，或连不上/超时）：\n  {}",
        tried.join("\n  ")
    ))
}
