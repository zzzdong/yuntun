//! **冷文件的分配**（`§178`，N4b / 归档 `S5-2` 的第一刀）。
//!
//! # 这一刀只落"分配"本身
//!
//! 冷文件在**共享**对象存储上 —— 任何节点都能读。所以"谁来读"是一个**调度**问题，
//! 而不是"数据在哪"的问题（热数据才是后者：chunk/WAL 在本地，**只能**由持有者读）。
//!
//! 这一刀先把"分给谁"变成一个**可测的纯函数**，并把"这一支只读分配给它的文件"做成接缝
//! （`ReadPath::only_files`，见 `table.rs`）。**执行**仍然在协调者本地 ——
//! 真正把读取搬走（远端冷读）是下一刀，它只需要在[`Assignment`]上再挂一个 RPC。
//!
//! # 今天的规则（刻意简单，三条）
//!
//! 1. **确定性**：节点先排序去重；文件按 `(大小降序, 路径升序)` 排队 ⇒ **同一输入同一输出**
//!    （分布式系统里"分配不可复现"会让问题**无法复现**，那比慢更糟）。
//! 2. **均衡**：贪心 **LPT**（每次把最大的文件给当前累计最小的节点）。
//!    理由：文件大小差异极大（合并产物 vs 刚 flush 的批），按字节贪心比"按个数轮流"稳得多。
//! 3. **完整**：每个文件**恰好**分给一个节点 —— 漏一个 = 少数据，重一个 = 双计。
//!
//! # 没做的（下一刀才谈得上）
//!
//! - **动态负载均衡**：今天**没有可用的负载信号**（datanode 侧还没有 `/metrics`，
//!   在途查询数也没上报）⇒ 只能静态分配。要按负载分，得先有信号。
//! - **亲和性**（让"刚写过这批文件的节点"读它们，省一次跨节点拉取）：需要
//!   `source_instance` 的语义确认（它今天是**热副本**归属，**不是**文件 owner）。
//! - **失败重分配**：拿到[`Assignment`]之后谁读不到就回退本地读（下一刀的降级语义）。

use std::collections::BTreeMap;

use yuntun_model::meta::FileManifest;

/// 一个节点被分到的那部分冷文件。
///
/// 只派生 （ 里有统计字段，不是 ）—— 判据比的是内容与分配。
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    /// 实例 id。**空串 = 本地**（协调者自己读 —— 它也是"一个节点"，分它一份才不必多跳一次）。
    pub instance: String,
    pub files: Vec<FileManifest>,
    /// 这份的累计字节（= `files` 的 `file_size` 之和；用于判据与观测）
    pub bytes: u64,
}

/// 把 `files` 分给 `nodes`；返回按**实例 id 升序**排好的分配。
///
/// ⚠️ **调用方要把"自己"也放进 `nodes`**（协调者也是一个能读冷文件的节点，
/// 分它一份才不必多跳一次）；传**空列表**表示"全部留给本地"。
/// 空实例 id（`""`）约定为**本地**，与 `nodes` 里的其它成员一样参与均衡。
///
/// - `nodes` 空 ⇒ 全给本地（空实例 id）；
/// - `files` 空 ⇒ 每个节点一条"空分配"（调用方因此不必区分"没分到"与"没节点"）。
pub fn assign(files: &[FileManifest], nodes: &[String]) -> Vec<Assignment> {
    // 排序 + 去重（确定性纪律 2：不要依赖传入顺序）
    let mut nodes: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    nodes.sort_unstable();
    nodes.dedup();

    let mut out: BTreeMap<String, Assignment> = if nodes.is_empty() {
        BTreeMap::from([(
            String::new(),
            Assignment {
                instance: String::new(),
                files: Vec::new(),
                bytes: 0,
            },
        )])
    } else {
        nodes
            .iter()
            .map(|n| {
                (
                    n.to_string(),
                    Assignment {
                        instance: n.to_string(),
                        files: Vec::new(),
                        bytes: 0,
                    },
                )
            })
            .collect()
    };

    // LPT：大的先分 ⇒ 尾巴上的小文件用来"找平"
    let mut queue: Vec<&FileManifest> = files.iter().collect();
    queue.sort_by(|a, b| {
        b.file_size
            .cmp(&a.file_size)
            .then_with(|| a.file_path.cmp(&b.file_path))
    });

    for f in queue {
        // 挑"当前累计最小"的节点；**并列时取实例 id 小的** ⇒ 消除 Map 迭代顺序的影响
        let pick = out
            .iter()
            .min_by(|(ka, a), (kb, b)| a.bytes.cmp(&b.bytes).then_with(|| ka.cmp(kb)))
            .map(|(k, _)| k.clone())
            .expect("out 至少有一条（nodes 空时给了本地）");
        let slot = out.get_mut(&pick).expect("刚挑出来的键必然存在");
        // 0 字节的文件也按 1 算：否则一批空文件会让所有节点的 load 都是 0，
        // 于是"最小"变成纯粹的字典序（结果是确定的，但**分布**会退化成全给第一个节点）
        slot.bytes += f.file_size.max(1);
        slot.files.push(f.clone());
    }

    out.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, size: u64) -> FileManifest {
        FileManifest {
            file_path: path.into(),
            batch_id: path.into(),
            file_size: size,
            ..Default::default()
        }
    }

    fn paths(a: &Assignment) -> Vec<String> {
        a.files.iter().map(|f| f.file_path.clone()).collect()
    }

    /// **完整**：每个文件恰好一次；**确定**：两次调用完全相同。
    #[test]
    fn assign_is_complete_and_deterministic() {
        let files = vec![
            file("p/a", 10),
            file("p/b", 1),
            file("p/c", 7),
            file("p/d", 3),
        ];
        let nodes = vec!["dn2".into(), "dn1".into()];
        let got = assign(&files, &nodes);
        let again = assign(&files, &nodes);
        assert_eq!(got, again, "同一输入必须给出同一分配");

        let all: Vec<String> = got.iter().flat_map(paths).collect();
        let mut sorted = all.clone();
        sorted.sort();
        assert_eq!(sorted, vec!["p/a", "p/b", "p/c", "p/d"], "一个都不能漏/重");
        // 按实例 id 升序 ⇒ 结果可对照
        assert_eq!(
            got.iter().map(|a| a.instance.clone()).collect::<Vec<_>>(),
            vec!["dn1", "dn2"]
        );
    }

    /// LPT：最大的先分，然后由小文件找平（大小差异大时明显比"按个数轮流"稳）。
    #[test]
    fn assign_follows_longest_processing_time_first() {
        let files = vec![
            file("p/a", 9),
            file("p/b", 7),
            file("p/c", 3),
            file("p/d", 1),
        ];
        let nodes = vec!["dn1".into(), "dn2".into(), "dn3".into()];
        let got = assign(&files, &nodes);
        // 每一步都把**当前最大**的文件给**当前累计最小**的节点：
        // 9 → dn1(9)；7 → dn2(7)；3 → dn3(3)；1 → 最小的是 dn3(3) ⇒ dn3 = 3+1
        // （这里我一开始写错了期望：以为 1 会给 dn2。用例报出的是 3+1 —— 算法是对的。）
        let got: Vec<_> = got.iter().map(|a| (a.instance.clone(), paths(a))).collect();
        assert_eq!(
            got,
            vec![
                ("dn1".to_string(), vec!["p/a".to_string()]),
                ("dn2".to_string(), vec!["p/b".to_string()]),
                (
                    "dn3".to_string(),
                    vec!["p/c".to_string(), "p/d".to_string()]
                ),
            ]
        );
    }

    /// 没有节点 ⇒ 全给本地（空实例 id）—— 协调者自己读，不必多跳一次。
    #[test]
    fn assign_with_no_nodes_keeps_everything_local() {
        let files = vec![file("p/a", 5), file("p/b", 2)];
        let got = assign(&files, &[]);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].instance, "");
        assert_eq!(paths(&got[0]), vec!["p/a", "p/b"]);
        assert_eq!(got[0].bytes, 7);
    }

    #[test]
    fn assign_with_one_node_takes_everything() {
        let files = vec![file("p/a", 5), file("p/b", 2)];
        let got = assign(&files, &["dn1".into()]);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].instance, "dn1");
        assert_eq!(paths(&got[0]), vec!["p/a", "p/b"]);
    }

    /// 没有文件时**每个节点一条空分配**（调用方不必区分"没分到"与"没节点"）。
    #[test]
    fn assign_with_no_files_keeps_every_node_listed() {
        let got = assign(&[], &["dn1".into(), "dn2".into()]);
        let got: Vec<_> = got
            .iter()
            .map(|a| (a.instance.clone(), a.files.len()))
            .collect();
        assert_eq!(got, vec![("dn1".to_string(), 0), ("dn2".to_string(), 0)]);
    }

    /// 节点顺序与重复都不影响结果（否则"分配"会随调用点漂移）。
    #[test]
    fn assign_ignores_node_order_and_duplicates() {
        let files = vec![file("p/a", 3), file("p/b", 2)];
        let a = assign(&files, &["dn2".into(), "dn1".into()]);
        let b = assign(&files, &["dn1".into(), "dn2".into(), "dn1".into()]);
        assert_eq!(a, b);
    }

    /// 大小相同 ⇒ 由**路径**决定（不是由输入顺序，也不是由 Map 迭代顺序）。
    #[test]
    fn ties_are_broken_by_path_not_by_input_order() {
        let f1 = vec![file("p/a", 1), file("p/b", 1), file("p/c", 1)];
        let f2 = vec![file("p/c", 1), file("p/b", 1), file("p/a", 1)];
        assert_eq!(
            assign(&f1, &["dn1".into(), "dn2".into()]),
            assign(&f2, &["dn1".into(), "dn2".into()])
        );
    }
}
