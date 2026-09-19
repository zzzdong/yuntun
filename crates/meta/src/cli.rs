//! metanode 的命令行参数（S3-3 收尾）。
//!
//! # 全仓统一用 clap
//!
//! 早先这里是手写的（理由是"参数少"）。改回 clap 的原因：手工解析只覆盖了"拿值"，
//! 而 `--help`、拼错的 flag、缺值、`--flag=value` 这些**边角**得自己兜；
//! 更要紧的是全仓多个 CLI（`metanode` / `yuntun-cli` / `yuntun` / 压测示例）各写一套，
//! help 文案与错误形式迟早漂移 —— 统一到 clap 后这些是**同一套行为**。
//!
//! clap 管"**语法**"（哪些 flag 存在、类型对不对），本模块的 [`Args::normalize`] 与
//! [`Args::check_bootstrap`] 管"**语义**"（组合有没有意义、目录状态与意图是否匹配）。
//! 这个分工是刻意的：语义校验里的话（"否则集群缺一票"）比 clap 的错误更该由本 crate 说。
//!
//! # 两条"启动安全闸门"（见 [`Args::check_bootstrap`]）
//!
//! 参数合法不等于**意图**合法。两条最容易把运维带沟里的情形被显式拦住：
//!
//! | 情形 | 不拦会怎样 |
//! |---|---|
//! | 空目录 + 没给 `--init` | 你以为在"重启"，其实在**新建一个成员只有自己的集群**（旧集群的节点各自成组，数据永远合不回来） |
//! | 有数据的目录 + 给了 `--init` | 你以为在"初始化"，其实是在**已存在的成员表上启动**（行为完全不同） |

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::Parser;

/// metanode 启动参数。
#[derive(Parser, Debug, Clone, PartialEq, Eq)]
#[command(
    name = "metanode",
    version,
    about = "yuntun 元数据节点（raft 单节点；多节点复制待网络传输落地）",
    after_help = "示例:\n  \
                  首次启动:  metanode --id 1 --dir ./data/node1 --init\n  \
                  重启:      metanode --id 1 --dir ./data/node1\n  \
                  随机端口:  metanode --id 1 --dir ./data/node1 --listen 127.0.0.1:0"
)]
pub struct Args {
    /// 本节点 id（必须出现在 --voters 里）
    #[arg(long)]
    pub id: u64,

    /// 数据目录（每节点一个；首次必须配 --init）
    #[arg(long)]
    pub dir: PathBuf,

    /// gRPC 监听地址；用 :0 让内核分配（启动时会打印真实地址）
    #[arg(long, default_value = "127.0.0.1:9000")]
    pub listen: SocketAddr,

    /// 初始成员表（逗号分隔），默认只有自己
    #[arg(long, value_delimiter = ',')]
    pub voters: Vec<u64>,

    /// 首次启动：显式声明「这个目录是新集群」
    #[arg(long)]
    pub init: bool,
}

impl Args {
    /// 解析命令行（语法 + 语义），任何一项不合法都按**用法错**退出（码 2）。
    ///
    /// `clap` 的 `Error::exit()` 已经把两种情形分开了：`--help`/`--version` 退 0
    /// （它们不是错误），用法错退 2 ✓ 与 [`Args::check_bootstrap`] 的退出码一致。
    pub fn parse_checked() -> Self {
        match Self::try_parse() {
            Ok(a) => a.normalize().unwrap_or_else(|e| {
                eprintln!("参数错误：{e}");
                std::process::exit(2);
            }),
            Err(e) => e.exit(),
        }
    }

    /// 归一化 + 自洽校验：clap 管"参数写得对不对"，这里管"**参数组合有意不有意**"。
    pub fn normalize(mut self) -> Result<Self, String> {
        self.voters.sort_unstable();
        self.voters.dedup();
        if self.voters.is_empty() {
            // 单节点是默认形态（多节点成员表要网络传输，见 MetaNode::open）
            self.voters = vec![self.id];
        }
        if !self.voters.contains(&self.id) {
            // 成员表里没有自己 = 本节点永远当不上 leader 也投不出票（集群会缺一票）
            return Err(format!(
                "--voters {:?} 必须包含 --id {}（否则本节点在成员表外，集群缺一票）",
                self.voters, self.id
            ));
        }
        Ok(self)
    }

    /// **启动安全闸门**：把"参数合法"提升到"意图合法"。
    ///
    /// 返回 `Err` 时应当**拒绝启动**（而不是打个警告继续跑）：这两条搞错，
    /// 数据是**静默**对不上的（各自成组 / 意外加入），事后极难查。
    pub fn check_bootstrap(&self) -> Result<(), String> {
        match (dir_has_data(&self.dir), self.init) {
            (true, true) => Err(format!(
                "目录 {} 已有数据，但给了 --init。\n\
                 --init 是「新建集群」；对已有数据的目录使用它，会让人以为在初始化、\
                 实际是在已存在的成员表上启动（行为完全不同）。\n\
                 重启请去掉 --init；确实要重建请先手动清空目录（那等于**弃掉**这份元数据）。",
                self.dir.display()
            )),
            (false, false) => Err(format!(
                "目录 {} 为空（或不存在），但没给 --init。\n\
                 首次启动必须显式 --init：否则很容易把「新建一个只有自己的集群」\
                 当成「重启」—— 旧集群的节点各自成组后，数据永远合不回来。",
                self.dir.display()
            )),
            _ => Ok(()),
        }
    }
}

/// 目录里是否已有内容（不存在视为"没有数据"）。
fn dir_has_data(dir: &Path) -> bool {
    match std::fs::read_dir(dir) {
        Ok(mut it) => it.next().is_some(),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::error::ErrorKind;

    fn parse(args: &[&str]) -> Result<Args, clap::Error> {
        let mut v = vec!["metanode".to_string()];
        v.extend(args.iter().map(|s| s.to_string()));
        Args::try_parse_from(v)
    }

    /// 解析 + 归一化（成功路径）。
    fn run(args: &[&str]) -> Args {
        parse(args)
            .expect("应当解析成功")
            .normalize()
            .expect("应当通过语义校验")
    }

    #[test]
    fn minimal_args_default_voters_to_self() {
        let a = run(&["--id", "2", "--dir", "/tmp/x", "--init"]);
        assert_eq!(a.id, 2);
        assert_eq!(a.dir, PathBuf::from("/tmp/x"));
        assert_eq!(a.voters, vec![2], "默认成员表 = 只有自己");
        assert_eq!(a.listen.port(), 9000, "默认端口");
        assert!(a.init);
    }

    #[test]
    fn explicit_voters_are_sorted_and_deduped() {
        let a = run(&[
            "--id", "2", "--dir", "/tmp/x", "--voters", "3,1,2,2", "--listen", "0.0.0.0:7000",
        ]);
        assert_eq!(a.voters, vec![1, 2, 3], "成员表要归一化（顺序/重复都不该影响语义）");
        assert_eq!(a.listen.to_string(), "0.0.0.0:7000");
    }

    /// 成员表里没有自己 = 集群少一票（本节点投不出票、也当不上 leader）→ 必须拒绝。
    #[test]
    fn voters_must_contain_self() {
        let a = parse(&["--id", "9", "--dir", "/tmp/x", "--voters", "1,2,3"]).unwrap();
        let e = a.normalize().unwrap_err();
        assert!(e.contains("必须包含 --id 9"), "{e}");
    }

    /// 语法层交给 clap：缺参/拼错/类型错都必须**报错并指出是哪个参数**。
    #[test]
    fn syntax_errors_come_from_clap_and_name_the_argument() {
        // clap 的错误信息里必然带出问题的参数名（这是统一用 clap 的收益之一）
        for (args, needle) in [
            (vec!["--dir", "/tmp/x"], "--id"),
            (vec!["--id", "1"], "--dir"),
            (vec!["--id", "1", "--dir", "/tmp/x", "--wat"], "--wat"),
            (vec!["--id", "abc", "--dir", "/tmp/x"], "--id"),
            (vec!["--id", "1", "--dir", "/tmp/x", "--listen", "not-an-addr"], "--listen"),
            (vec!["--id", "1", "--dir", "/tmp/x", "--voters", "1,x"], "--voters"),
        ] {
            let e = parse(&args).unwrap_err();
            assert!(
                e.to_string().contains(needle),
                "args={args:?} 的错误信息里应含 {needle:?}，实际：{e}"
            );
        }
    }

    /// 缺必填参数是 clap 的 `MissingRequiredArgument`（不是"我们自己发现"）——
    /// 钉住它，避免以后有人把必填改成 `Option` + 手写检查（错误信息会悄悄变差）。
    #[test]
    fn missing_required_argument_has_the_right_kind() {
        assert_eq!(
            parse(&["--id", "1"]).unwrap_err().kind(),
            ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn help_is_not_an_error() {
        // clap 用 `DisplayHelp` 表示"这不是错"；调用方据此退 0
        assert_eq!(parse(&["--help"]).unwrap_err().kind(), ErrorKind::DisplayHelp);
        // `--version` 同理
        assert_eq!(parse(&["--version"]).unwrap_err().kind(), ErrorKind::DisplayVersion);
    }

    /// 两条安全闸门：空目录必须 `--init`；有数据的目录不许 `--init`。
    #[test]
    fn bootstrap_gates_reject_the_two_silent_disasters() {
        let tmp = std::env::temp_dir().join(format!(
            "yuntun-cli-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        // ① 不存在/空目录 + 没给 --init → 拒绝（否则会静默新建一个只有自己的集群）
        let a = run(&["--id", "1", "--dir", tmp.to_str().unwrap()]);
        let e = a.check_bootstrap().unwrap_err();
        assert!(e.contains("首次启动必须显式 --init"), "{e}");

        // ② 有数据的目录 + 给了 --init → 拒绝（否则会静默"加入"而不是"重建"）
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::write(tmp.join("CURRENT"), b"x").unwrap();
        let a = run(&["--id", "1", "--dir", tmp.to_str().unwrap(), "--init"]);
        let e = a.check_bootstrap().unwrap_err();
        assert!(e.contains("已有数据"), "{e}");

        // ③ 正确搭配都放行
        assert!(run(&["--id", "1", "--dir", tmp.to_str().unwrap()])
            .check_bootstrap()
            .is_ok());
        let empty = tmp.join("fresh");
        assert!(run(&["--id", "1", "--dir", empty.to_str().unwrap(), "--init"])
            .check_bootstrap()
            .is_ok());
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
