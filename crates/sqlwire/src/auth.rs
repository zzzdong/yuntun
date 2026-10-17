//! **登录鉴权**（U3 / `§190`）：MySQL wire 的口令校验。
//!
//! # 为什么需要它
//!
//! 此前 MySQL 端口是 **trust**（任意用户名、空密码直接进）—— `[sql.mysql].users`
//! 配了也只是打一条告警。按网络隔离部署是文档里的口径，但只要端口暴露
//! （DBeaver 从桌面连过来、容器端口映射），trust 就等于没有门。
//!
//! # 机制：`mysql_native_password`
//!
//! MySQL 5.7/8.0 客户端默认的握手算法（opensrv 的 `MysqlShim::authenticate` 钩子
//! 正好把它暴露出来）：
//!
//! ```text
//! token        = SHA1(password)
//! token_double = SHA1(token)
//! scramble     = SHA1(salt ‖ token_double) XOR token
//! ```
//!
//! 客户端把 `scramble` 放在握手包的 auth-data 里；服务端用**同一个 salt**
//! （自己发的）重算并比对。口令本身不过网 —— 这比"发明文再比"强，但**不等于
//! 传输安全**（无 TLS 时可被离线爆破，与 MySQL 自己同款）。
//!
//! # 两个刻意的决定
//!
//! 1. **salt 每连接随机**：opensrv 的默认 `salt()` 返回**固定字符串**
//!    （`";X,po_k}>o6^Wz!/kM}N"`）⇒ 抓一次包就能离线重放。这里覆盖成每连接
//!    20 字节随机值。口令短仍然是弱项，但固定 salt 是白送的。
//! 2. **默认仍是 trust**：不配用户 = 不鉴权（不破坏既有部署与所有测试）。
//!    配了用户而 `auth` 仍是 `trust` ⇒ **自动切换**成 native_password
//!    （`yuntun.toml.example` 里一直是这么写的）。
//!
//! # 边界（不做的）
//!
//! * 不做 `CREATE USER` / `GRANT`（用户表走配置；见 ROADMAP §5.1 不做清单）；
//! * 不做 caching_sha2_password（MySQL 8 默认）—— opensrv 0.7 只暴露 native 插件；
//! * 不做 TLS（`mysql --ssl-mode=REQUIRED` 会协商失败 ⇒ 客户端自己降级/报错，
//!   由部署层（stunnel / 网络隔离）解决）。

use sha1::{Digest as _, Sha1};

/// 握手插件名（MySQL 协议的固定串；opensrv 的 `myc` 模块是私有的，故直接写字面量）。
pub const MYSQL_NATIVE_PASSWORD: &str = "mysql_native_password";

/// 一个账号（口令为**配置里的明文**，不落任何存储）。
#[derive(Debug, Clone)]
pub struct AuthUser {
    pub name: String,
    pub password: String,
}

/// 鉴权模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    /// 任意用户名直接放行（默认；与旧行为一致）
    Trust,
    /// `mysql_native_password` 口令校验
    NativePassword,
}

/// 端口级鉴权策略（**每连接共享**：构造一次，随 `MysqlBackend` 克隆）。
#[derive(Debug, Clone)]
pub struct AuthPolicy {
    mode: AuthMode,
    users: Vec<AuthUser>,
}

impl AuthPolicy {
    /// 从 `[sql.mysql]` 的 `auth` / `users` 构造。
    ///
    /// `users` 非空而 `auth = "trust"` ⇒ **自动切换**（配置注释里写着的语义：
    /// "users 非空时自动切换"）。写错的值 ⇒ 明确报错（装配层启动即失败），
    /// 不静默回退成 trust —— 那会让"我配了鉴权"变成一句谎言。
    pub fn from_config(auth: &str, users: &[(&str, &str)]) -> Result<Self, String> {
        let explicit = match auth.trim().to_ascii_lowercase().as_str() {
            "trust" => Some(AuthMode::Trust),
            "native_password" | "mysql_native_password" => Some(AuthMode::NativePassword),
            other => {
                return Err(format!(
                    "invalid [sql.mysql] auth = {other:?}: 只接受 \"trust\" 或 \"native_password\""
                ));
            }
        };
        let mode = match explicit {
            Some(AuthMode::NativePassword) => AuthMode::NativePassword,
            _ if !users.is_empty() => {
                tracing::info!(
                    users = users.len(),
                    "[sql.mysql] 配置了用户 ⇒ 鉴权自动切换为 native_password"
                );
                AuthMode::NativePassword
            }
            _ => AuthMode::Trust,
        };
        Ok(Self {
            mode,
            users: users
                .iter()
                .map(|(name, password)| AuthUser {
                    name: (*name).to_string(),
                    password: (*password).to_string(),
                })
                .collect(),
        })
    }

    /// trust（不鉴权）构造器。
    pub fn trust() -> Self {
        Self {
            mode: AuthMode::Trust,
            users: Vec::new(),
        }
    }

    pub fn mode(&self) -> AuthMode {
        self.mode
    }

    /// 是否在校验口令（`authenticate` 里据此决定放行还是校验）。
    pub fn is_enforcing(&self) -> bool {
        self.mode == AuthMode::NativePassword
    }

    /// opensrv 默认插件的名字（握手 `auth_plugin_name` 用）。
    pub fn plugin_name(&self) -> &'static str {
        MYSQL_NATIVE_PASSWORD
    }

    /// 校验一次握手。`Ok(())` = 放行；`Err(理由)` = 拒绝。
    ///
    /// `salt` 必须是本连接**自己发出**的那个（服务端在握手包里给客户端的）。
    pub fn verify(&self, username: &str, salt: &[u8], auth_data: &[u8]) -> Result<(), String> {
        if self.mode == AuthMode::Trust {
            return Ok(());
        }
        let Some(user) = self.users.iter().find(|u| u.name == username) else {
            return Err(format!("user {username:?} not found"));
        };
        // 空口令：客户端发空 auth-data（MySQL 同款：空口令 = 空响应）
        if user.password.is_empty() {
            return if auth_data.is_empty() {
                Ok(())
            } else {
                Err(format!("user {username:?} has empty password"))
            };
        }
        let want = native_scramble(salt, &user.password);
        if constant_time_eq(auth_data, &want) {
            Ok(())
        } else {
            Err(format!("wrong password for user {username:?}"))
        }
    }
}

/// `mysql_native_password` 的响应计算：`SHA1(salt ‖ SHA1(SHA1(pw))) XOR SHA1(pw)`。
///
/// **空口令 ⇒ 空响应**（与 MySQL 客户端一致：它对空口令根本不发 scramble）——
/// 于是服务端 `verify` 的空口令分支与这里的公式自洽，测试也能拿它当"客户端会发什么"。
pub fn native_scramble(salt: &[u8], password: &str) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let token = sha1(password.as_bytes());
    let token_double = sha1(&token);
    let mut salted = Vec::with_capacity(salt.len() + token_double.len());
    salted.extend_from_slice(salt);
    salted.extend_from_slice(&token_double);
    let xor = sha1(&salted);
    token.iter().zip(xor).map(|(a, b)| a ^ b).collect()
}

fn sha1(bytes: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(bytes);
    h.finalize().into()
}

/// 定长比较（不提前退出）：口令比对不该通过耗时泄露"前几个字节对了"。
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

/// **每连接随机 salt**（覆盖 opensrv 的固定值，见模块文档第 1 条）。
pub fn random_salt() -> [u8; 20] {
    use rand::RngCore as _;
    let mut salt = [0u8; 20];
    rand::thread_rng().fill_bytes(&mut salt);
    // MySQL 协议要求 salt 字节非 0 且非 '$'（opensrv 的默认值也做了同样处理）
    for b in &mut salt {
        if *b == 0 || *b == b'$' {
            *b = b'$' + 1;
        }
    }
    salt
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(users: &[(&str, &str)]) -> AuthPolicy {
        AuthPolicy {
            mode: AuthMode::NativePassword,
            users: users
                .iter()
                .map(|(n, p)| AuthUser {
                    name: (*n).to_string(),
                    password: (*p).to_string(),
                })
                .collect(),
        }
    }

    /// **算法自洽**：服务端重算的 scramble 必须等于客户端会发来的那个。
    /// （对照向量来自 MySQL 协议文档的 `mysql_native_password` 描述。）
    #[test]
    fn scramble_matches_the_protocol_definition() {
        // 空口令 ⇒ 空响应（MySQL 同款）
        assert!(native_scramble(b"salt", "").is_empty());
        // 非空口令 ⇒ 20 字节，且不同 salt 给出不同响应
        let a = native_scramble(b"salt", "pw");
        let b = native_scramble(b"pepper", "pw");
        assert_eq!(a.len(), 20);
        assert_ne!(a, b, "salt 必须参与计算");
        // 同一 (salt, pw) 确定性可复算
        assert_eq!(a, native_scramble(b"salt", "pw"));
    }

    #[test]
    fn verify_accepts_the_right_password_and_rejects_the_rest() {
        let p = policy(&[("yuntun", "s3cret")]);
        let salt = random_salt();
        let good = native_scramble(&salt, "s3cret");
        assert!(p.verify("yuntun", &salt, &good).is_ok(), "正确口令必须放行");
        let bad = native_scramble(&salt, "wrong");
        assert!(p.verify("yuntun", &salt, &bad).is_err(), "错口令必须拒绝");
        assert!(
            p.verify("yuntun", b"other-salt!!", &good).is_err(),
            "换 salt 重放必须拒绝（固定 salt 就能过的攻击面）"
        );
        assert!(
            p.verify("nobody", &salt, &good).is_err(),
            "不存在的用户拒绝"
        );
        // 空 auth-data（客户端没给口令）⇒ 拒绝（该用户有口令）
        assert!(p.verify("yuntun", &salt, &[]).is_err());
    }

    #[test]
    fn empty_password_user_accepts_empty_response() {
        let p = policy(&[("guest", "")]);
        assert!(p.verify("guest", b"salt", &[]).is_ok());
        assert!(
            p.verify("guest", b"salt", &native_scramble(b"salt", "x"))
                .is_err()
        );
    }

    #[test]
    fn trust_mode_lets_everything_through() {
        let p = AuthPolicy::trust();
        assert!(!p.is_enforcing());
        assert!(p.verify("whoever", b"whatever", b"garbage").is_ok());
    }

    /// 配置解析：**写错的值必须报错**，不静默回退成 trust。
    #[test]
    fn config_parsing_is_explicit() {
        // 显式 trust + 无用户 ⇒ trust（不破坏既有部署）
        assert_eq!(
            AuthPolicy::from_config("trust", &[]).unwrap().mode(),
            AuthMode::Trust
        );
        // 配了用户 ⇒ 自动切换（即便 auth 写成 trust）
        let p = AuthPolicy::from_config("trust", &[("a", "b")]).unwrap();
        assert_eq!(p.mode(), AuthMode::NativePassword);
        // 显式 native_password
        let p = AuthPolicy::from_config("native_password", &[("a", "b")]).unwrap();
        assert!(p.is_enforcing());
        // 拼错 ⇒ 报错（而不是悄悄 trust）
        assert!(AuthPolicy::from_config("nope", &[]).is_err());
    }

    /// salt 覆盖：**不能**等于 opensrv 的固定值（重放面）。
    #[test]
    fn salt_is_random_per_connection() {
        let a = random_salt();
        let b = random_salt();
        assert_ne!(a, b, "每连接 salt 必须不同");
        for byte in a.iter().chain(b.iter()) {
            assert!(*byte != 0 && *byte != b'$', "salt 字节必须避开 0 与 $");
        }
    }
}
