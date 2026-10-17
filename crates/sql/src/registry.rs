//! **查询注册表 / 取消**（U4 / `§191`）：`SHOW PROCESSLIST` 与 `KILL` 的数据源。
//!
//! # 为什么需要它
//!
//! 此前 `SHOW PROCESSLIST` / `KILL` / `SHOW STATUS` 一律报 1149（"只支持 SELECT/…"），
//! 全仓也**没有**任何 in-flight 查询表 —— 运维看不到"谁在跑什么"，也无法取消一条
//! 跑飞了的大查询。对一个"单机即完整列数据库"来说，这是功能面最后的硬缺口
//! （`ROADMAP §5.2.1` 的 U4）。
//!
//! # 形状：会话表 + 每查询一个可取消段
//!
//! 借 MySQL 的语义：**Id 是连接**，一条连接上同一时刻最多一个查询。
//!
//! ```text
//! ConnectionGuard（Drop = 连接断开）
//!   └─ Session { id, user, host, db, running: Option<Running>, cancel, kill_connection }
//!        └─ QueryGuard（Drop = 查询结束，回到 Sleep）
//! ```
//!
//! * `begin()` 把会话标成 `Query` 并**重置**取消位；`QueryGuard::drop` 标回 `Sleep`；
//! * `kill(id, QUERY)` 点取消位 —— 引擎侧 `select!` / 流包装器看见它就返回
//!   [`crate::SqlError::Cancelled`]（MySQL 1317）；
//! * `kill(id, CONNECTION)` 除取消外还立一个旗标，由协议层在本次响应后断开连接
//!   （`sqlwire` 侧检查 —— 注册表不该知道 socket）。
//!
//! # 刻意的边界
//!
//! * **不做并发闸门**（`max_connections` / 最大并发查询）：那是另一件事（准入），
//!   与"看见并取消"不同；本模块只提供事实，不拒绝任何人（`ROADMAP` U4 的下一刀）。
//! * **Flight 轨没有连接概念**：每次查询建一条**临时会话**（`host = "flight"`），
//!   查询结束即消失。它在 `SHOW PROCESSLIST` 里只是一行，符合"一次查询一个 Id"。
//! * 取消是**协作式**的：只能打断注册表看得见的执行段（引擎的 `select!` 与流包装器）。
//!   已经交给客户端的批次不会被"撤回"。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// 一条 `SHOW PROCESSLIST` 行（`snapshot()` 在**取快照那一刻**算好时间）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessRow {
    pub id: u64,
    pub user: String,
    pub host: String,
    pub db: String,
    /// `Query`（正在执行）或 `Sleep`（空闲）
    pub command: &'static str,
    /// 当前状态已持续秒数（`Query` = 执行时长；`Sleep` = 空闲时长）
    pub time_secs: i64,
    /// MySQL 把 `State` 留给"正在做什么"；本实现没有更细的阶段 ⇒ 恒为空
    pub state: String,
    /// 正在执行的 SQL（`Sleep` 时为空）
    pub info: String,
}

/// 取消位：`AtomicBool` + `Notify` 的组合。
///
/// 只用一个 `Notify` 会在"取消发生在等待之前"时丢通知；只轮询 `AtomicBool` 又要
/// 定时器。两者合起来：先看旗标、再等通知、醒后再看一次（经典的 lost-wakeup 消解）。
#[derive(Default)]
pub struct Cancellation {
    flag: AtomicBool,
    notify: tokio::sync::Notify,
}

impl Cancellation {
    pub fn cancel(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub fn reset(&self) {
        self.flag.store(false, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    /// 等到被取消（已取消时立即返回）。
    pub async fn cancelled(&self) {
        loop {
            if self.is_cancelled() {
                return;
            }
            let notified = self.notify.notified();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

/// 正在执行的段。
struct Running {
    sql: String,
    started: Instant,
}

/// 一条连接（或一次临时查询）的可见状态。
pub struct Session {
    pub id: u64,
    pub host: String,
    user: Mutex<String>,
    db: Mutex<String>,
    running: Mutex<Option<Running>>,
    /// 上一次活动结束的时刻（`Sleep` 的 `Time` 从这里算）
    idle_since: Mutex<Instant>,
    cancel: Cancellation,
    kill_connection: AtomicBool,
    /// **一次查询一条**的临时会话（Flight 轨）：`Threads_connected` / `Connections`
    /// 不该把它算成客户端连接。
    ephemeral: bool,
}

impl Session {
    pub fn user(&self) -> String {
        self.user_guard().clone()
    }

    fn user_guard(&self) -> std::sync::MutexGuard<'_, String> {
        // 毒锁 = 某个持锁线程 panic；这里的数据是"诊断信息"，不因污染而不可用
        self.user.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn db_guard(&self) -> std::sync::MutexGuard<'_, String> {
        self.db.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn running_guard(&self) -> std::sync::MutexGuard<'_, Option<Running>> {
        self.running.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn idle_guard(&self) -> std::sync::MutexGuard<'_, Instant> {
        self.idle_since.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `KILL CONNECTION` 已受理（协议层看到后断开）。
    pub fn kill_requested(&self) -> bool {
        self.kill_connection.load(Ordering::SeqCst)
    }

    fn row(&self) -> ProcessRow {
        let now = Instant::now();
        if let Some(running) = self.running_guard().as_ref() {
            ProcessRow {
                id: self.id,
                user: self.user(),
                host: self.host.clone(),
                db: self.db_guard().clone(),
                command: "Query",
                time_secs: secs(now.duration_since(running.started)),
                state: String::new(),
                info: running.sql.clone(),
            }
        } else {
            ProcessRow {
                id: self.id,
                user: self.user(),
                host: self.host.clone(),
                db: self.db_guard().clone(),
                command: "Sleep",
                time_secs: secs(now.duration_since(*self.idle_guard())),
                state: String::new(),
                info: String::new(),
            }
        }
    }
}

fn secs(d: Duration) -> i64 {
    d.as_secs().min(i64::MAX as u64) as i64
}

/// 一次连接的句柄：**登记它的那一份 Drop 才从表里消失**（连接断开）。
///
/// `lookup()` 会为同一个会话再造一份句柄（引擎把查询挂到连接上）—— 那份
/// **不能**在查询结束时就注销会话（否则第一条查询之后连接就从进程列表里消失，
/// 下一条查询又变成一个新 Id）。所以用 `registered` 区分"登记者"与"借用者"。
pub struct ConnectionGuard {
    registry: Arc<QueryRegistry>,
    session: Arc<Session>,
    registered: bool,
}

impl ConnectionGuard {
    pub fn id(&self) -> u64 {
        self.session.id
    }

    /// 握手成功后回填用户名（`SHOW PROCESSLIST` 的 `User` 列）。
    pub fn set_user(&self, user: &str) {
        *self.session.user_guard() = user.to_string();
    }

    /// 开始一条查询：标记 `Query`、重置取消位、返回随查询结束而自清的句柄。
    ///
    /// `db` 用当前会话 schema（`USE` 之后 `SHOW PROCESSLIST` 的 `db` 列应跟着变）。
    pub fn begin(self: &Arc<Self>, sql: &str, db: &str) -> QueryGuard {
        *self.session.db_guard() = db.to_string();
        self.session.cancel.reset();
        *self.session.running_guard() = Some(Running {
            sql: sql.to_string(),
            started: Instant::now(),
        });
        QueryGuard {
            conn: Arc::clone(self),
        }
    }

    /// 这条连接是否已被 `KILL CONNECTION` 点名（协议层据此断开）。
    pub fn kill_requested(&self) -> bool {
        self.session.kill_requested()
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        // 借用句柄（`lookup`）不注销会话 —— 它只活一条查询，连接还在。
        if !self.registered {
            return;
        }
        let mut map = self
            .registry
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // 只删自己（id 唯一；这里再比一次 Arc 是防御性写法）
        if map
            .get(&self.session.id)
            .is_some_and(|s| Arc::ptr_eq(s, &self.session))
        {
            map.remove(&self.session.id);
        }
    }
}

/// 一次执行段的句柄：`Drop` = 查询结束（回到 `Sleep`）。
pub struct QueryGuard {
    conn: Arc<ConnectionGuard>,
}

impl QueryGuard {
    pub fn is_cancelled(&self) -> bool {
        self.conn.session.cancel.is_cancelled()
    }

    pub async fn cancelled(&self) {
        self.conn.session.cancel.cancelled().await
    }

    /// `KILL CONNECTION` 的标志（查询被取消后，协议层还要断开连接）。
    pub fn kill_requested(&self) -> bool {
        self.conn.session.kill_requested()
    }
}

impl Drop for QueryGuard {
    fn drop(&mut self) {
        *self.conn.session.running_guard() = None;
        *self.conn.session.idle_guard() = Instant::now();
    }
}

/// `SHOW STATUS` 的快照（`§192`）。**在取快照那一刻**算好，不是一段字符串。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusSnapshot {
    pub uptime_secs: i64,
    pub threads_connected: u64,
    pub threads_running: u64,
    pub connections_total: u64,
    pub max_used_connections: u64,
    /// 被 `max_connections` 挡在门外的次数（MySQL 的 `Connection_errors_max_connections`）
    pub connection_errors_max_connections: u64,
    /// 已执行的语句数（MySQL 的 `Questions`：含 `SHOW PROCESSLIST` 自己）
    pub questions_total: u64,
    pub max_connections: u64,
}

/// 会话表。**一个进程一份**（`SqlEngine` 持有；MySQL 与 Flight 共用）。
pub struct QueryRegistry {
    next_id: AtomicU64,
    sessions: Mutex<BTreeMap<u64, Arc<Session>>>,
    /// 本进程的 SQL 面启动时刻（`Uptime` 的零点）
    started: Instant,
    /// 准入上限（`[sql.mysql].max_connections`）：只在 MySQL 监听器上执行。
    /// **为什么放在注册表**：`SHOW VARIABLES` 与 `SHOW STATUS` 要答的就是这个同一个数，
    /// 两份拷贝迟早对不上。
    max_connections: AtomicU64,
    connections_total: AtomicU64,
    max_used_connections: AtomicU64,
    rejected: AtomicU64,
    questions: AtomicU64,
}

impl Default for QueryRegistry {
    fn default() -> Self {
        Self {
            next_id: AtomicU64::new(0),
            sessions: Mutex::new(BTreeMap::new()),
            started: Instant::now(),
            // 与 MySQL 默认一致；`0` 会被 clamp 成 1（0 = 谁都连不上不是"无限"）
            max_connections: AtomicU64::new(151),
            connections_total: AtomicU64::new(0),
            max_used_connections: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            questions: AtomicU64::new(0),
        }
    }
}

impl QueryRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 设连接数上限（`0` ⇒ 1：**不是**"无限"，MySQL 也不接受 0）。
    pub fn set_max_connections(&self, n: u64) {
        self.max_connections.store(n.max(1), Ordering::SeqCst);
    }

    pub fn max_connections(&self) -> u64 {
        self.max_connections.load(Ordering::SeqCst)
    }

    /// 门卫拒了一条连接（`Connection_errors_max_connections++`）。
    pub fn note_rejected(&self) {
        self.rejected.fetch_add(1, Ordering::SeqCst);
    }

    /// 一条**客户端连接**被接纳（`Connections++`）。
    ///
    /// 只由协议层在真正接受连接时调：Flight 的临时会话（一次查询一条）不是"连接"，
    /// 把它们算进 `Connections` 会让监控以为连接数在飙升。
    ///
    /// 峰值 `Max_used_connections` **不在这里**刷新：本函数在 `spawn` 之前调，
    /// 而会话要到 spawn 里才 `attach` —— 在这里数会把峰值少算 1（实测 `=1` 而
    /// 实际有 2 条）。峰值在 [`Self::insert_session`] 真正入表时更新。
    pub fn note_connection(&self) {
        self.connections_total.fetch_add(1, Ordering::SeqCst);
    }

    /// 一条语句开始执行（`Questions++`；引擎在 `begin_query` 里调）。
    pub fn note_question(&self) {
        self.questions.fetch_add(1, Ordering::SeqCst);
    }

    /// `SHOW STATUS` 的数据源。
    pub fn status(&self) -> StatusSnapshot {
        let sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        let running = sessions
            .values()
            .filter(|s| s.running_guard().is_some())
            .count() as u64;
        StatusSnapshot {
            uptime_secs: secs(self.started.elapsed()),
            threads_connected: sessions.values().filter(|s| !s.ephemeral).count() as u64,
            threads_running: running,
            connections_total: self.connections_total.load(Ordering::SeqCst),
            max_used_connections: self.max_used_connections.load(Ordering::SeqCst),
            connection_errors_max_connections: self.rejected.load(Ordering::SeqCst),
            questions_total: self.questions.load(Ordering::SeqCst),
            max_connections: self.max_connections(),
        }
    }

    /// 分配一个永远不会复用的 Id（连接与临时查询共用一个号段，避免 `KILL` 撞号）。
    pub fn alloc_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// 登记一条**客户端连接**（`Drop` 即注销）。
    pub fn attach(self: &Arc<Self>, id: u64, host: impl Into<String>) -> Arc<ConnectionGuard> {
        let session = self.insert_session(id, host, false);
        Arc::new(ConnectionGuard {
            registry: Arc::clone(self),
            session,
            registered: true,
        })
    }

    /// 登记一条**临时会话**（Flight / 裸引擎：一次查询一条，不占连接配额）。
    pub fn attach_ephemeral(
        self: &Arc<Self>,
        id: u64,
        host: impl Into<String>,
    ) -> Arc<ConnectionGuard> {
        let session = self.insert_session(id, host, true);
        Arc::new(ConnectionGuard {
            registry: Arc::clone(self),
            session,
            registered: true,
        })
    }

    fn insert_session(&self, id: u64, host: impl Into<String>, ephemeral: bool) -> Arc<Session> {
        let session = Arc::new(Session {
            id,
            host: host.into(),
            user: Mutex::new(String::new()),
            db: Mutex::new(yuntun_model::ops::DEFAULT_SCHEMA.to_string()),
            running: Mutex::new(None),
            idle_since: Mutex::new(Instant::now()),
            cancel: Cancellation::default(),
            kill_connection: AtomicBool::new(false),
            ephemeral,
        });
        let live = {
            let mut map = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
            map.insert(id, Arc::clone(&session));
            map.values().filter(|s| !s.ephemeral).count() as u64
        };
        if !ephemeral {
            // 真实连接入表时才可能刷新峰值（见 `note_connection` 的说明）
            self.max_used_connections.fetch_max(live, Ordering::SeqCst);
        }
        session
    }

    /// 按 Id 找已登记的连接（`SqlEngine` 用它把查询挂到连接上）。
    ///
    /// 返回的是**借用句柄**（`registered = false`）：查询结束它一丢，会话仍在表里
    /// （`Sleep`），直到 `attach` 的那份被丢掉（连接断开）才注销。
    pub fn lookup(self: &Arc<Self>, id: u64) -> Option<Arc<ConnectionGuard>> {
        let session = self
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .cloned()?;
        Some(Arc::new(ConnectionGuard {
            registry: Arc::clone(self),
            session,
            registered: false,
        }))
    }

    /// 当前活着的会话快照（按 Id 升序）。
    pub fn snapshot(&self) -> Vec<ProcessRow> {
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .map(|s| s.row())
            .collect()
    }

    /// `KILL [CONNECTION|QUERY] <id>`。
    ///
    /// * 未知 Id ⇒ `Err(id)`（MySQL 1094 `ER_NO_SUCH_THREAD`）；
    /// * `connection = false` 且连接空闲 ⇒ `Ok(())`（MySQL 对 `KILL QUERY` 的空闲连接
    ///   返回成功且什么都不做）；* `connection = true` ⇒ 取消当前查询并立断连旗标。
    pub fn kill(&self, id: u64, connection: bool) -> Result<(), u64> {
        let map = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        let Some(session) = map.get(&id) else {
            return Err(id);
        };
        if connection {
            session.kill_connection.store(true, Ordering::SeqCst);
        }
        session.cancel.cancel();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> Arc<QueryRegistry> {
        Arc::new(QueryRegistry::new())
    }

    #[test]
    fn ids_are_unique_and_start_at_one() {
        let r = registry();
        assert_eq!(r.alloc_id(), 1);
        assert_eq!(r.alloc_id(), 2);
        assert_ne!(r.alloc_id(), 2);
    }

    #[test]
    fn snapshot_tracks_sleep_then_query_then_sleep() {
        let r = registry();
        let id = r.alloc_id();
        let conn = r.attach(id, "10.0.0.9:5000");
        conn.set_user("alice");
        let rows = r.snapshot();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].command, "Sleep");
        assert_eq!(rows[0].user, "alice");
        assert!(rows[0].info.is_empty());

        let q = conn.begin("SELECT 1", "sales");
        let rows = r.snapshot();
        assert_eq!(rows[0].command, "Query");
        assert_eq!(rows[0].info, "SELECT 1");
        assert_eq!(rows[0].db, "sales", "USE 之后 db 列要跟着变");
        assert!(!q.is_cancelled());

        drop(q);
        assert_eq!(r.snapshot()[0].command, "Sleep");
        drop(conn);
        assert!(r.snapshot().is_empty(), "连接断开后必须注销");
    }

    /// 借用句柄（`lookup`）**不能**在查询结束时注销连接 ——
    /// 否则第一条查询之后连接就从进程列表消失、下一条查询又换一个新 Id
    /// （`§191` 的实现期实测踩到：`SHOW PROCESSLIST` 里冒出 host=flight 的幽灵行）。
    #[test]
    fn a_borrowed_guard_does_not_unregister_the_connection() {
        let r = registry();
        let id = r.alloc_id();
        let conn = r.attach(id, "h");
        {
            let borrowed = r.lookup(id).expect("已登记");
            let q = borrowed.begin("SELECT 1", "public");
            drop(q);
            drop(borrowed);
        }
        let rows = r.snapshot();
        assert_eq!(rows.len(), 1, "查询结束后连接必须还在");
        assert_eq!(rows[0].id, id);
        assert_eq!(rows[0].command, "Sleep");
        drop(conn);
        assert!(r.snapshot().is_empty(), "连接断开后才注销");
    }

    #[tokio::test]
    async fn kill_query_cancels_the_running_segment() {
        let r = registry();
        let id = r.alloc_id();
        let conn = r.attach(id, "h");
        let q = conn.begin("SELECT slow", "public");

        r.kill(id, false).expect("已知 Id 必须受理");
        assert!(q.is_cancelled());
        // 取消位在**下一条查询开始时**重置（KILL 只打当前这条）
        drop(q);
        let q2 = conn.begin("SELECT next", "public");
        assert!(!q2.is_cancelled(), "取消位必须每条查询重置");
    }

    #[test]
    fn kill_connection_sets_the_disconnect_flag() {
        let r = registry();
        let id = r.alloc_id();
        let conn = r.attach(id, "h");
        assert!(!conn.kill_requested());
        r.kill(id, true).expect("已知 Id 必须受理");
        assert!(conn.kill_requested());
        // QUERY 形态**不**断连
        let id2 = r.alloc_id();
        let conn2 = r.attach(id2, "h");
        r.kill(id2, false).unwrap();
        assert!(!conn2.kill_requested());
    }

    #[test]
    fn unknown_thread_is_reported_not_ignored() {
        let r = registry();
        assert_eq!(r.kill(42, false), Err(42), "未知 Id ⇒ 1094，不静默成功");
    }

    #[tokio::test]
    async fn cancellation_is_lost_wakeup_safe() {
        let c = Arc::new(Cancellation::default());
        // 取消发生在等待**之前**
        c.cancel();
        tokio::time::timeout(Duration::from_secs(1), c.cancelled())
            .await
            .expect("已取消时必须立即返回");
        // 取消发生在等待**之后**
        c.reset();
        let c2 = Arc::clone(&c);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            c2.cancel();
        });
        tokio::time::timeout(Duration::from_secs(1), c.cancelled())
            .await
            .expect("等待中的取消必须被唤醒");
    }
}
