//! 攒批器：按 (table, shard, window) 分组（详细设计 §5.3）。
//!
//! 触发条件（任一满足即 flush，§5.3）：
//! - 行数 >= `rows_threshold`（表配置，默认 10000）
//! - 距窗口开始 >= `time_threshold`（默认 5s）
//! - 整分钟对齐 + Jitter：`flush_at = window_start + hash(shard+table) % 60s`（ADR-10）
//! - 绝对空闲超时兜底：5 分钟（防定时器 bug 导致无限滞留）

use crate::timeutil::{jitter_seconds, window_start_ms, MINUTE_MS};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use yuntun_model::wal_record::DataPayload;

/// 窗口归属：数据按 event_time（缺省用 received_at）对齐到整分钟（ADR-10）。
pub fn window_of(event_time_ms: Option<i64>, received_at_ms: i64) -> (i64, String) {
    let t = event_time_ms.unwrap_or(received_at_ms);
    let w = window_start_ms(t);
    (w, crate::timeutil::format_window(w))
}

/// 从 RecordBatch 提取 event_time 列（i64 毫秒，列名 event_time；缺省 None）。
pub fn extract_event_time_ms(batch: &arrow::record_batch::RecordBatch) -> Option<i64> {
    let idx = batch.schema().column_with_name("event_time")?.0;
    let arr = batch.column(idx);
    use arrow::array::{Array, Int64Array, TimestampMillisecondArray};
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return if a.null_count() == arr.len() {
            None
        } else {
            Some(a.value(0))
        };
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return if a.null_count() == arr.len() {
            None
        } else {
            Some(a.value(0))
        };
    }
    None
}

/// 一个 (table, shard, window) 分组的攒批缓冲。
#[derive(Debug, Default)]
pub struct WindowGroup {
    pub table: String,
    pub shard: String,
    /// 窗口起点毫秒
    pub window_ms: i64,
    pub window: String,
    /// 累积的 WAL Data 记录（按到达顺序）
    pub payloads: Vec<DataPayload>,
    /// 该组第一个 payload 的 WAL seq（BatchPending.wal_seq_range 起点用）
    pub first_seq: u64,
    /// 累积行数
    pub rows: u64,
    pub created_at_ms: u64,
}

impl WindowGroup {
    /// flush 触发判定（§5.3 任一满足即 flush）。
    pub fn should_flush(&self, now_ms: u64, cfg: &crate::pipeline::IngestorConfig) -> bool {
        // ① 行数阈值
        if self.rows >= cfg.rows_threshold {
            return true;
        }
        // ② 时间阈值：距窗口开始 >= time_threshold
        let now_wall = now_ms as i64;
        if now_wall - self.window_ms >= cfg.time_threshold_secs as i64 * 1000 {
            // ③ Jitter 削峰（ADR-10）：flush 时刻 = window_start + jitter；
            //    窗口已关闭（超过窗口末尾）则无视 jitter 立即 flush
            let jitter = jitter_seconds(&self.shard, &self.table, cfg.flush_jitter_secs);
            let flush_at = self.window_ms + (jitter as i64) * 1000;
            let window_end = self.window_ms + MINUTE_MS;
            if now_wall >= flush_at || now_wall >= window_end {
                return true;
            }
        }
        // ④ 绝对空闲超时兜底（v7 评审 A 建议）
        if now_ms.saturating_sub(self.created_at_ms) >= cfg.idle_timeout.as_millis() as u64 {
            return true;
        }
        false
    }
}

/// 攒批缓冲集合：key = (table, shard, window)。
#[derive(Debug, Default)]
pub struct BatchAccumulator {
    groups: HashMap<(String, String, String), WindowGroup>,
}

impl BatchAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, payload: DataPayload, seq: u64, rows: u64, now_ms: u64) {
        let key = (
            payload.table.clone(),
            payload.shard.clone(),
            payload.time_window.clone(),
        );
        let entry = self.groups.entry(key).or_insert_with(|| WindowGroup {
            table: payload.table.clone(),
            shard: payload.shard.clone(),
            window_ms: window_start_ms(now_ms as i64),
            window: payload.time_window.clone(),
            payloads: Vec::new(),
            first_seq: seq,
            rows: 0,
            created_at_ms: now_ms,
        });
        entry.payloads.push(payload);
        entry.rows += rows;
    }

    /// 检查并弹出满足 flush 条件的组。
    pub fn drain_ready(
        &mut self,
        now_ms: u64,
        cfg: &crate::pipeline::IngestorConfig,
    ) -> Vec<WindowGroup> {
        let mut ready = Vec::new();
        let keys: Vec<_> = self.groups.keys().cloned().collect();
        for k in keys {
            if let Some(g) = self.groups.get(&k) {
                if g.should_flush(now_ms, cfg) {
                    ready.push(self.groups.remove(&k).unwrap());
                }
            }
        }
        ready
    }

    pub fn len(&self) -> usize {
        self.groups.len()
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 简单重试：指数退避（100ms 起，上限 10s，最多 5 次，§10.2）。
pub async fn with_backoff<T, E, F, Fut>(mut f: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let mut delay = Duration::from_millis(100);
    let mut last_err = None;
    for _ in 0..5 {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(10));
            }
        }
    }
    Err(last_err.unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(table: &str, shard: &str, window: &str, rows_hint: u64) -> DataPayload {
        DataPayload {
            table: table.into(),
            shard: shard.into(),
            schema_version: 1,
            batch_ipc: vec![0; rows_hint as usize],
            client_request_id: String::new(),
            time_window: window.into(),
        }
    }

    fn cfg() -> crate::pipeline::IngestorConfig {
        crate::pipeline::IngestorConfig {
            rows_threshold: 10_000,
            time_threshold_secs: 5,
            idle_timeout: Duration::from_secs(300),
            flush_jitter_secs: 0, // 测试禁用 jitter
            ..Default::default()
        }
    }

    #[test]
    fn rows_threshold_triggers() {
        let mut acc = BatchAccumulator::new();
        let now = now_ms();
        // 未来窗口：时间阈值不会触发，纯行数阈值判定
        let future_window_ms = (now as i64) + 3_600_000;
        acc.groups.insert(
            ("t".into(), "s0".into(), "w1".into()),
            WindowGroup {
                table: "t".into(),
                shard: "s0".into(),
                window_ms: future_window_ms,
                window: "w1".into(),
                payloads: Vec::new(),
                first_seq: 0,
                rows: 9_999,
                created_at_ms: now,
            },
        );
        assert!(acc.drain_ready(now, &cfg()).is_empty());
        let g = acc
            .groups
            .get_mut(&("t".into(), "s0".into(), "w1".into()))
            .unwrap();
        g.rows += 1;
        let ready = acc.drain_ready(now, &cfg());
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].rows, 10_000);
    }

    #[test]
    fn time_threshold_triggers() {
        let mut acc = BatchAccumulator::new();
        // 窗口 6 秒前 → 已超过 5s 时间阈值（jitter=0）
        acc.push(payload("t", "s0", "w1", 1), 0, 5, now_ms() - 6_000);
        let ready = acc.drain_ready(now_ms(), &cfg());
        assert_eq!(ready.len(), 1);
    }

    #[test]
    fn idle_timeout_bottomline_triggers() {
        let mut acc = BatchAccumulator::new();
        // 窗口在未来（时钟偏差防护场景）：仅空闲兜底触发
        let mut c = cfg();
        c.idle_timeout = Duration::from_millis(100);
        acc.push(payload("t", "s0", "w1", 1), 0, 1, now_ms() - 150);
        let ready = acc.drain_ready(now_ms(), &c);
        assert_eq!(ready.len(), 1);
    }

    #[test]
    fn groups_isolated_by_shard_and_window() {
        let mut acc = BatchAccumulator::new();
        acc.push(payload("t", "s0", "w1", 1), 0, 1, now_ms());
        acc.push(payload("t", "s1", "w1", 1), 1, 1, now_ms());
        acc.push(payload("t", "s0", "w2", 1), 2, 1, now_ms());
        assert_eq!(acc.len(), 3);
    }

    #[test]
    fn jitter_spread() {
        // ADR-10：不同 shard 的 flush 时刻被分散（防惊群）
        let c = crate::pipeline::IngestorConfig {
            flush_jitter_secs: 60,
            time_threshold_secs: 0,
            ..Default::default()
        };
        let mut acc = BatchAccumulator::new();
        let now = now_ms();
        for i in 0..5 {
            acc.push(payload("t", &format!("s{i}"), "w1", 1), i, 1, now);
        }
        // 窗口刚开始 + jitter>0 → 不应立即全部 flush
        let ready = acc.drain_ready(now, &c);
        assert!(ready.len() < 5, "jitter 应推迟 flush（防同秒惊群）");
    }
}
