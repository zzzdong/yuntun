//! 攒批工具：窗口归属与批次工具函数（详细设计 §5.3）。
//!
//! > **分组与 seal / flush 决策已收敛到 chunk 层**（`yuntun_chunk::ChunkStore`）。
//! > 旧实现里"攒批分组（`WindowGroup`）"与"内存分片（`MemoryShard`）"各算一套，
//! > 语义会漂移；现在 chunk 就是那个分组单元，seal / spill / flush 计划全在
//! > `SealPolicy` 一处（架构 §2.2 / §2.7 / §5.3）。本模块只剩纯函数工具。

use crate::timeutil::{format_window, window_start_ms};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// 窗口归属：数据按 event_time（缺省用 received_at）对齐到整分钟（ADR-10）。
pub fn window_of(event_time_ms: Option<i64>, received_at_ms: i64) -> (i64, String) {
    let t = event_time_ms.unwrap_or(received_at_ms);
    let w = window_start_ms(t);
    (w, format_window(w))
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
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as SArc;

    #[test]
    fn window_alignment_is_minute_bucketed() {
        let ts = 1_788_166_527_000i64;
        let (w, s) = window_of(Some(ts), 0);
        assert_eq!(w % crate::timeutil::MINUTE_MS, 0, "窗口起点必须是整分钟");
        assert!(ts >= w && ts < w + crate::timeutil::MINUTE_MS, "事件时间必须落在窗口内");
        assert_eq!(s.len(), 16, "窗口标识形如 YYYY-MM-DDTHH:MM: {s}");
        assert_eq!(&s[10..11], "T");

        // 缺省 event_time → 用 received_at
        let (w1, s1) = window_of(None, ts);
        assert_eq!((w1, s1), (w, s));
    }

    #[test]
    fn extract_event_time_handles_i64_and_null() {
        let schema = SArc::new(Schema::new(vec![Field::new(
            "event_time",
            DataType::Int64,
            true,
        )]));
        let b = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![SArc::new(Int64Array::from(vec![123i64, 456]))],
        )
        .unwrap();
        assert_eq!(extract_event_time_ms(&b), Some(123));

        let all_null = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![SArc::new(Int64Array::from(vec![Option::<i64>::None]))],
        )
        .unwrap();
        assert_eq!(extract_event_time_ms(&all_null), None);

        // 无 event_time 列 → None（按 received_at 归属窗口）
        let other = SArc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));
        let b2 = arrow::record_batch::RecordBatch::try_new(
            other,
            vec![SArc::new(StringArray::from(vec!["a"]))],
        )
        .unwrap();
        assert_eq!(extract_event_time_ms(&b2), None);
    }

    #[tokio::test]
    async fn backoff_gives_up_after_retries() {
        let mut calls = 0;
        let res: Result<(), &str> = with_backoff(|| {
            calls += 1;
            async { Err("boom") }
        })
        .await;
        assert!(res.is_err());
        assert_eq!(calls, 5);
    }
}
