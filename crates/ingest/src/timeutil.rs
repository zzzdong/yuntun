//! 时间窗口工具：整分钟对齐（ADR-10）+ 无 chrono 依赖的 epoch 格式化。

/// 分钟毫秒。
pub const MINUTE_MS: i64 = 60_000;

/// 将 event_time（Unix 毫秒）对齐到整分钟，返回窗口起点毫秒。
pub fn window_start_ms(event_time_ms: i64) -> i64 {
    event_time_ms.div_euclid(MINUTE_MS) * MINUTE_MS
}

/// 窗口标识字符串："YYYY-MM-DDTHH:MM"（UTC）。
pub fn format_window(window_start_ms: i64) -> String {
    let (y, mo, d, h, mi) = civil_from_epoch_ms(window_start_ms);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}")
}

/// Flush Jitter（ADR-10 / v8 修正）：
/// `flush_moment = window_start + (hash(shard+table) % jitter_secs) 秒`。
/// Jitter 打散的是 flush 动作发生的时刻，不改变 time_window 的数据归属。
pub fn jitter_seconds(shard: &str, table: &str, jitter_secs: u64) -> u64 {
    // FNV-1a 64
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in shard.as_bytes().iter().chain(table.as_bytes().iter()) {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    if jitter_secs == 0 {
        0
    } else {
        h % jitter_secs
    }
}

/// epoch 毫秒 → (year, month, day, hour, minute)（UTC，Howard Hinnant civil 算法）。
pub fn civil_from_epoch_ms(ms: i64) -> (i64, u32, u32, u32, u32) {
    let secs = ms.div_euclid(1000);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (h, mi, _s) = (
        (rem / 3600) as u32,
        ((rem % 3600) / 60) as u32,
        (rem % 60) as u32,
    );
    // civil_from_days
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097); // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d, h, mi)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_alignment() {
        // 2026-08-31T14:03:27Z = 1788206607... 用已知值验证对齐
        let ms = 1_788_166_527_000i64; // 任意时刻
        let w = window_start_ms(ms);
        assert_eq!(w % MINUTE_MS, 0);
        assert!(ms >= w && ms < w + MINUTE_MS);
    }

    #[test]
    fn window_format() {
        // 2026-08-31T14:00:00Z 的 epoch 毫秒
        // 用 civil 算法往返验证：2026-08-31 → days
        let ms = days_from_civil(2026, 8, 31) * 86_400_000 + 14 * 3_600_000;
        assert_eq!(format_window(window_start_ms(ms)), "2026-08-31T14:00");
        assert_eq!(
            format_window(window_start_ms(ms + 59_999)),
            "2026-08-31T14:00"
        );
    }

    #[test]
    fn jitter_is_stable_and_bounded() {
        let a = jitter_seconds("s0", "tbl", 60);
        let b = jitter_seconds("s0", "tbl", 60);
        assert_eq!(a, b, "同一 shard+table 的 flush 时刻稳定可预测");
        assert!(a < 60);
        // 不同 shard 大概率分散
        let set: std::collections::HashSet<u64> = (0..20)
            .map(|i| jitter_seconds(&format!("s{i}"), "tbl", 60))
            .collect();
        assert!(set.len() > 1, "jitter 应打散不同 shard");
    }

    fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
        let y = if m <= 2 { y - 1 } else { y };
        let era = y.div_euclid(400);
        let yoe = y - era * 400;
        let mp = if m > 2 { m - 3 } else { m + 9 } as i64;
        let doy = (153 * mp + 2) / 5 + d as i64 - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era * 146_097 + doe - 719_468
    }
}
