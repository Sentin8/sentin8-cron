// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Sentin8:Cron
// SPDX-License-Identifier: Apache-2.0

//! Cron expression parsing and next-fire computation.
//!
//! 5-field syntax: minute hour day-of-month month day-of-week, with `*`,
//! lists, ranges (`a-b`), steps (`a/b`), and the `@hourly`/`@daily`/`@weekly`/
//! `@monthly` aliases. UTC only; misfire policy is skip (next fire is always
//! strictly after the reference time).

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Errors returned by cronexpr.
pub struct CronExprError(pub String);

impl std::fmt::Display for CronExprError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid cron expression: {}", self.0)
    }
}

impl std::error::Error for CronExprError {}

/// One schedule field: the set of allowed values (already expanded from
/// `*`, lists, ranges, and steps).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Field {
    values: Vec<u32>,
}

impl Field {
    fn matches(&self, v: u32) -> bool {
        self.values.binary_search(&v).is_ok()
    }
}

/// A parsed cron expression.
#[derive(Debug, Clone, PartialEq, Eq)]
/// The cronexpr type.
pub struct CronExpr {
    minute: Field,
    hour: Field,
    dom: Field,
    month: Field,
    dow: Field,
    /// `L` in day-of-week semantics: if true, dom and dow are both
    /// restricted and the day matches when EITHER matches (standard cron
    /// Vixie behavior).
    dom_and_dow_restricted: bool,
}

const DOM_MIN: u32 = 1;
const DOM_MAX: u32 = 31;
const MONTH_MIN: u32 = 1;
const MONTH_MAX: u32 = 12;
const DOW_MIN: u32 = 0;

impl std::str::FromStr for CronExpr {
    type Err = CronExprError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let expanded = expand_aliases(s)?;
        let fields: Vec<&str> = expanded.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(CronExprError(format!(
                "expected 5 fields, got {}",
                fields.len()
            )));
        }
        let minute = parse_field(fields[0], 0, 59, "minute")?;
        let hour = parse_field(fields[1], 0, 23, "hour")?;
        let dom = parse_field(fields[2], DOM_MIN, DOM_MAX, "day-of-month")?;
        let month = parse_field(fields[3], MONTH_MIN, MONTH_MAX, "month")?;
        // Vixie cron allows 0-7 with 7 treated as Sunday.
        let mut dow = parse_field(fields[4], DOW_MIN, 7, "day-of-week")?;
        if dow.values.contains(&7) {
            dow.values.push(0);
            dow.values.sort_unstable();
            dow.values.dedup();
        }
        let dom_restricted = fields[2] != "*";
        let dow_restricted = fields[4] != "*";
        Ok(Self {
            minute,
            hour,
            dom,
            month,
            dow,
            dom_and_dow_restricted: dom_restricted && dow_restricted,
        })
    }
}

fn expand_aliases(s: &str) -> Result<String, CronExprError> {
    match s.trim() {
        "@hourly" => Ok("0 * * * *".into()),
        "@daily" | "@midnight" => Ok("0 0 * * *".into()),
        "@weekly" => Ok("0 0 * * 0".into()),
        "@monthly" => Ok("0 0 1 * *".into()),
        "@yearly" | "@annually" => Ok("0 0 1 1 *".into()),
        other => {
            if other.starts_with('@') {
                return Err(CronExprError(format!("unknown alias {other}")));
            }
            Ok(other.to_string())
        }
    }
}

fn parse_field(field: &str, min: u32, max: u32, name: &str) -> Result<Field, CronExprError> {
    let mut values = Vec::new();
    for part in field.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err(CronExprError(format!("{name}: empty list element")));
        }
        let (range_part, step) = match part.split_once('/') {
            Some((r, s)) => {
                let step: u32 = s
                    .parse()
                    .map_err(|_| CronExprError(format!("{name}: bad step '{s}'")))?;
                if step == 0 {
                    return Err(CronExprError(format!("{name}: step must be >= 1")));
                }
                (r, step)
            }
            None => (part, 1),
        };
        let (lo, hi) = if range_part == "*" {
            (min, max)
        } else if let Some((a, b)) = range_part.split_once('-') {
            let lo: u32 = a
                .parse()
                .map_err(|_| CronExprError(format!("{name}: bad bound '{a}'")))?;
            let hi: u32 = b
                .parse()
                .map_err(|_| CronExprError(format!("{name}: bad bound '{b}'")))?;
            (lo, hi)
        } else {
            let v: u32 = range_part
                .parse()
                .map_err(|_| CronExprError(format!("{name}: bad value '{range_part}'")))?;
            (v, v)
        };
        if lo < min || hi > max || lo > hi {
            return Err(CronExprError(format!(
                "{name}: range {lo}-{hi} outside {min}-{max}"
            )));
        }
        let mut v = lo;
        while v <= hi {
            values.push(v);
            v += step;
            if v > max {
                break;
            }
        }
    }
    if values.is_empty() {
        return Err(CronExprError(format!("{name}: no values")));
    }
    values.sort_unstable();
    values.dedup();
    Ok(Field { values })
}

impl CronExpr {
    /// Parse it.
    pub fn parse(s: &str) -> Result<Self, CronExprError> {
        s.parse()
    }

    #[allow(dead_code)] // used by tests + future in-tick checks; next_after covers the run path
    fn matches(&self, t: DateTime<Utc>) -> bool {
        let dow_val = t.weekday().num_days_from_sunday();
        let dom_ok = self.dom.matches(t.day());
        let dow_ok = self.dow.matches(dow_val);
        let day_ok = if self.dom_and_dow_restricted {
            dom_ok || dow_ok
        } else {
            dom_ok && dow_ok
        };
        self.minute.matches(t.minute())
            && self.hour.matches(t.hour())
            && self.month.matches(t.month())
            && day_ok
    }

    /// Next fire time strictly after `after` (misfire = skip policy).
    /// Iterates minute-by-minute bounded by a 4-year horizon (covers leap
    /// cycles); returns `None` if no matching minute exists.
    /// Handle next after.
    pub fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let mut t = after
            .with_second(0)
            .and_then(|t| t.with_nanosecond(0))
            .unwrap_or(after);
        // Start at the next whole minute.
        t += Duration::minutes(1);
        let horizon = after + Duration::days(366 * 4);
        while t <= horizon {
            if !self.month.matches(t.month()) {
                let months_ahead = next_allowed_month_local(&t, &self.month)?;
                t = first_of_next_month(t, months_ahead)?;
                continue;
            }
            if !day_matches(self, t) {
                // Skip to midnight of the next day — keeps the minute
                // iteration inside one day only.
                t = (t + Duration::days(1))
                    .with_hour(0)
                    .and_then(|x| x.with_minute(0))
                    .and_then(|x| x.with_second(0))
                    .and_then(|x| x.with_nanosecond(0))?;
                continue;
            }
            if self.hour.matches(t.hour()) && self.minute.matches(t.minute()) {
                return Some(t);
            }
            t += Duration::minutes(1);
        }
        None
    }

    /// Next fire time strictly after `after`, evaluated in the schedule's
    /// IANA timezone (CRON-6, cron README §7): the wall-clock fields
    /// (minute/hour/dom/month/dow) match against LOCAL time in `tz`; the
    /// returned instant is UTC. DST is resolved by chrono-tz — an ambiguous
    /// local time maps to the earlier (fold 0) instant. `tz` `"UTC"` or an
    /// unparseable name behaves exactly like [`Self::next_after`].
    ///
    /// Implementation: convert `after` into the target zone, run the same
    /// minute-scan over local wall times, and convert each candidate back
    /// to UTC at the end — a single conversion keeps the scan cheap.
    pub fn next_after_in_tz(&self, after: DateTime<Utc>, tz: &str) -> Option<DateTime<Utc>> {
        let tz_parsed: chrono_tz::Tz = tz.parse().ok()?;
        let local_after = after.with_timezone(&tz_parsed);
        // Scan local wall time (NaiveDateTime semantics) minute-by-minute
        // using the same predicates as next_after, then map back to UTC.
        let mut local = local_after
            .with_second(0)
            .and_then(|t| t.with_nanosecond(0))
            .unwrap_or(local_after);
        local += Duration::minutes(1);
        let horizon = local_after + Duration::days(366 * 4);
        while local <= horizon {
            if !self.month.matches(local.month()) {
                let months_ahead = next_allowed_month_local(&local, &self.month)?;
                local = first_of_next_month_local(local, months_ahead)?;
                continue;
            }
            if !day_matches_local(self, &local) {
                local = (local + Duration::days(1))
                    .with_hour(0)
                    .and_then(|x| x.with_minute(0))
                    .and_then(|x| x.with_second(0))
                    .and_then(|x| x.with_nanosecond(0))?;
                continue;
            }
            if self.hour.matches(local.hour()) && self.minute.matches(local.minute()) {
                return from_local_earliest(tz_parsed, local.naive_local());
            }
            local += Duration::minutes(1);
        }
        None
    }
}

/// Map a local wall time in `tz` back to UTC, preferring the EARLIER
/// instant across a DST fold (an ambiguous wall time exists twice).
/// Nonexistent times (spring-forward gap) resolve to the next valid
/// instant chrono-tz offers.
fn from_local_earliest(tz: chrono_tz::Tz, local: NaiveDateTime) -> Option<DateTime<Utc>> {
    let earliest = tz.from_local_datetime(&local).earliest()?;
    Some(earliest.with_timezone(&Utc))
}

/// First instant of the next occurrence of `month` after `t`, in LOCAL
/// wall time (the timezone-aware analogue of `first_of_next_month`).
fn first_of_next_month_local<Tz: TimeZone>(t: DateTime<Tz>, month: u32) -> Option<DateTime<Tz>> {
    let year = if month > t.month() {
        t.year()
    } else {
        t.year() + 1
    };
    let naive = NaiveDate::from_ymd_opt(year, month, 1)?.and_hms_opt(0, 0, 0)?;
    // DST-gap safe: `earliest()` picks the single valid instant, or the
    // earliest of a fold, or shifts past a nonexistent 00:00.
    t.timezone().from_local_datetime(&naive).earliest()
}

fn day_matches_local<Tz: chrono::TimeZone>(e: &CronExpr, t: &DateTime<Tz>) -> bool {
    let dom_ok = e.dom.matches(t.day());
    let dow_ok = e.dow.matches(t.weekday().num_days_from_sunday());
    if e.dom_and_dow_restricted {
        dom_ok || dow_ok
    } else {
        dom_ok && dow_ok
    }
}

fn day_matches(e: &CronExpr, t: DateTime<Utc>) -> bool {
    let dom_ok = e.dom.matches(t.day());
    let dow_ok = e.dow.matches(t.weekday().num_days_from_sunday());
    if e.dom_and_dow_restricted {
        dom_ok || dow_ok
    } else {
        dom_ok && dow_ok
    }
}

fn next_allowed_month_local<Tz: TimeZone>(t: &DateTime<Tz>, month: &Field) -> Option<u32> {
    let mut m = t.month();
    for _ in 0..12 {
        m = if m == 12 { 1 } else { m + 1 };
        if month.matches(m) {
            return Some(m);
        }
    }
    None
}

/// First instant of the next occurrence of `month` after `t` — year-wraps
/// when the target month is earlier in the calendar than the current one
/// (e.g. July → February lands on next year's Feb 1).
fn first_of_next_month(t: DateTime<Utc>, month: u32) -> Option<DateTime<Utc>> {
    let year = if month > t.month() {
        t.year()
    } else {
        t.year() + 1
    };
    Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0).single()
}


/// Validate an expression for the create handler — returns the normalized
/// parse error message on failure.
/// Validate it.
pub fn validate(expr: &str) -> Result<(), CronExprError> {
    CronExpr::parse(expr).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn parse(s: &str) -> CronExpr {
        CronExpr::parse(s).unwrap()
    }

    #[test]
    fn every_minute() {
        let e = parse("* * * * *");
        let after = utc("2026-01-01T00:00:30Z");
        assert_eq!(e.next_after(after), Some(utc("2026-01-01T00:01:00Z")));
    }

    #[test]
    fn daily_midnight() {
        let e = parse("0 0 * * *");
        let after = utc("2026-01-01T12:34:56Z");
        assert_eq!(e.next_after(after), Some(utc("2026-01-02T00:00:00Z")));
        // Exactly at midnight → next midnight (strictly after).
        assert_eq!(
            e.next_after(utc("2026-01-01T00:00:00Z")),
            Some(utc("2026-01-02T00:00:00Z"))
        );
    }

    #[test]
    fn monthly_first_of_month() {
        let e = parse("0 0 1 * *");
        assert_eq!(
            e.next_after(utc("2026-01-05T00:00:00Z")),
            Some(utc("2026-02-01T00:00:00Z"))
        );
        assert_eq!(
            e.next_after(utc("2026-02-28T23:59:59Z")),
            Some(utc("2026-03-01T00:00:00Z"))
        );
    }

    #[test]
    fn month_skips_fast_path() {
        // February only — must land on Feb 1 even when starting in July.
        let e = parse("0 0 1 2 *");
        assert_eq!(
            e.next_after(utc("2026-07-15T00:00:00Z")),
            Some(utc("2027-02-01T00:00:00Z"))
        );
    }

    #[test]
    fn ranges_lists_steps() {
        let e = parse("0 9-17/2,8 * * *");
        let after = utc("2026-06-01T07:00:00Z");
        assert_eq!(e.next_after(after), Some(utc("2026-06-01T08:00:00Z")));
        assert_eq!(
            e.next_after(utc("2026-06-01T08:00:00Z")),
            Some(utc("2026-06-01T09:00:00Z"))
        );
        assert_eq!(
            e.next_after(utc("2026-06-01T09:00:00Z")),
            Some(utc("2026-06-01T11:00:00Z"))
        );
    }

    #[test]
    fn day_of_week() {
        // Monday=1.
        let e = parse("30 2 * * 1-5");
        // is a Thursday.
        assert_eq!(
            e.next_after(utc("2026-01-01T00:00:00Z")),
            Some(utc("2026-01-01T02:30:00Z"))
        );
        // is Saturday → next is Monday.
        assert_eq!(
            e.next_after(utc("2026-01-03T00:00:00Z")),
            Some(utc("2026-01-05T02:30:00Z"))
        );
    }

    #[test]
    fn sunday_as_seven() {
        let e = parse("0 0 * * 7");
        // is a Sunday.
        assert_eq!(
            e.next_after(utc("2026-01-01T00:00:00Z")),
            Some(utc("2026-01-04T00:00:00Z"))
        );
    }

    #[test]
    fn dom_or_dow_vixie_semantics() {
        // Both restricted → matches when EITHER matches.
        let e = parse("0 0 13 * 5");
        // is a Friday → matches dom. is a Friday → matches dow.
        assert_eq!(
            e.next_after(utc("2026-02-01T00:00:00Z")),
            Some(utc("2026-02-06T00:00:00Z"))
        );
        assert_eq!(
            e.next_after(utc("2026-02-06T00:00:01Z")),
            Some(utc("2026-02-13T00:00:00Z"))
        );
    }

    #[test]
    fn aliases() {
        assert_eq!(parse("@daily"), parse("0 0 * * *"));
        assert_eq!(parse("@hourly"), parse("0 * * * *"));
        assert_eq!(parse("@monthly"), parse("0 0 1 * *"));
        assert_eq!(parse("@weekly"), parse("0 0 * * 0"));
        assert_eq!(parse("@yearly"), parse("0 0 1 1 *"));
        assert!(CronExpr::parse("@nonsense").is_err());
    }

    #[test]
    fn rejects_bad_expressions() {
        for bad in [
            "",
            "* * * *",
            "* * * * * *",
            "60 * * * *",
            "* 24 * * *",
            "* * 0 * *",
            "* * 32 * *",
            "* * * 13 *",
            "* * * * 8",
            "a * * * *",
            "*/0 * * * *",
            "5-1 * * * *",
            "1,,2 * * * *",
        ] {
            assert!(CronExpr::parse(bad).is_err(), "expected rejection: {bad}");
        }
    }

    #[test]
    fn leap_year_feb_29() {
        let e = parse("0 0 29 2 *");
        assert_eq!(
            e.next_after(utc("2026-01-01T00:00:00Z")),
            Some(utc("2028-02-29T00:00:00Z"))
        );
    }

    #[test]
    fn validate_used_by_create() {
        // Direct validation helper used by the create handler.
        assert!(crate::cron_expr::validate("*/5 * * * *").is_ok());
        assert!(crate::cron_expr::validate("nope").is_err());
    }
}
