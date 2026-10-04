# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Phạm Long Nhật / 2A202602844
**Repo:** https://github.com/Nhatcony0902/K4-Track02-Day17-PhamLongNhat-2A202602844-DataPipelineEngineering
**Commit bài nộp:** `82f8978` (ba bản sửa trong `pipeline/`); REPORT và `checksums.txt` ở commit `30f9c6c` và commit ngay sau trên `main`
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (Claude Opus 5.5) — đọc đề và code, chạy lệnh kiểm tra, đề xuất 3 chỗ sửa trong `pipeline/` và soạn nháp REPORT; tôi đã review từng dòng sửa và chạy lại toàn bộ kiểm tra.
**Nguồn tham khảo khác (nếu có):** slide Ngày 17; tài liệu Debezium (định dạng change event Postgres).

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | verify: "24 rows for 12 tickets"; T-91 có 3 hàng (`low/open`, `high/open`, `high/closed/bug`) | `gold_feature_daily` lệch full recompute (`c50b…` ≠ `8630…`); u05 ngày 08-12 = `(2, 0)`, đúng phải `(5, 1)` | T-97 vẫn `is_deleted = False`, còn user/subject/body; còn trong snapshot `v2026-08-16` và 2 chunk RAG |
| **Nguyên nhân gốc** | Chỉ dedup *trong* batch rồi `INSERT`: không có khoá giữa các batch, mỗi lần chạy thêm hàng | `LOOKBACK_DAYS = 0` là đoán; event 08-12 của u05 tới 08-15 nhưng partition 08-12 không được tính lại | Khoá lấy từ `after.ticket_id`; với `op='d'` thì `after = null` → bản ghi xoá bị lọc mất |
| **Cách sửa** | `silver.py`: `MERGE … ON ticket_id`, `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE`, chưa có thì `INSERT` | `config.py`: `LOOKBACK_DAYS = 3` = ceil(P99) đo từ Bronze; mỗi run tính lại `[day−3, day]` | `staging.py`: `coalesce(after.ticket_id, before.ticket_id)` → MERGE biến hàng thành tombstone; Gold lọc `is_deleted` |
| **Khái niệm** | Silver có khoá, MERGE, LSN guard | Data về muộn, event time ≠ ingest time, lookback | CDC log-based, “Xoá phải lan” |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (p50 0, p95 2.90, max 3) → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: **PARITY**. Verify 8/18 → 18/18; pytest 34 passed; dbt PASS=19.

## 3. Lựa chọn công cụ / kỹ thuật

- **MERGE cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`:** ticket là thực thể thay đổi nên upsert theo khoá, LSN quyết định bản nào mới (chạy lại batch cũ = no-op); feature là aggregate tính lại được từ Silver nên xoá-rồi-tính-lại cửa sổ vừa idempotent vừa bắt event muộn.
- **Tombstone thay vì xoá hẳn:** giữ khoá + LSN của lần xoá để replay bản ghi cũ hơn không hồi sinh ticket; đổi lại hàng tồn tại mãi (có thể dọn định kỳ).
- **Snapshot dựng lại “as of” ngày đó, không sửa bản cũ:** model train trên `vX` phải tái lập được; thay đổi mới đi vào version mới.
- **DuckDB / dbt thay vì Spark:** vài chục bản ghi/ngày chạy gọn trong một process; dbt cho sẵn merge, microbatch, contract, unit test. Spark chỉ đáng khi dữ liệu vượt một máy.

## 4. Hai câu hỏi suy ngẫm

1. **Snapshot bất biến vs quyền được xoá:** quyền xoá thắng vì là nghĩa vụ pháp lý. Tôi tạo version thay thế (vd. `v2026-08-12-r1`) đã loại T-97, đánh dấu bản gốc `revoked` rồi xoá vật lý, ghi audit log lý do checksum đổi và lên lịch train lại model liên quan. Lâu dài: crypto-shredding — mã hoá PII theo khoá từng user, xoá khoá là xoá dữ liệu ở mọi snapshot.
2. **PII như tên người:** đặt chốt ở ranh giới Bronze → Silver: regex + NER tiếng Việt (vd. underthesea, PhoBERT-NER) thay tên bằng `<NAME>`, thêm contract test quét lại ở Gold. Đo bằng tập mẫu có gán nhãn PII (ưu tiên recall vì bỏ sót đắt hơn che nhầm) và theo dõi tỉ lệ phát hiện hằng ngày; Bronze giới hạn quyền truy cập.

## 5. Output (dán nguyên văn)

Chạy trên Windows PowerShell với `.\.venv\Scripts\python.exe` (Python 3.11.9), các lệnh tương đương theo [SUBMISSION.md](../docs/SUBMISSION.md).

**Baseline trước khi sửa (`python -m scripts.verify`) — triệu chứng ban đầu:**

```text
  [XX ] Silver  silver_tickets has exactly one row per ticket_id  (24 rows for 12 tickets)
  [XX ] Silver  T-91 shows its latest state: high / closed / bug  (got [('low', 'open', None), ('high', 'open', None), ('high', 'closed', 'bug')])
  [XX ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left  (got [(False, 'u06', 'Yêu cầu xoá tài khoản', 'Tôi là Nguyễn Văn An, email <EMAIL>, sđt <PHONE>. Xin xoá toàn bộ dữ liệu của tôi.'), (False, 'u06', 'Yêu cầu xoá tài khoản', 'Tôi là Nguyễn Văn An, email <EMAIL>, sđt <PHONE>. Xin xoá toàn bộ dữ liệu của tôi.')])
  [XX ] Gold    gold_feature_daily reconciles with a full recompute from Silver  (c50b8851affe != 8630e04a61d1)
  [XX ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12  (got (2, 0), expected (5, 1))
  [XX ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)  (LOOKBACK_DAYS=0 < 3)
  [XX ] Gold    latest training snapshot excludes the deleted ticket T-97  (1 row(s))
  [XX ] Gold    deletes propagate to the RAG index: no chunk of T-97  (2 chunk(s))
  [XX ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks  (22 rows / 9 chunks, embedded 0)
  [XX ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build  (see submission/checksums.txt)

RESULT: 8/18 checks — FAILURES ABOVE
```

**Sau khi sửa:**

```text
$ python -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ python -m pytest
..................................                                       [100%]
34 passed in 2.18s

$ python -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ python main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ python main.py --land-only; cd dbt_project; dbt build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
10:11:33  Running with dbt=1.12.5
10:11:33  Registered adapter: duckdb=1.11.0
10:11:37  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
10:11:42  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.16s]
10:11:43  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.06s]
10:11:43  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.19s]
10:11:43  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.26s]
10:11:43  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.22s]
10:11:43  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.08s]
10:11:44  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.04s]
10:11:44  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.04s]
10:11:44  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.05s]
10:11:44  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.04s]
10:11:44  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.04s]
10:11:44  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.03s]
10:11:44  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.04s]
10:11:44  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.03s]
10:11:44  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.03s]
10:11:44  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.05s]
10:11:44  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.10s]
10:11:44  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.06s]
10:11:44  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.06s]
10:11:44  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.07s]
10:11:44  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.08s]
10:11:44  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.08s]
10:11:44  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.57s]
10:11:44  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.04s]
10:11:45  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.04s]
10:11:45  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.04s]
10:11:45  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 8.02 seconds (8.02s).
10:11:45  Completed successfully
10:11:45  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ python -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

(Output dbt đã lược các dòng `START … [RUN]`; giữ nguyên mọi dòng kết quả.)
