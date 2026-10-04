# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Phạm Long Nhật / 2A202602844
**Repo:** https://github.com/Nhatcony0902/K4-Track02-Day17-PhamLongNhat-2A202602844-DataPipelineEngineering
**Commit bài nộp:** `<hash commit cuối>`
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (Claude Opus 5.5) — đọc đề và code, chạy lệnh kiểm tra, đề xuất 3 chỗ sửa trong `pipeline/` và soạn nháp REPORT; tôi đã review từng dòng sửa và chạy lại toàn bộ kiểm tra.
**Nguồn tham khảo khác (nếu có):** slide Ngày 17; tài liệu Debezium (định dạng change event Postgres).

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `verify`: "24 rows for 12 tickets"; T-91 có 3 hàng `low/open`, `high/open`, `high/closed/bug` thay vì 1 | `gold_feature_daily` lệch full recompute (`c50b8851affe != 8630e04a61d1`); u05 ngày 08-12 ra `(2, 0)` thay vì `(5, 1)` | T-97 vẫn `is_deleted = False`, còn `user_id`, subject, body (“Nguyễn Văn An…”); còn 1 hàng trong snapshot `v2026-08-16` và 2 chunk trong RAG |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dedup *trong* batch rồi `INSERT` — không có khoá giữa các batch, mỗi lần chạy thêm hàng; chạy lại batch cũ còn thêm trạng thái cũ | `LOOKBACK_DAYS = 0` dựa trên giả định “event tới trong vài giây”; event offline của u05 (event time 08-12) land ngày 08-15, khi đó partition 08-12 không được tính lại | Staging lấy khoá từ `after.ticket_id`; với `op = 'd'` thì `after = null` → `ticket_id` null → bị `WHERE ticket_id IS NOT NULL` lọc mất, thao tác xoá không bao giờ tới Silver |
| **Cách sửa** | `pipeline/silver.py`: đổi `INSERT` thành `MERGE … ON ticket_id`, `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE` mọi cột, `WHEN NOT MATCHED THEN INSERT` | `pipeline/config.py`: `LOOKBACK_DAYS = 3` = ceil(P99 = 3.00) đo bằng `main.py --lateness`; mỗi run xoá-và-tính-lại `[day−3, day]` | `pipeline/staging.py`: `coalesce(after.ticket_id, before.ticket_id)`. Bản ghi xoá đi tới MERGE → hàng thành tombstone (PII null); Gold đã lọc `is_deleted` / `_op <> 'd'` nên xoá lan xuống |
| **Khái niệm trên slide** | Silver — có khoá; MERGE theo khoá; LSN guard (batch cũ không thắng batch mới) | Data về muộn: event time ≠ ingest time; lookback = P99 đo từ Bronze; overwrite-partition | CDC log-based (phong bì `before/after/op`); delete ≠ Kafka tombstone; “Xoá phải lan” |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (p50 = 0, p95 = 2.90, max = 3, n = 43) → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f` (C0 = C1 = C2 = C3)
- `make parity`: **PARITY** (`silver_tickets` 3c15dfd43701, `gold_feature_daily` 8630e04a61d1)
- Trước khi sửa: verify 8/18; sau khi sửa: 18/18, pytest 34 passed, dbt PASS=19.

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- **MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`:** ticket là *thực thể* thay đổi theo thời gian nên cần upsert theo `ticket_id` với LSN làm thứ tự (chạy lại batch cũ là no-op), còn feature là *aggregate theo ngày event* tính lại được hoàn toàn từ Silver, nên xoá rồi tính lại cả cửa sổ `[day−3, day]` vừa idempotent vừa bắt được event muộn.
- **Tombstone thay vì xoá hẳn hàng trong Silver:** giữ lại khoá + `_lsn` của lần xoá để một bản ghi `c/u` cũ hơn (replay, chạy lại batch cũ) không “hồi sinh” ticket — MERGE thấy LSN cũ hơn và bỏ qua; đổi lại hàng tồn tại mãi (chi phí nhỏ, có thể dọn định kỳ sau khi chắc chắn không còn replay).
- **Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ:** mô hình đã train trên `vX` phải tái lập được nguyên văn (reproducibility, audit); thay đổi mới (feedback muộn, xoá) đi vào version mới.
- **DuckDB (lite) / dbt cho bài toán cỡ này, chứ không phải Spark:** dữ liệu vài chục bản ghi/ngày chạy trong một process, không cần cluster; dbt cho sẵn merge/microbatch/contract/unit test và parity chứng minh hai cách cài đặt cho cùng kết quả. Spark chỉ đáng khi dữ liệu vượt RAM một máy.

## 4. Hai câu hỏi suy ngẫm

1. **Snapshot bất biến vs quyền được xoá.** Quyền xoá (pháp lý) thắng. Tôi tách *định danh version* khỏi *nội dung*: khi có yêu cầu xoá, tạo lại các snapshot chứa T-97 thành version kế thừa (ví dụ `v2026-08-12-r1`) đã loại T-97, đánh dấu bản gốc `revoked` rồi xoá vật lý file cũ (và xoá ở Bronze/raw, hoặc dùng crypto-shredding: mã hoá PII theo khoá từng user, xoá khoá là xoá dữ liệu). Ghi lại một audit log “snapshot X đã bị sửa vì yêu cầu xoá #…, đã loại N hàng” để vẫn giải thích được vì sao checksum đổi; model đã train trên dữ liệu đó cần được lên lịch train lại.
2. **PII ngoài email/số điện thoại (tên “Nguyễn Văn An”).** Đặt chốt ở ranh giới Bronze → Silver (không cột free-text nào ra khỏi Bronze mà chưa qua chốt): regex + NER tiếng Việt (ví dụ model NER/Presidio có recognizer tiếng Việt) thay tên bằng `<NAME>`, kèm một contract test ở Silver/Gold quét lại. Đo bằng một tập vàng có gán nhãn PII (precision/recall theo loại thực thể, mục tiêu recall cao vì bỏ sót đắt hơn che nhầm) và theo dõi tỉ lệ phát hiện trên dữ liệu thật mỗi ngày như một metric chất lượng; Bronze giữ raw nhưng giới hạn quyền truy cập và có TTL.

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
