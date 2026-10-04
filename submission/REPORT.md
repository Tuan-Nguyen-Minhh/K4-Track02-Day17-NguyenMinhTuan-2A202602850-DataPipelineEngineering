# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV: Nguyen Minh Tuan - 2A202602850**

**Repo: https://github.com/Tuan-Nguyen-Minhh/K4-Track02-Day17-NguyenMinhTuan-2A202602850-DataPipelineEngineering**

**Commit bài nộp:**

**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Gemini, Qwen và OpenCode — dùng để đọc
code và giải thích log, gợi ý nguyên nhân và soạn văn bản REPORT. Toàn bộ thay đổi mã nguồn
trong `pipeline/` đã được tự đọc lại, tự chạy và tự đối chiếu output thực tế
(`verify` 18/18, `pytest` 34 passed, `rerun3` PASS, `parity` PARITY) trước khi nộp.

**Nguồn tham khảo khác (nếu có):**

## 1. Ba lỗi

Triệu chứng = thứ *thấy* đầu tiên. Chi tiết output ở mục 5.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | verify fail "one row per ticket_id": 24 dòng cho 12 ticket; T-91 có 3 bản | verify fail u05 08-12: got (2, 0), expected (5, 1); checksum Gold lệch full recompute | verify fail: T-97 không phải tombstone (còn user_id/subject/body); vẫn trong snapshot mới nhất + 2 chunk RAG |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng `INSERT INTO` trần: chạy lại ngày nào cũng thêm dòng, không có khoá | `LOOKBACK_DAYS = 0`: run chỉ recompute partition của chính nó, nên event tới muộn 3 ngày không quay lại ngày của nó | `ticket_changes_sql` chỉ lấy khoá từ `after`; delete Debezium có `after=null` → khoá NULL → bị `WHERE ticket_id IS NOT NULL` loại mất |
| **Cách sửa** (file) | `silver.py`: `MERGE ... ON t.ticket_id=s.ticket_id`, `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT` | `config.py`: `LOOKBACK_DAYS = 3` = ceil(P99) đo từ Bronze | `staging.py`: `ticket_id = coalesce(after.ticket_id, before.ticket_id)`; giữ tombstone `is_deleted=true` thay vì xoá dòng |
| **Khái niệm trên slide** | "Silver có khoá": MERGE theo khoá, LSN quyết định thay đổi nào mới hơn | event time ≠ ingest time; lookback = P99 đo từ Bronze, recompute theo partition | "Xoá phải lan": delete Debezium ≠ Kafka tombstone; xoá xuyên Silver → snapshot → RAG |

## 2. Các con số

- Lateness từ Bronze (43 bản ghi): P50 `0.00`, P95 `2.90`, P99 `3.00` (max 3) → `LOOKBACK_DAYS = 3`
- `checksums.txt`: **PASS** — Gold `39e115c510ec…`, C0 = C1 = C2 = C3
- `parity`: **PARITY**; `dbt`: **PASS=19 ERROR=0**; `verify`: **18/18**; `pytest`: **34 passed**

## 3. Lựa chọn công cụ / kỹ thuật (vì sao)

- **MERGE theo khoá** cho ticket (thực thể có vòng đời → phải so phiên bản bằng LSN), **overwrite-partition** cho feature (tổng hợp theo ngày không có phiên bản → xoá rồi tính lại rẻ và idempotent).
- **Tombstone** thay vì xoá dòng: xoá là sự kiện trong log CDC; xoá hẳn làm mất LSN nên replay batch cũ sẽ hồi sinh ticket. Đánh đổi: giữ dòng vĩnh viễn → cần TTL/purge.
- **Snapshot dựng lại từ Bronze "as of"**: mọi version tái lập được, đổi code về sau không phá checksum (lệch thì `SnapshotImmutableError` bắt ngay).
- **DuckDB + dbt**, không phải Spark: 7 ngày / 39 event chạy vài giây, zero-key, kiểm chứng bằng checksum; dbt cho microbatch/merge/test để đối chiếu parity.
- **Lookback = ceil(P99) = 3**: P99 = 3.00 ngày nên 3 là ngắn nhất bao phủ 99% event trễ, làm tròn lên vì lookback đếm ngày lịch; nhỏ hơn thì mất event u05 08-12, lớn hơn chỉ tốn recompute.

## 4. Hai câu hỏi suy ngẫm

**1. Snapshot `v08-12..v08-14` vẫn chứa văn bản T-97 (xoá ngày 08-15). Bất biến vs quyền được xoá?**

Tách hai mục tiêu: giữ bất biến cho *tính tái lập*, xử quyền xoá bằng cơ chế khác (không sửa tại chỗ). Ghi **deletion ledger** (`ticket_id`, `_lsn`, ngày xoá) và chỉ cho train trên snapshot `v >= ngày xoá`; snapshot mới đã lọc `_op='d'` nên mô hình sau 08-15 không học từ T-97. Với GDPR/PII cứng: ghi thêm **bản ghi xoá có version mới** (append-only erasure) rồi purge theo retention window (ví dụ 90 ngày). Đánh đổi: checksum version cũ đổi → phải công bố "đã xoá vì lý do bảo vệ dữ liệu", khác hẳn sửa dữ liệu để làm test pass. Lab này chỉ chứng minh xoá lan tới snapshot mới và RAG, **không phải** xoá PII đầy đủ cho production.

**2. Regex che email/SĐT còn tên "Nguyễn Văn An". Chốt PII nào, ở tầng nào, đo ra sao?**

*Trực tiếp* (tên, email, SĐT, địa chỉ, CCCD): che ngay ở **Silver lúc đọc từ Bronze** — tầng sau chỉ là bản sao, phải chặn từ đầu; regex chỉ đủ cho mẫu cứng, tên tiếng Việt cần danh mục tên + duyệt mẫu của người, thay bằng `[PERSON]` để giữ ngữ nghĩa phân loại. *Gián tiếp* (`user_id`, `ticket_id` nối về người thật): **pseudonymize** bằng token bất biến `hmac(salt, id)` trong secret vault — vẫn join được theo người nhưng không đọc ra danh tính. *Đo*: tập mẫu ~500 dòng có nhãn mỗi quý, đo precision/recall lớp PII (target recall ≥ 0.95) và `leak_rate` bằng regex+NER trên mọi cột `text` của Silver/Gold, alert khi > 0 — mở rộng đúng check "no email / phone survives past Bronze" sẵn có, vì RAG và training set là nơi dữ liệu thoát ra ngoài nhiều nhất.

## 5. Output (dán nguyên văn)

Chạy trên Windows PowerShell, từ thương mục gốc repo (đường dẫn tương đương cho macOS/Linux
nằm trong [SUBMISSION.md](../docs/SUBMISSION.md)).

```text
> .\.venv\Scripts\python.exe -m scripts.verify

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
```

```text
> .\.venv\Scripts\python.exe -m pytest

..................................                                       [100%]
34 passed in 3.89s
```

```text
> .\.venv\Scripts\python.exe -m scripts.rerun_check

# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
```

```text
> .\.venv\Scripts\python.exe main.py --lateness

event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
```

```text
> .\.venv\Scripts\python.exe main.py --land-only
  2026-08-10  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-11  tickets:already-landed(3)  events:already-landed(5)  transcripts:already-landed(2)
  2026-08-12  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-13  tickets:already-landed(3)  events:already-landed(7)  transcripts:already-landed(1)
  2026-08-14  tickets:already-landed(4)  events:already-landed(4)  transcripts:already-landed(1)
  2026-08-15  tickets:already-landed(4)  events:already-landed(8)  transcripts:already-landed(1)
  2026-08-16  tickets:already-landed(4)  events:already-landed(7)  transcripts:already-landed(2)

> # dbt build chạy trên database sạch (đã xoá dbt.duckdb + target/ để cả 7 ngày
> # seed đều được build thật, không dùng lại kết quả cũ)
> $env:DO_NOT_TRACK = '1'
> cd dbt_project
> ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17

16:38:49  Running with dbt=1.12.5
16:38:49  Registered adapter: duckdb=1.11.0
16:38:49  Unable to do partial parsing because saved manifest not found. Starting full parse.
16:38:51  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
16:38:51  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.07s]
16:38:51  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.03s]
16:38:51  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.12s]
16:38:52  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.11s]
16:38:52  15 of 19 PASS unique_silver_tickets_ticket_id ............................ [PASS in 0.03s]
16:38:52  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
16:38:52  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.04s]
16:38:52  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.06s]
16:38:52  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.03s]
16:38:52  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.04s]
16:38:52  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.03s]
16:38:52  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.05s]
16:38:52  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.05s]
16:38:52  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.32s]
16:38:52  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.03s]
16:38:53  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.50 seconds (1.50s).
16:38:53  Completed successfully
16:38:53  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
```

Mốc `--event-time-end 2026-08-17` là **biên không bao phủ**, nên đúng 7 batch
`2026-08-10 .. 2026-08-16` được xử lý — Batch 7 là `2026-08-16`, không có batch `08-17`.

**Ý nghĩa từng cấu hình dbt** (cùng ý bản Python, nhưng do dbt thực thi thay):

| Cấu hình | Ý nghĩa | Tương đương bên Python |
|---|---|---|
| `unique_key='ticket_id'` | Khoá thực thể: mỗi `ticket_id` đúng một hàng, dbt sinh `MERGE ... ON ticket_id` | `MERGE INTO silver_tickets ON t.ticket_id = s.ticket_id` |
| `merge_update_condition='..._lsn > ..._lsn'` | Chỉ ghi đè khi thay đổi mới hơn — batch cũ chạy sau không hồi sinh trạng thái mới | `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE` |
| `batch_size='day'` + `event_time='event_date'` | Mỗi lần chạy chia batch theo ngày *event*, mỗi ngày một partition độc lập | `build_feature_daily(con, day)` theo ngày ingest |
| `lookback=3` | Mỗi batch còn tính lại 3 ngày trước để nhận event đến muộn — cùng `LOOKBACK_DAYS = 3 = ceil(P99)` | `start = config.shift(day, -config.LOOKBACK_DAYS)` |
| `incremental_strategy='append'` + `not exists` (`silver_events`) | Event là sự kiện bất biến: chỉ chèn cái chưa thấy, không có đường update | `WHEN NOT MATCHED THEN INSERT` trên `event_id` |

```text
> .\.venv\Scripts\python.exe -m scripts.parity

=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

`scripts/parity.py` tự `fresh_build()` lại pipeline Python trước rồi mới so checksum với
`dbt_project/dbt.duckdb` đã build, nên hai bảng được so trên cùng một Bronze.

### Bằng chứng đường xoá (thử thách 3)

Hai bản ghi T-97 trong `data/cdc/tickets/2026-08-15.jsonl` là **hai thứ khác nhau**:

```text
offset=19  key={'ticket_id': 'T-97'}
  value.op         = 'd'
  value.after      = None       <-- CDC DELETE: mang thay đổi (before + lsn 24020000)
  value.before     = ticket_id=T-97, status='closed'
offset=20  key={'ticket_id': 'T-97'}
  value            = null       <-- KAFKA TOMBSTONE: chỉ phục vụ compaction, KHÔNG phải thay đổi
```

Khoá khi `after` là null nằm ở `before`, nên `ticket_changes_sql` đọc
`coalesce(after.ticket_id, before.ticket_id)`; còn tombstone Kafka không có `op` nên bị loại ở
`WHERE _op IS NOT NULL` (vẫn được giữ nguyên trong Bronze theo cam kết append-only).

```text
STEP 3/5 — tombstone ở silver_tickets
  silver_tickets T-97 -> is_deleted=True, user_id=None, subject=None, body=None
                      priority=None, status=None, _lsn=24020000, _batch_id=2026-08-15
  (n, n_distinct) = (12, 12)  <- vẫn đúng một hàng mỗi ticket

  SCD2 history của T-97 (bản xoá là bản hiện hành):
    lsn=24006000  2026-08-11 10:05 -> 2026-08-12 11:00  medium/open   is_deleted=False is_current=False
    lsn=24011000  2026-08-12 11:00 -> 2026-08-15 09:00  medium/closed is_deleted=False is_current=False
    lsn=24020000  2026-08-15 09:00 -> NULL               NULL/NULL     is_deleted=True  is_current=True

STEP 5b — replay batch cũ 2026-08-12 (thay đổi mới nhất của T-97 trong batch là UPDATE,
           lsn nhỏ hơn lsn của lệnh xoá)
  T-97 -> is_deleted=True, user_id=None, subject=None, _lsn=24020000, _batch_id=2026-08-15
  RESURRECTED? no — tombstone giữ nguyên

STEP 6 — xoá lan xuống Gold
  gold_training_set T-97: v2026-08-12 = 1, v2026-08-13 = 1, v2026-08-14 = 1  (snapshot cũ: bất biến có chủ đích)
  latest snapshot v2026-08-16: 0 row(s)    gold_doc_chunks: 0 chunk(s)
```

Như `[!NOTE]` của đề bài nhắc: đây **không phải** cơ chế xoá PII đầy đủ cho production — snapshot
cũ vẫn giữ văn bản của T-97 (xem câu 1 ở mục 4).

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
