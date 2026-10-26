# IPI ML — Data Inventory and Organization

Oct 26, 2026 · @Dat · Owner for execution: Joshua

## Principle

**Copy, don't organize.** Mirror each source exactly as received and let a script-built manifest do the organizing. The manifest becomes the `IMAGE_CATALOG` table from Phase 1, so the inventory and the catalog are one piece of work, not two.

- Never rename, move or edit files in `raw/`. Original paths carry vendor, IAC and structure number.
- Grouping (by pole, record, vendor, year) happens in tables and queries, not in folder layouts.
- Everything generated can be deleted and rebuilt from a script in the repo.

## Folder layout

The same layout works on the network drive now and in Azure Blob later, so the move is a straight copy and paths map one to one.

```
IPI_ML/
├── README.md              ← what each folder is, who owns it
├── raw/                   ← read-only, exact copies, never renamed
│   ├── vendor_a/
│   │   ├── 2024/          ← PDFs as received
│   │   ├── 2025/
│   │   └── 2026/          ← 04-Photos as received
│   ├── vendor_b/ …
│   └── _tabular/          ← spreadsheets people send us
│       └── 2025_decisions/2026-10-28/decisions.xlsx
├── derived/               ← generated, always rebuildable
│   └── pdf_extract/
│       └── vendor_a/2025/{pdf_name}/p01_i01.jpg
├── manifest/
│   ├── image_manifest_2026-10-30.parquet
│   └── profile_2026-10-30.md
└── reference/
    ├── MS454_5.5.pdf
    └── labeling_guide_v1.docx
```

### Rules

- Set `raw/` to read-only on the share once each copy finishes. Fixes go in the catalog, never by renaming files.
- Use ISO dates on every snapshot (`2026-10-30`) and never overwrite one. A new manifest run gets a new file.
- Spreadsheets get a dated snapshot folder under `raw/_tabular/`, because "final" files change.
- Photos never go in git. The repo holds scripts, SQL and the golden-set ID list.

## Group by record, not by pole

The label belongs to an **inspection record**, not a pole. The same pole can appear in 2024, 2025 and 2026 with different codes and photos. Merging those into one folder mixes years, leaks later photos into earlier labels and breaks the vendor-and-year split.

- Keep `raw/` as the vendor delivered it.
- Each prior-year PDF is one visual, so `derived/pdf_extract/{vendor}/{year}/{pdf_name}/` is already one folder per record.
- For browsing by pole or record, use a Snowflake view or FiftyOne grouped by record ID instead of a second copy of the files.

## Step 0: check the PDFs first

Prior years' photos are bundled 3–5 per PDF, and 2025 holds the labels.

1. Pick ~20 PDFs across vendors.
2. Extract the **embedded images** (not page renders) with pypdf. Avoid PyMuPDF, which is AGPL.
3. Check whether EXIF datetime and GPS survived. Some PDF tools keep the original JPEG intact; others re-encode and strip it.
4. If EXIF is stripped, 2025 gets no GPS or time-window checks. Ask vendors early whether the 2025 originals still exist.

## Image manifest: one row per image

| Column | Type | Notes |
| --- | --- | --- |
| `image_id` | string | Stable hash of source path + PDF page + image index, so reruns give the same ID |
| `source_path` | string | Path relative to `raw/` |
| `source_type` | string | `image` or `pdf` |
| `pdf_page` | int | Null for loose images |
| `pdf_image_index` | int | Null for loose images |
| `derived_path` | string | Path under `derived/` for PDF extracts |
| `file_ext` | string | Flag odd types (HEIC, video, TIFF) |
| `file_size_bytes` | int | |
| `md5` | string | Exact duplicates, and Azure transfer verification |
| `vendor` | string | Parsed from path |
| `iac` | string | Parsed from path |
| `structure_no` | string | Parsed from path |
| `cycle_year` | int | Parsed from path |
| `parse_status` | string | `ok`, `partial` or `failed` |
| `width`, `height` | int | |
| `readable` | bool | False if the file won't open |
| `has_exif` | bool | |
| `exif_datetime` | timestamp | |
| `gps_lat`, `gps_lon` | float | |
| `exif_orientation` | int | |
| `phash` | string | Perceptual hash for near-duplicates |
| `manifest_run_date` | date | |
| `blob_path` | string | Added after the Azure move |

## Order of work

1. **One vendor end to end.** Copy it, write that vendor's path-parsing regex, run the script, eyeball the results.
2. **All vendors.** Make the script resumable: skip rows already in the manifest by path and size.
3. **Profile.** Counts by vendor, IAC and year; share with GPS; parse failures; duplicates; unreadable files; odd file types. Write it to `manifest/profile_{date}.md`.
4. **Load to Snowflake** once the project schema exists. The manifest is metadata only, so it doesn't wait for Azure. Join to return data on structure number and year. The match rate is the real result: records with no photos, and photos with no record.
5. **Azure move** when provisioning lands. `azcopy copy` the `raw/` folder as-is with `--put-md5`, add `blob_path`, and verify file counts and MD5s against the manifest.

## Definition of done (Joshua)

- [ ] Script in the repo that rebuilds the manifest from scratch
- [ ] Manifest covering every vendor and year
- [ ] Every parse failure listed with its path
- [ ] One-page profile in `manifest/`
- [ ] Match rate between photos and return data, with both unmatched lists

## Non-photo sources

Inventory these alongside the photos. The 2025 decisions are the gating item: if they can't be tied to a record, nothing downstream works.

| Source | Where it lives | Row count | Join key | Date range | Owner |
| --- | --- | --- | --- | --- | --- |
| 2025 closeout decisions | | | | | Kevin / Jaime |
| Visual return data and notes | | | | | |
| SAP asset tables | | | | | |
| Obstruction list | | | | | |
| SAP quick-find queries (3C, 3D) | | | | | Jaime |
| MS454 Section 5.5 | `reference/` | n/a | n/a | | |

## Snowflake tables (project schema)

| Table | Grain | Purpose |
| --- | --- | --- |
| `IMAGE_CATALOG` | One row per image | Loaded from the manifest |
| `INSPECTION_RECORD` | One row per visual | Vendor, IAC, year, structure number, code, 2025 decision; the label table |
| `RECORD_IMAGE` | One row per record–image pair | Links records to their images |
| `GOLDEN_SET` | One row per record ID | Frozen; never edited, never trained on |

**Record ID:** use the return data's own unique ID if it has one. If not, combine vendor + year + structure number. Settle this in Phase 0, because every table and split hangs off it.
