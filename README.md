# Processing Big Data — Ringkasan Hasil Analisa (Task 1–3)

## 1. Ikhtisar

| Task | Fokus | Data yang dipakai | Status eksekusi |
|---|---|---|---|
| Task 1 — Data Ingestion | Baca CSV mentah → schema string → rename kolom → casting tipe → tulis parquet | `stocks.csv` 2020 (1 hari, 5.779 baris) |  Jalan penuh, tanpa error |
| Task 2 — Data Profiling | Cek manual 6 dimensi data quality | `stocks.csv` 1962 (1 hari, 20 ticker) |  Jalan penuh, tanpa error |
| Task 3 — Deequ Testing | Otomatisasi 6 dimensi yang sama pakai `pydeequ`, lintas tahun | `stocks.csv` 1963, 1974, 1985, 1996, 2007, 2018 (masing-masing 1 hari) |  Kode benar, tapi belum tereksekusi dengan `pydeequ` asli (lihat §4) |

**Catatan penting**: semua file `stocks.csv` yang tersedia — termasuk untuk 2020, 1962, dan keenam tahun Task 3 — ternyata berisi **satu hari perdagangan saja** per tahun (bukan satu tahun penuh seperti dataset asli EDSA). Ini konsisten di semua file, jadi kemungkinan besar memang begitu bentuk data yang disediakan untuk tugas ini.

---

## 2. Task 1 — Data Ingestion

- Bug yang ditemukan & diperbaiki: kolom `volume` tersimpan sebagai string desimal (`"1410500.0"`), sehingga cast langsung `string → bigint` gagal di bawah Spark ANSI mode. Solusi: cast dulu ke `DoubleType`, baru ke `LongType`.
- Hasil akhir: 0 nilai null di semua kolom sebelum & sesudah casting, parquet berhasil ditulis dengan `coalesce(2)` (ukuran data ~4.47 MB).

---

## 3. Task 2 — Data Profiling (data 1962, 20 ticker)

### Accuracy
- **`open > 2`**: `AA` (6.53), `ARNC` (6.13), `IBM` (7.63) — 3 ticker ini punya harga buka jauh di atas ticker lain.
- Tidak ada anomali ekstrem di `high`/`low`/`close`/`adj_close` relatif terhadap `open`.

### Completeness
- **0 nilai null** di semua kolom.
- **Zero values**: 10 dari 20 ticker punya `open = 0.0` padahal `high`/`low`/`close`-nya positif — `CVX, DTE, ED, FL, GT, IP, JNJ, MO, NAV, PG, XOM`.
- Imputasi rata-rata per stock **tidak dilakukan** — data cuma 1 hari per stock sehingga tidak ada riwayat untuk dihitung rata-ratanya.

### Consistency
- Semua 20 ticker cocok dengan metadata (tidak ada nama tidak konsisten) — **catatan: metadata dibuat manual untuk 20 ticker ini saja**, jadi hasil "konsisten" ini tidak memverifikasi terhadap file metadata resmi EDSA yang sebenarnya (5.000+ baris, tidak bisa diunduh penuh di sandbox).

### Timeliness
- Tidak bisa diuji gap antar tanggal maupun pola bulanan — data cuma 1 hari (1962-01-03).

### Uniqueness
- **0 duplikat** primary key (`stock` + `date`), **0 duplikat** baris penuh.

### Validity
- Semua ticker valid (sesuai metadata buatan), semua tanggal valid & di masa lalu, **tidak ada nilai negatif** di kolom numerik manapun.

---

## 4. Task 3 — Deequ Testing (6 tahun: 1963, 1974, 1985, 1996, 2007, 2018)

>  **`pydeequ` belum bisa dieksekusi di sandbox saya.** Library ini butuh PySpark 3.1–3.5 (sudah disesuaikan ke 3.5.1) dan men-download JAR `com.amazon.deequ` dari Maven Central saat runtime — domain itu diblokir di jaringan sandbox. Error persis: `com.amazon.deequ#deequ;2.0.8-spark-3.5: not found`.
>
> Kode di notebook sudah ditulis sesuai API resmi `pydeequ` (belum diverifikasi eksekusi). Sebagai gantinya, tabel di bawah ini dihitung dengan **PySpark biasa** (sudah tereksekusi dan tervalidasi) yang secara logis setara dengan 6 test Deequ yang diminta.

### Test 1 — Null values

| Tahun | Baris | Null (semua kolom numerik) |
|---|---|---|
| 1963 | 20 | 0 |
| 1974 | 177 | 0 |
| 1985 | 670 | 0 |
| **1996** | **1** | **6 dari 6 kolom — seluruh baris kosong** |
| 2007 | 3.008 | 0 |
| **2018** | **1** | **6 dari 6 kolom — seluruh baris kosong** |

→ **1996** (ticker `AGYS`) dan **2018** (ticker `CAH`) adalah kasus "data hilang total": satu-satunya baris di tahun itu tidak punya nilai numerik sama sekali.

### Test 2 — Zero values

| Tahun | open | volume |
|---|---|---|
| 1963 | 11 | 0 |
| 1974 | 95 | 11 |
| 1985 | 363 | 46 |
| 2007 | 0 | 131 |

→ Kolom `open` paling sering bernilai 0 di tahun-tahun awal (1963–1985); di 2007 justru `volume` yang lebih sering 0.

### Test 3 — Negative values

| Tahun | Ticker bermasalah | `adj_close` |
|---|---|---|
| 1974 | `CLF` | −0.0032 |
| 1985 | `AAN` | **−1.62 × 10²¹**  (data corrupt, bukan sekadar salah tanda) |
| 1985 | `CLF` | −0.0028 |
| 1985 | `ERIC` | −0.0168 |
| 2007 | `ITRN` | −0.1970 |
| 2007 | `TAK` | −0.0111 |

→ **`AAN` (1985) adalah anomali paling parah** di seluruh dataset — nilainya bukan cuma negatif tapi secara matematis tidak masuk akal (order magnitude 10²¹). Sisanya (`CLF`, `ERIC`, `ITRN`, `TAK`) negatif kecil mendekati nol, kemungkinan artefak perhitungan adjusted-close, tapi tetap melanggar aturan "semua nilai harus positif".

### Test 4 — Maximum values

| Tahun | max `open` | Catatan |
|---|---|---|
| 1963 | 5.45 | wajar |
| 1974 | 346.25 | wajar |
| 1985 | 58.750,00 | tinggi tapi masih masuk akal (kemungkinan sebelum stock split) |
| 2007 | **35.002.798.080,00** |  ticker **`TOPS`** — harga saham senilai puluhan miliar dolar, mustahil secara ekonomi, **data rusak** |

→ **`TOPS` (2007) adalah anomali paling ekstrem** dari sisi nilai maksimum — jauh melampaui ticker lain di tahun yang sama (ticker berikutnya, `CLRB`, hanya ~293 ribu).

### Test 5 — Stock tickers (validitas terhadap metadata)

- **Tidak bisa disimpulkan dengan andal.** Metadata yang tersedia cuma mencakup 20 ticker (dari sampel 1962/1963). Tahun-tahun lain punya jauh lebih banyak ticker unik (1974: 177, 1985: 670, 2007: 3.008) yang otomatis tidak akan "ketemu" di metadata — bukan karena ticker-nya invalid, tapi karena metadata resminya tidak bisa diunduh penuh di sandbox ini.

### Test 6 — Duplication

| Tahun | Duplikat primary key (`stock`+`date`) | Duplikat baris penuh |
|---|---|---|
| Semua 6 tahun | 0 | 0 |

→ Tidak ditemukan duplikat sama sekali di tahun manapun.

---

## 5. Ringkasan Temuan Utama (untuk MCQ)

1. **Anomali paling parah**: ticker `AAN` tahun 1985 (`adj_close` ≈ −1.62×10²¹) dan ticker `TOPS` tahun 2007 (`open` ≈ 35 miliar).
2. **Data hilang total**: tahun 1996 (`AGYS`) dan 2018 (`CAH`) — satu-satunya baris di tahun itu seluruhnya null.
3. **Zero values** paling banyak muncul di kolom `open`, terkonsentrasi di tahun-tahun lama (1963–1985).
4. **Tidak ada duplikat** primary key maupun baris penuh di data manapun yang diperiksa.
5. **Ticker/metadata validity** tidak bisa diuji tuntas karena keterbatasan akses ke file metadata resmi EDSA di sandbox ini.

---

## 6. Keterbatasan yang Perlu Diketahui Sebelum Submit

1. Task 1 pakai data 2020, bukan 1962 seperti soal asli.
2. Semua sampel per tahun (termasuk Task 3) hanya 1 hari perdagangan, bukan 1 tahun penuh — beberapa pengecekan (gap tanggal, pola bulanan, imputasi rata-rata historis) tidak bisa dilakukan secara bermakna.
3. Metadata ticker (`symbols_valid_meta_subset.csv`) dibuat manual untuk 20 ticker yang dikenal, bukan file resmi EDSA (5.000+ baris) — hasil uji konsistensi/validitas ticker di luar 20 ticker itu tidak bisa diandalkan.
4. Notebook Task 3 (`pydeequ`) **belum tereksekusi** di sandbox ini karena blokir akses ke Maven Central — wajib dijalankan ulang di environment sendiri (`pip install pyspark==3.5.1 pydeequ`, dengan akses internet penuh) sebelum submit final.
