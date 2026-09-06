# Catatan Pengerjaan Task 1–3 (Processing Big Data)

## Korelasi antar task
- **Task 1 (Ingestion)** membangun pipeline reusable: CSV mentah -> schema string
  -> rename kolom -> casting tipe data -> tulis parquet. Ini fondasi yang
  dipakai ulang di Task 2 & 3.
- **Task 2 (Profiling)** membaca parquet Task 1, lalu memeriksa 6 dimensi data
  quality **secara manual** (accuracy, completeness, consistency, timeliness,
  uniqueness, validity) untuk satu tahun (1962).
- **Task 3 (Deequ)** mengambil jenis-jenis pemeriksaan yang sama persis dari
  Task 2 (null, zero, negative, max value, ticker valid, duplikat), lalu
  **mengotomatisasinya** pakai library Deequ sehingga bisa dijalankan
  berulang untuk banyak tahun sekaligus.

## Keterbatasan penting - tolong dibaca sebelum submit

1. **Data yang tersedia**: Task 1 pakai sampel 1 hari tahun **2020**
   (`stocks_2020.csv`). Task 2 pakai sampel 1 hari tahun **1962**
   (`stocks_1962.csv`, 20 ticker). Task 3 sudah dapat data **nyata** untuk
   keenam tahun yang diminta (1963, 1974, 1985, 1996, 2007, 2018) - lihat
   `data/stocks_<tahun>.csv` dan parquet hasil ingest-nya di
   `data/stocks_parquet_<tahun>/`. Catatan: 1996 dan 2018 masing-masing
   cuma 1 baris dengan semua kolom numerik `null` (kasus "data hilang
   total"). Beberapa cell di Task 2 (gaps antar tanggal, imputasi rata-rata
   per bulan) tetap di-skip karena sampel 1962-nya cuma 1 hari.

   **Temuan anomali nyata dari data 6 tahun** (lihat `data/all_years_summary.json`
   untuk angka lengkap semua tahun):
   - **1985, ticker `AAN`**: `adj_close ≈ -1.62 × 10²¹` - data corrupt parah,
     bukan sekadar salah tanda.
   - **2007, ticker `TOPS`**: `open ≈ 35 miliar`, `volume = 0` - harga saham
     sebesar itu mustahil secara ekonomi, jelas data rusak.
   - `CLF` (1974 & 1985), `ERIC` (1985), `ITRN` & `TAK` (2007): `adj_close`
     sedikit negatif (mendekati nol) - kemungkinan artefak perhitungan
     adjusted-close, tapi tetap melanggar aturan "semua nilai harus positif".
   - Tidak ada duplikat primary-key (`stock`+`date`) maupun duplikat baris
     penuh di tahun manapun.

2. **`symbols_valid_meta_subset.csv`**: file metadata resmi dari S3 punya
   5.000+ baris dan tidak bisa di-download penuh dari sandbox saya. Saya
   bikin subset berisi 20 ticker yang relevan, dengan nama perusahaan asli
   (4 di antaranya - AA, ARNC, BA, CAT - terverifikasi langsung dari file
   S3 asli; sisanya nama umum yang saya tahu tapi tidak diverifikasi ulang).
   Karena itu, semua pengecekan "ticker valid / inconsistent naming" akan
   selalu lolos (trivial) - ini tidak akan menangkap kasus inconsistent
   naming asli dari predict.

3. **Task 3 (Deequ) belum bisa dieksekusi di sandbox saya.** `pydeequ` butuh
   PySpark 3.1-3.5 (sudah saya sesuaikan) dan men-download JAR
   `com.amazon.deequ` dari Maven Central saat runtime - domain itu diblokir
   di jaringan sandbox saya. Errornya persis:
   `com.amazon.deequ#deequ;2.0.8-spark-3.5: not found`.
   Kode di notebook Task 3 sudah saya tulis sesuai API resmi pydeequ, tapi
   **belum terverifikasi lewat eksekusi** - beda dengan Task 1 & 2 yang
   sudah saya jalankan penuh dan terbukti jalan tanpa error.

   **Sebagai gantinya**, saya tambahkan 1 cell di paling akhir notebook Task 3
   ("Ringkasan 6 tahun") yang menjalankan pengecekan setara (null, zero,
   negative, max, duplikat) pakai PySpark biasa untuk keenam tahun - **cell
   ini sudah dieksekusi dan hasilnya tersimpan di notebook**, jadi kamu tetap
   punya angka nyata untuk menjawab MCQ sambil menyiapkan environment
   `pydeequ` yang sebenarnya.

## Cara menjalankan Task 3 di komputermu sendiri
```
pip install pyspark==3.5.1 pydeequ
export SPARK_VERSION=3.5   # Windows PowerShell: $env:SPARK_VERSION="3.5"
```
Pastikan koneksi internet aktif (untuk download JAR Deequ dari Maven Central
saat sel `spark = SparkSession.builder...` pertama kali dijalankan), lalu
jalankan notebook seperti biasa. Untuk memenuhi instruksi asli, ulangi dengan
mengganti `year = 1962` menjadi 1963/1974/1985/1996/2007/2018 (butuh data
CSV/parquet asli untuk tahun-tahun tersebut, diproses lewat Task 1 lebih
dulu).

## Isi folder
- `Task1_data_ingestion/` - notebook ingestion (2020), **sudah dieksekusi**.
- `Task2_data_profiling/` - notebook profiling (1962), **sudah dieksekusi**.
- `Task3_automatic_data_quality_testing/` - notebook Deequ (1962), kode
  lengkap tapi **belum dieksekusi** (lihat poin 3 di atas).
- `data/` - CSV mentah, metadata subset, dan parquet 1962 hasil Task 1
  (dipakai Task 3).
