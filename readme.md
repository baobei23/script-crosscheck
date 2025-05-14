# Script Crosscheck Usaha

Aplikasi ini digunakan untuk memverifikasi keberadaan bisnis/usaha dengan menggunakan Google Maps. Program akan mencari nama usaha beserta lokasinya di Google Maps dan memvalidasi keberadaannya secara otomatis.

## Fitur

- Pencarian otomatis di Google Maps dengan web scraping
- Ekstraksi nama bisnis dan lokasi dari hasil pencarian
- Validasi nama usaha dengan perbandingan fuzzy (kesamaan 75% atau lebih)
- Validasi lokasi bisnis (Kabupaten/Kota) dengan perbandingan fuzzy
- Proses banyak usaha secara paralel dengan 5 permintaan bersamaan
- Menyimpan hasil pencarian dalam format CSV

## Persyaratan

- Python 3.10 atau lebih baru
- Google Chrome browser
- Internet yang stabil
- File `bisnis.txt` yang berisi daftar bisnis

## Cara Penggunaan

### Windows

1. Jalankan file `run.bat` dengan klik dua kali
2. Script akan menginstal Python 3.10 (jika belum terinstal) dan semua dependensi yang diperlukan
3. Aplikasi akan langsung berjalan setelah instalasi selesai

### Manual

1. Pastikan Python 3.10 atau lebih baru sudah terinstal
2. Instal dependensi dengan perintah: `pip install -r requirements.txt`
3. Jalankan aplikasi dengan perintah: `python main.py`

## Format Data Input

File `bisnis.txt` harus berisi daftar bisnis dengan format:
```
NAMA BISNIS Kabupaten/Kota NAMA_LOKASI
```

Contoh:
```
ORGEN PELAMINAN <ALMADIN> Kabupaten Kepulauan Mentawai
FOTO COPY <ACIL> Kabupaten Kepulauan Mentawai
JASA PENGETIKAN <JIMI WILSON> Kabupaten Kepulauan Mentawai
```

## Hasil Output

Hasil akan disimpan dalam file bernama `hasil_crosscheck.csv` dengan format:
- Nama Usaha
- Hasil Crosscheck (Ditemukan/Tidak Ditemukan)

## Teknologi yang Digunakan

- [Botasaurus](https://github.com/omkarcloud/botasaurus) - Framework web scraping otomatis
- FuzzyWuzzy - Library untuk perbandingan string secara fuzzy
- BeautifulSoup - Parser HTML/XML