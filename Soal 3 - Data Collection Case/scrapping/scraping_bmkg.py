import asyncio
import httpx
import csv
import json
from bs4 import BeautifulSoup
from pathlib import Path

# URL target BMKG
URL = "https://www.bmkg.go.id/cuaca/prakiraan-cuaca.bmkg"

# Direktori untuk menyimpan data
DATA_DIR = Path("datasets")
DATA_DIR.mkdir(exist_ok=True)

# File untuk menyimpan halaman yang gagal diambil
SKIPPED_FILE = DATA_DIR / "skipped.json"
skipped_pages = []

async def fetch_page(client, url):
    """ Mengambil halaman dengan async request """
    try:
        response = await client.get(url, timeout=10)
        response.raise_for_status()  # Cek jika ada error HTTP
        return response.text
    except Exception as e:
        print(f"⚠️ Gagal mengambil halaman {url}: {e}")
        skipped_pages.append(url)
        return None

async def scrape_bmkg():
    """ Fungsi utama untuk scraping data cuaca BMKG """
    async with httpx.AsyncClient() as client:
        html = await fetch_page(client, URL)
        if html is None:
            return

        soup = BeautifulSoup(html, "html.parser")
        cities = soup.find_all("div", class_="prakicu-kota")

        data = []
        for city in cities:
            try:
                name = city.find("h2").text.strip()
                temperature = city.find("span", class_="heading-md").text.strip()
                weather = city.find("img")["alt"]
                data.append([name, temperature, weather])
            except Exception as e:
                print(f"⚠️ Gagal mengekstrak data untuk kota: {e}")

        # Simpan ke dalam CSV
        csv_file = DATA_DIR / "cuaca_bmkg.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["Kota", "Suhu", "Cuaca"])
            writer.writerows(data)

        print(f"✅ Data berhasil disimpan di {csv_file}")

        # Simpan halaman yang gagal diambil
        if skipped_pages:
            with open(SKIPPED_FILE, "w", encoding="utf-8") as file:
                json.dump(skipped_pages, file, indent=4)
            print(f"⚠️ Halaman yang gagal disimpan di {SKIPPED_FILE}")

# Jalankan script async
asyncio.run(scrape_bmkg())
