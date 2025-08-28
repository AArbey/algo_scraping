# This program is meant to grab every csv data file and zip them into one file

import zipfile
from pathlib import Path
from datetime import datetime

FILES = [
    "../AMAZON/amazon_offers.csv",
    "../CARREFOUR/scraping_carrefour.csv",
    "../CDISCOUNT/scraping_cdiscount.csv",
    "../LECLERC/scraping_leclerc.csv",
    "../RAKUTEN/Rakuten_data.csv"
]

def create_zip(output_name: str | None = None) -> Path:
    base_dir = Path(__file__).resolve().parent
    if not output_name:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_name = f"aggregated_csv_{timestamp}.zip"
    output_path = base_dir / output_name

    missing = []
    with zipfile.ZipFile(output_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for rel_path in FILES:
            file_path = (base_dir / rel_path).resolve()
            if file_path.exists():
                # Store only the filename inside the archive
                zf.write(file_path, arcname=file_path.name)
            else:
                missing.append(rel_path)

    if missing:
        print(f"Warning: missing files not added: {missing}")
    print(f"Created zip: {output_path}")
    return output_path


if __name__ == "__main__":
    create_zip()
