import re
from pathlib import Path

base = Path(__file__).parent
sql = base / 'dump' / 'orari.dipendenti.sql'
csv = base / 'csv' / 'orari.dipendenti.csv'

issues = []

# pattern to extract NOME from INSERT lines in SQL: VALUES (..., 'NOME', ...)
# we will do a simple parse assuming columns order matches COLUMNS and NOME is second column
if sql.exists():
    with sql.open('r', encoding='utf-8') as f:
        for i, line in enumerate(f, 1):
            if line.strip().upper().startswith('INSERT INTO DIPENDENTI'):
                # find the VALUES(...) portion
                m = re.search(r'VALUES\s*\((.*)\)\s*;', line, flags=re.IGNORECASE)
                if not m:
                    continue
                vals = m.group(1)
                # split top-level commas (values may contain escaped quotes but not commas inside names)
                parts = [p.strip() for p in vals.split(',')]
                if len(parts) >= 2:
                    nome_val = parts[1]
                    # remove surrounding quotes if present
                    if nome_val.startswith("'") and nome_val.endswith("'"):
                        nome = nome_val[1:-1].replace("''", "'")
                    else:
                        nome = nome_val
                    if nome != ' '.join(nome.split()):
                        issues.append(f"SQL line {i}: NOME not normalized: >{nome}< -> normalized >{' '.join(nome.split())}<")

if csv.exists():
    import csv as _csv
    with csv.open('r', encoding='utf-8-sig', newline='') as f:
        reader = _csv.DictReader(f)
        for i, row in enumerate(reader, 2):
            nome = row.get('NOME', '')
            if nome != ' '.join(nome.split()):
                issues.append(f"CSV row {i}: NOME not normalized: >{nome}< -> normalized >{' '.join(nome.split())}<")

if not issues:
    print('OK: nessun problema trovato: tutte le colonne NOME sono normalizzate (nessun doppio spazio né spazi iniziali/finali).')
else:
    print('Trovati problemi:')
    for it in issues[:200]:
        print(it)
    if len(issues) > 200:
        print(f"...e altri {len(issues)-200} problemi")

