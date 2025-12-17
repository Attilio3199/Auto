#!/usr/bin/env python3
import os
import sys
import pyodbc
from dotenv import load_dotenv
import csv
from datetime import datetime, date

load_dotenv()

HOST = os.getenv("MSSQL_HOST")
PORT = os.getenv("MSSQL_PORT")
USER = os.getenv("MSSQL_USER")
PASSWORD = os.getenv("MSSQL_PASS")
DATABASE = os.getenv("MSSQL_DB")
DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")

def sql_literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (datetime, date)):
        return "'{}'".format(value.strftime('%Y-%m-%d'))
    s = str(value).replace("'", "''")
    return f"'{s}'"

QUERY = """
SELECT 
    D.RifCommPref AS Neg,
    D.Descrizione AS NOME,
    D.Ore_Sett,
    D.Codice AS CODICEPERSONALE,
    L.Livello,
    CASE 
        WHEN Agg.Max_a_data_attivo = CAST('1900-01-01 00:00:00' AS DATE)
        THEN CAST('2099-12-31 00:00:00' AS DATE)
        ELSE Agg.Max_a_data_attivo
    END AS DATA_ASSUNZIONE,
    Agg.Min_da_data_attivo AS DATA_FINE_CONTRATTO,
    D.Ore_Lun AS Lunedi,
    D.Ore_Mar AS Martedi,
    D.Ore_Mer AS Mercoledi,
    D.Ore_Gio AS Giovedi,
    D.Ore_Ven AS Venerdi,
    D.Ore_Sab AS Sabato,
    D.Ore_Dom AS Domenica
FROM Tk_TabDipendenti AS D
INNER JOIN (
    SELECT 
        coddip,
        MIN(da_data_attivo) AS Min_da_data_attivo,
        MAX(a_data_attivo) AS Max_a_data_attivo
    FROM tk_Tab_DettDip
    GROUP BY coddip
) AS Agg
    ON D.Codice = Agg.coddip
INNER JOIN Tk_Tab_LivContDip AS L
    ON D.Codice = L.CodiceDip
WHERE D.Attivo = 1
  AND D.RifCommPref IS NOT NULL
  AND D.RifCommPref <> ''
  AND D.RifCommPref NOT IN ('WEB','AAA','AAAAA')
ORDER BY D.RifCommPref ASC;
"""

COLUMNS = [
    "Neg",
    "NOME",
    "Ore_Sett",
    "CODICEPERSONALE",
    "Livello",
    "DATA_ASSUNZIONE",
    "DATA_FINE_CONTRATTO",
    "Lunedi",
    "Martedi",
    "Mercoledi",
    "Giovedi",
    "Venerdi",
    "Sabato",
    "Domenica",
]

def main():
    try:
        if not HOST or not PORT:
            print('XXX')
            sys.exit(1)

        server = f"{HOST},{PORT}"
        if USER and PASSWORD:
            conn_str = (
                f"DRIVER={{{DRIVER}}};SERVER={server};DATABASE={DATABASE or ''};UID={USER};PWD={PASSWORD};Encrypt=no;"
            )
        else:
            conn_str = (
                f"DRIVER={{{DRIVER}}};SERVER={server};DATABASE={DATABASE or ''};Trusted_Connection=yes;Encrypt=no;"
            )

        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        DUMP_DIR = os.path.join(BASE_DIR, "dump")
        CSV_DIR = os.path.join(BASE_DIR, "csv")
        SQL_FILENAME = os.path.join(DUMP_DIR, "orari.dipendenti.sql")
        CSV_FILENAME = os.path.join(CSV_DIR, "orari.dipendenti.csv")

        CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dipendenti (
  Neg varchar(10) DEFAULT NULL,
  NOME varchar(100) DEFAULT NULL,
  Ore_Sett int(11) DEFAULT NULL,
  CODICEPERSONALE varchar(20) NOT NULL,
  Livello int(11) DEFAULT NULL,
  DATA_ASSUNZIONE date DEFAULT NULL,
  DATA_FINE_CONTRATTO date DEFAULT NULL,
  Lunedi int(11) DEFAULT 0,
  Martedi int(11) DEFAULT 0,
  Mercoledi int(11) DEFAULT 0,
  Giovedi int(11) DEFAULT 0,
  Venerdi int(11) DEFAULT 0,
  Sabato int(11) DEFAULT 0,
  Domenica int(11) DEFAULT 0,
  PRIMARY KEY (CODICEPERSONALE)
)
"""

        os.makedirs(DUMP_DIR, exist_ok=True)
        os.makedirs(CSV_DIR, exist_ok=True)

        with pyodbc.connect(conn_str, timeout=10) as conn:
            cur = conn.cursor()
            cur.execute(QUERY)
            rows = cur.fetchall()

        # write SQL dump
        with open(SQL_FILENAME, "w", encoding="utf-8") as fsql:
            fsql.write(CREATE_TABLE_SQL)
            fsql.write('\n\n')
            fsql.write('DELETE FROM dipendenti;\n\n')
            for row in rows:
                values = []
                for i, col in enumerate(COLUMNS):
                    try:
                        val = row[i]
                    except Exception:
                        val = None
                    if col == "NOME" and val is not None:
                        try:
                            val = ' '.join(str(val).split())
                        except Exception:
                            pass
                    values.append(sql_literal(val))
                cols_sql = ", ".join(COLUMNS)
                vals_sql = ", ".join(values)
                fsql.write(f"INSERT INTO dipendenti ({cols_sql}) VALUES ({vals_sql});\n")

        # write CSV
        with open(CSV_FILENAME, "w", encoding="utf-8-sig", newline="") as fcsv:
            writer = csv.writer(fcsv)
            writer.writerow(COLUMNS)
            for row in rows:
                row_vals = []
                for i, col in enumerate(COLUMNS):
                    try:
                        v = row[i]
                    except Exception:
                        v = None
                    if col == "NOME" and v is not None:
                        try:
                            v = ' '.join(str(v).split())
                        except Exception:
                            pass
                    if isinstance(v, (datetime, date)):
                        row_vals.append(v.strftime('%Y-%m-%d'))
                    elif v is None:
                        row_vals.append("")
                    else:
                        row_vals.append(str(v))
                writer.writerow(row_vals)

        print('$$$')
    except Exception:
        print('XXX')
        sys.exit(1)

if __name__ == '__main__':
    main()
