import os
import sys
import pyodbc
from dotenv import load_dotenv
import csv
from datetime import datetime, date

# Carica .env dalla directory corrente (se presente)
load_dotenv()

# Contratto (input/output):
# - Input: MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASS, MSSQL_DB (da .env o env vars)
# - Output: stampa "Collegato con successo" alla console se la connessione ha successo;
#           esegue la query fornita, scrive:
#             - C:\Users\Riparazioni2\Desktop\Debby\Auto\dump\orari.dipendenti.sql
#             - C:\Users\Riparazioni2\Desktop\Debby\Auto\csv\orari.dipendenti.csv

HOST = os.getenv("MSSQL_HOST")
PORT = os.getenv("MSSQL_PORT")
USER = os.getenv("MSSQL_USER")
PASSWORD = os.getenv("MSSQL_PASS")
DATABASE = os.getenv("MSSQL_DB")

if not HOST or not PORT:
    print("Errore: MSSQL_HOST e MSSQL_PORT devono essere impostati nel file .env o nelle variabili d'ambiente.")
    print("Vedi .env.example per il formato.")
    sys.exit(1)

# Driver ODBC da usare. Cambiare se sul sistema è installata una versione diversa.
DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")

server = f"{HOST},{PORT}"

# Costruisce la stringa di connessione; se non sono forniti USER/PASSWORD, tenta una connessione 'trusted' (integrated security)
if USER and PASSWORD:
    conn_str = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={server};"
        f"DATABASE={DATABASE or ''};"
        f"UID={USER};"
        f"PWD={PASSWORD};"
        "Encrypt=no;"
    )
else:
    # Trusted connection (Integrated Security) - su Windows richiede che l'utente abbia permessi
    conn_str = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={server};"
        f"DATABASE={DATABASE or ''};"
        "Trusted_Connection=yes;"
        "Encrypt=no;"
    )

# Query fornita dall'utente
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

# Percorsi output (assunti dalla richiesta dell'utente)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DUMP_DIR = os.path.join(BASE_DIR, "dump")
CSV_DIR = os.path.join(BASE_DIR, "csv")
SQL_FILENAME = os.path.join(DUMP_DIR, "orari.dipendenti.sql")
CSV_FILENAME = os.path.join(CSV_DIR, "orari.dipendenti.csv")

# Schema di destinazione per il dump (testo statico richiesto dall'utente)
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

# Ordine colonne per INSERT e CSV
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


def sql_literal(value):
    """Formatta un valore Python come literal SQL adatto per INSERT.
    - None -> NULL
    - str -> 'escaped'
    - int/float -> as is
    - date/datetime -> 'YYYY-MM-DD'
    """
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (datetime, date)):
        return "'{}'".format(value.strftime('%Y-%m-%d'))
    # per sicurezza, convertiamo a stringa e scappiamo eventuali singole virgolette
    s = str(value).replace("'", "''")
    return f"'{s}'"


try:
    with pyodbc.connect(conn_str, timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute(QUERY)
            rows = cur.fetchall()

        print("Collegato con successo")

        # Prepara directory di output
        os.makedirs(DUMP_DIR, exist_ok=True)
        os.makedirs(CSV_DIR, exist_ok=True)

        # Scrive file SQL di dump
        with open(SQL_FILENAME, "w", encoding="utf-8") as fsql:
            fsql.write(CREATE_TABLE_SQL)
            fsql.write('\n\n')
            fsql.write('DELETE FROM dipendenti;\n\n')

            # Genera INSERT per ogni riga
            for row in rows:
                # row è una pyodbc.Row che si comporta come sequenza secondo l'ordine della query
                values = []
                for i, col in enumerate(COLUMNS):
                    # Se ci sono meno colonne nella query rispetto a COLUMNS, usiamo NULL
                    try:
                        val = row[i]
                    except Exception:
                        val = None
                    # Normalizza la colonna NOME: rimuove spazi multipli e spazi iniziali/finali
                    if col == "NOME" and val is not None:
                        try:
                            val = ' '.join(str(val).split())
                        except Exception:
                            pass
                    # Per date/datetime che possono essere stringhe, proviamo a convertire
                    if isinstance(val, str):
                        # se è una stringa che rappresenta una data con tempo, tentiamo il parse
                        try:
                            # pyodbc normalmente ritorna date/datetime per campi datetime/date
                            # ma in caso sia stringa controlliamo solo formati comuni
                            if len(val) >= 10 and val[4] == '-' and val[7] == '-':
                                # keep as string, sql_literal la renderà correttamente
                                pass
                        except Exception:
                            pass
                    values.append(sql_literal(val))

                cols_sql = ", ".join(COLUMNS)
                vals_sql = ", ".join(values)
                fsql.write(f"INSERT INTO dipendenti ({cols_sql}) VALUES ({vals_sql});\n")

        # Scrive CSV
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
                    # Normalizza la colonna NOME anche per il CSV
                    if col == "NOME" and v is not None:
                        try:
                            v = ' '.join(str(v).split())
                        except Exception:
                            pass
                    # Normalizziamo le date a YYYY-MM-DD
                    if isinstance(v, (datetime, date)):
                        row_vals.append(v.strftime('%Y-%m-%d'))
                    elif v is None:
                        row_vals.append("")
                    else:
                        row_vals.append(str(v))
                writer.writerow(row_vals)

        print(f"Dump SQL scritto in: {SQL_FILENAME}")
        print(f"CSV scritto in: {CSV_FILENAME}")

except Exception as e:
    print("Errore connessione o query:", str(e))
    sys.exit(2)
