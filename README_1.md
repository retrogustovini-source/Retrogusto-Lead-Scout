# Retrogusto Lead Scout 🍷

Agente automatico che ogni lunedì mattina cerca nuovi lead (wine bar, enoteche, ristoranti) 
a Praga tramite Google Places API e invia un report su Telegram con il CRM xlsx aggiornato.

## Funzionamento

- Gira ogni **lunedì alle 7:00** (Railway cron)
- Cerca venue su Google Maps con query mirate (wine bar, vinoteka, enoteca, ristorante italiano...)
- Confronta con i lead già presenti nel CRM per evitare duplicati
- Aggiunge i nuovi lead in fondo al foglio "Leads" dell'xlsx
- Invia su Telegram:
  1. Messaggio con lista nuovi lead (nome, tipo, zona, telefono, sito, rating)
  2. File xlsx aggiornato pronto da caricare su Google Drive

## Variabili d'ambiente (Railway)

| Variabile | Descrizione |
|-----------|-------------|
| `GOOGLE_API_KEY` | API Key Google Cloud (Places API New abilitata) |
| `TELEGRAM_TOKEN` | Token del bot Telegram (@retrogusto_leads_bot) |
| `TELEGRAM_CHAT_ID` | Il tuo chat ID Telegram |
| `XLSX_PATH` | Percorso del file xlsx (default: `leads.xlsx`) |

## Deploy su Railway

1. Crea nuovo progetto su Railway
2. Collega questo repository GitHub
3. Aggiungi le variabili d'ambiente
4. Carica il file `leads.xlsx` nella root del progetto
5. Railway legge `railway.toml` e schedula il cron automaticamente

## File xlsx

Il file viene letto all'avvio per caricare i lead esistenti ed evitare duplicati.
Il file aggiornato viene salvato come `leads_updated_YYYY-MM-DD.xlsx` e inviato su Telegram.
L'originale non viene mai sovrascritto.

## Colonne aggiunte automaticamente

- Lead Name, Type, Language, Zone
- Contact Channels: Email
- Contact Quality: freddo
- Situation: cold mail
- Status: Waiting
- Priority: Media
- Note: include sito web e rating Google
- Next Action: Email follow-up
- Probability: 0.2
