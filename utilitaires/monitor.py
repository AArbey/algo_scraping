import os
import io
import logging
import pwd
import re
import signal
import shutil
import tempfile
import asyncio
import functools
import typing
import datetime

import discord
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from discord.ext import tasks


# --- Logging setup for systemd/journald ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def drop_privileges():
    if os.getuid() != 0:
        return  # Déjà exécuté en tant qu'utilisateur normal
    
    # Identifiants de l'utilisateur scraping
    user_name = "scraping"
    user_info = pwd.getpwnam(user_name)
    user_uid = user_info.pw_uid
    user_gid = user_info.pw_gid
    
    # Définir le groupe
    os.setgid(user_gid)
    # Définir l'utilisateur
    os.setuid(user_uid)
    # Définir le HOME
    os.environ['HOME'] = f'/home/{user_name}'

# Changer d'utilisateur
drop_privileges()

# 1) Configure your files, columns, and webhook URL here:
FILES_TO_MONITOR = {
    "/home/scraping/algo_scraping/AMAZON/amazon_offers.csv": ["pfid","idsmartphone","url","timestamp","Price","shipcost","seller","offertype","descriptsmartphone","batch_id"],
    "/home/scraping/algo_scraping/CARREFOUR/scraping_carrefour.csv": ["Platform","Product Name","Seller","Delivery Info","Price","Seller Rating","Timestamp","Batch ID"],
    "/home/scraping/algo_scraping/CDISCOUNT/scraping_cdiscount.csv": ["Platform","Product Name","Price","Product state","Seller","Seller Status","Seller Rating","Delivery Fee","Timestamp","Batch ID"],
    "/home/scraping/algo_scraping/LECLERC/scraping_leclerc.csv": ["Platform","Product Name","Seller","Price","Delivery Fees","Delivery Date","Product State","Seller Rating","Timestamp","Batch ID"],
    "/home/scraping/algo_scraping/RAKUTEN/Rakuten_data.csv": ["pfid","idsmartphone","url","timestamp","price","shipcost","rating","ratingnb","offertype","shipcountry","sellercountry","seller","batch_id"]
    #"/home/scraping/algo_scraping/FNAC/fnac_offers.csv": ["pfid","idsmartphone","url","timestamp","Price","shipcost","product_rating","seller","seller_rating","seller_sales_count","seller_rating_count","offertype","shipcountry","sellercountry","descriptsmartphone","batch_id"]
}


# Discord bot credentials (set these as environment variables)
BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", 0))  # The channel ID where alerts will be sent

EMBED_COLOR = 0x3498DB  # Default embed color (blue)


# Validate configuration
if not BOT_TOKEN:
    raise RuntimeError("DISCORD_BOT_TOKEN environment variable is not set.")
if CHANNEL_ID == 0:
    raise RuntimeError("DISCORD_CHANNEL_ID environment variable is not set or invalid.")

# Ensure a font that contains fullwidth parentheses is used
mpl.rcParams['font.family'] = ['Noto Sans CJK JP', 'DejaVu Sans', 'sans-serif']

# -------- State Tracking --------
# Keep track of which alerts are currently active (path, column)
active_alerts: set[tuple[str, str]] = set()
# Keep track of which low price alerts have been sent (path, column, row index)
low_price_alerts: set[tuple[str, str, int]] = set()
monitor_task_started = False

# -------- Helper Functions --------
def format_error(message: str) -> str:
    """
    Format a message as an @everyone mention with bold Markdown for Discord.
    """
    return f"@everyone **🚨 ERREUR**: {message}"

def format_recovery(message: str) -> str:
    """
    Format a recovery message when an issue is resolved.
    """
    return f"✅ **RÉSOLU**: {message}"

def format_low_price(message: str) -> str:
    """
    Format a low-price alert message for Discord.
    """
    return f"ℹ️ **PRIX BAS**: {message}"

# Embed colors by type
COLOR_INFO     = 0x3498DB  # blue
COLOR_ERROR    = 0xE74C3C  # red
COLOR_RECOVERY = 0x2ECC71  # green

async def send_message(channel, content: str, color: int = COLOR_INFO):
    """
    Send a message to Discord as an embed with a border.
    """
    try:
        embed = discord.Embed(description=content, color=color)
        await channel.send(embed=embed)
    except Exception as e:
        logging.error("Could not send message to Discord: %s", e)

def to_thread(func: typing.Callable) -> typing.Coroutine:
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        return await asyncio.to_thread(func, *args, **kwargs)
    return wrapper

# -------- Monitoring Logic --------
@to_thread
def check_file(path: str, columns: list[str]) -> tuple[list[str], list[str]]:
    new_alerts: list[str] = []
    resolved_alerts: list[str] = []

    try:
        logging.info("Checking file: %s", path) # Check with separator ';' or ','
        # Try reading with both separators, fallback to ',' if ';' fails
        logging.info("Reading offers file with ';' separator")
        df = pd.read_csv(path, sep=';', low_memory=False)
        logging.info("Offers file read successfully")
    except Exception as e:
        key = (path, "__read_error__")
        msg = f"Impossible de lire `{path}`: {e}"
        if key not in active_alerts:
            new_alerts.append(format_error(msg))
            active_alerts.add(key)
        return new_alerts, resolved_alerts

    for col in columns:
        logging.info("Checking column: %s in file: %s", col, path)
        key = (path, col)
        if col not in df.columns:
            msg = f"Colonne `{col}` introuvable dans `{path}`."
            if key not in active_alerts:
                new_alerts.append(format_error(msg))
                active_alerts.add(key)
            continue
        
        # If file fnac and columns seller_rating	seller_sales_count	seller_rating_count, check the last 20 rows
        if path.endswith("fnac_offers.csv") and col in ["seller_rating", "seller_sales_count", "seller_rating_count", "product_rating"]:
            last_lines = df[col].tail(20)
        # If file amazon and columns rating, check the last 40 rows
        elif path.endswith("amazon_offers.csv") and col == "rating":
            last_lines = df[col].tail(50)
        else:
            last_lines = df[col].tail(10)

        # helper: null or empty/"N/A"
        def _is_empty(x):
            return pd.isnull(x) or (isinstance(x, str) and x.strip().upper() in ("", "N/A"))

        if last_lines.apply(_is_empty).all():
            #nb_lines est le nombre de lignes à vérifier
            nb_lines = last_lines.shape[0]
            msg = f"`{path}` → colonne `{col}` contient {nb_lines} valeurs consécutives nulles ou vides."
            if key not in active_alerts:
                new_alerts.append(format_error(msg))
                active_alerts.add(key)
        else:
            # If previously an alert existed, and now values are OK, mark resolved
            if key in active_alerts:
                resolved_alerts.append(format_recovery(f"`{path}` colonne `{col}`"))
                active_alerts.remove(key)

    logging.info("Finished checking file: %s", path)
    return new_alerts, resolved_alerts

@to_thread
def check_low_prices(path: str, columns: list[str]) -> list[str]:
    """
    Checks for any prices under 50 € in the CSV at `path`, for columns named 'Price' or 'price'.
    Returns a list of formatted low-price alert messages, only for new occurrences.
    """
    new_msgs: list[str] = []
    try:
        # load only header + last 100 lines to save CPU/memory, but count total lines
        logging.info("Reading offers file with ';' separator")
        df = pd.read_csv(path, sep=';', low_memory=False, nrows=101)
        tail_data = "".join(tail_lines)
        df = pd.read_csv(io.StringIO(header + tail_data))
        # compute offset for absolute data indices (exclude header)
        offset = (total_lines - 1) - len(tail_lines)
    except Exception:
        return new_msgs

    # helper to normalize strings like "759,00 €", "310€00" or "991.75" → float
    def _parse_price(x):
        try:
            s = str(x).strip()
            # direct parse for plain numeric values
            if re.fullmatch(r'\d+(\.\d+)?', s):
                return float(s)
            # match e.g. 310€00 or 759,00 €
            m = re.search(r'(\d+)[,\.\s]*€?(\d{2})', s)
            if m:
                return float(f"{m.group(1)}.{m.group(2)}")
            # fallback: strip non-numeric, convert commas
            s2 = re.sub(r'[^\d\.\,]', '', s)
            return float(s2.replace(',', '.'))
        except:
            return float('nan')

    for col in columns:
        if col.lower() == "price":
            prices = df[col].apply(_parse_price)
            # only alert on prices greater than 2€ and less than 50€
            mask = (prices > 1) & (prices < 50)
            if mask.any():
                rows = df.loc[mask]
                # Alert only on new occurrences
                for idx, row in rows.iterrows():
                    absolute_idx = offset + idx  + 1                    # absolute index in data
                    key = (path, col, int(absolute_idx))
                    if key in low_price_alerts:
                        continue
                    low_price_alerts.add(key)
                    url = row.get("url", "")
                    price = prices.iloc[idx]
                    product_name = row.get('Product Name', row.get('descriptsmartphone', row.get('idsmartphone', 'N/A')))
                    seller_name = row.get('Seller', row.get('seller', 'N/A'))
                    new_msgs.append(format_low_price(
                        f"`{path}` ligne {absolute_idx+1}: {col} = {price}€, "
                        f"model: {product_name}, seller: {seller_name}, url: {url if url else ''}"
                    ))

    # If no new alerts, return empty list
    if not new_msgs:
        return []
    # Limit to first 5 messages, then summarize additional ones
    if len(new_msgs) > 5:
        extra = len(new_msgs) - 5
        new_msgs = new_msgs[:5] + [format_low_price(f"`{path}`: {extra} autres nouveaux prix < 50 € détectés.")]
    return new_msgs

# New: check that the most recent timestamp is not older than 40 minutes
@to_thread
def check_timestamp(path: str, columns: list[str]) -> tuple[list[str], list[str]]:
    new_alerts: list[str] = []
    resolved_alerts: list[str] = []
    ts_cols = [c for c in columns if c.lower() == "timestamp"]
    if not ts_cols:
        return new_alerts, resolved_alerts
    col = ts_cols[0]
    try:
        df = pd.read_csv(path, usecols=[col], low_memory=False, sep=";")
    except Exception:
        return new_alerts, resolved_alerts
    series = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
    if series.empty or series.isna().all():
        return new_alerts, resolved_alerts
    max_ts = series.max()
    now = datetime.datetime.now()
    key = (path, "__timestamp__")
    if (now - max_ts) > datetime.timedelta(minutes=70):
        msg = f"`{path}`: dernière mise à jour {max_ts}, > 70 minutes."
        if key not in active_alerts:
            new_alerts.append(format_error(msg))
            active_alerts.add(key)
    else:
        if key in active_alerts:
            resolved_alerts.append(format_recovery(f"`{path}` mis à jour récemment."))
            active_alerts.remove(key)
    return new_alerts, resolved_alerts

@tasks.loop(minutes=1)
async def monitor_loop():
    """Runs every 1 minute to check all configured CSV files."""
    channel = client.get_channel(CHANNEL_ID)
    if channel is None:
        logging.error("Could not find Discord channel with ID %s", CHANNEL_ID)
        return

    for path, cols in FILES_TO_MONITOR.items():
        new_alerts, resolved_alerts = await check_file(path, cols)
        
        # Envoi des erreurs : si plusieurs, un seul préfixe et liste
        if new_alerts:
            if len(new_alerts) == 1:
                logging.error("New alert for %s: %s", path, new_alerts[0])
                await send_message(channel, new_alerts[0], COLOR_ERROR)
            else:
                logging.error("Multiple new alerts for %s: %s", path, new_alerts)
                contents = [m.split(": ", 1)[1] for m in new_alerts]
                header = "@everyone **🚨 ERREUR**:"
                payload = header + "\n" + "\n".join(f"- {c}" for c in contents)
                await send_message(channel, payload, COLOR_ERROR)
        else:
            logging.info("No new alerts for %s", path)
        # Envoi groupé des messages de récupération pour ce fichier
        if resolved_alerts:
            logging.info("Resolved alerts for %s: %s", path, resolved_alerts)
            await send_message(channel, "\n".join(resolved_alerts), COLOR_RECOVERY)

        # Check for any low prices (< 50€) and send those alerts
        logging.info("Checking for low prices in %s", path)
        low_price_msgs = await check_low_prices(path, cols)
        logging.info("Low price alerts for %s: %s", path, low_price_msgs)
        for msg in low_price_msgs:
            await send_message(channel, msg, COLOR_INFO)

        # New: check timestamp freshness
        ts_new, ts_res = await check_timestamp(path, cols)
        for msg in ts_new:
            await send_message(channel, msg, COLOR_ERROR)
        for msg in ts_res:
            await send_message(channel, msg, COLOR_RECOVERY)

@monitor_loop.before_loop
async def before_monitor():
    await client.wait_until_ready()
    logging.info("Logged in as %s, starting CSV monitoring task...", client.user)

# -------- Discord notifications for start/stop --------
async def send_startup_message():
    channel = client.get_channel(CHANNEL_ID)
    if channel:
        await send_message(channel,
            "🟢 **Surveillance démarrée** : les fichiers CSV sont maintenant surveillés.\n"
            "\n"
            "💡 **Fonctionnalité** : ce programme analyse en continu vos CSV pour :\n"
            "- détecter les erreurs de lecture et colonnes manquantes\n"
            "- alerter sur valeurs vides ou nulles consécutives\n"
            "- signaler les prix bas (< 50 €)\n"
            "- vérifier la fraîcheur des données via le timestamp\n"
            "🛠️ **Commandes disponibles**:\n"
            "- `clear <n>` : supprimer les n derniers messages du bot\n"
            "- `clear` : supprimer tous les messages du bot depuis le dernier démarrage\n"
            "- `data` : afficher les 5 dernières lignes de chaque dataset\n"
            "- `status` : afficher les alertes et erreurs en cours\n"
            "Toutes les alertes et résolutions seront publiées ici en live."
        )

async def send_shutdown_message():
    channel = client.get_channel(CHANNEL_ID)
    if channel:
        await send_message(channel, "🔴 **Surveillance arrêtée** : le programme a été arrêté.")

# wrap send + close into one safe coroutine
async def _shutdown_sequence():
    try:
        await send_shutdown_message()
    except Exception:
        pass
    await client.close()

def _handle_exit(sig, frame):
    """
    On SIGINT/SIGTERM, schedule shutdown sequence without spurious errors.
    """
    loop = client.loop
    loop.call_soon_threadsafe(lambda: asyncio.create_task(_shutdown_sequence()))

# -------- Entry Point --------
if __name__ == "__main__":
    # catch SIGINT/SIGTERM to send shutdown message
    signal.signal(signal.SIGINT, _handle_exit)
    signal.signal(signal.SIGTERM, _handle_exit)

    intents = discord.Intents.default()
    intents.message_content = True       # ← enable reading message.content
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        logging.info("Logged in as %s, starting CSV monitoring task...", client.user)
        global monitor_task_started
        if not monitor_task_started:
            monitor_loop.start()
            monitor_task_started = True
        # send a clean startup message
        await send_startup_message()

    @client.event
    async def on_message(message):
        # ignore messages from ourselves
        if message.author == client.user:
            return

        logging.info(f"Received message from {message.author}: {message.content}")
        # match "clear {n}"
        m = re.match(r'^clear\s+(\d+)$', message.content.strip(), re.IGNORECASE)
        if m:
            n = int(m.group(1))
            deleted = 0

            # iterate recent history and delete our own messages
            async for msg in message.channel.history(limit=1000):
                if msg.author == client.user:
                    try:
                        await msg.delete()
                        deleted += 1
                    except Exception as e:
                        logging.error("Failed to delete bot message: %s", e)
                    if deleted >= n:
                        break

            # remove the command message if possible
            try:
                await message.delete()
            except:
                pass

            # clear {n} confirmation
            embed = discord.Embed(description=f"🗑️ Deleted {deleted} message(s).", color=EMBED_COLOR)
            await message.channel.send(embed=embed, delete_after=5)
            return

        # match "clear" (no number): delete all bot messages since last startup
        m_all = re.match(r'^clear\s*$', message.content.strip(), re.IGNORECASE)
        if m_all:
            deleted = 0
            startup_msg = None
            # find the last startup notice
            async for msg2 in message.channel.history(limit=200, oldest_first=False):
                if msg2.author == client.user and msg2.content.startswith("🟢 **Surveillance démarrée**"):
                    startup_msg = msg2
                    break
            after = startup_msg.created_at if startup_msg else None
            # delete any bot messages after that
            async for msg2 in message.channel.history(limit=1000, after=after):
                if msg2.author == client.user:
                    try:
                        await msg2.delete()
                        deleted += 1
                    except Exception as e:
                        logging.error("Failed to delete bot message: %s", e)
            # clear all confirmation
            embed = discord.Embed(description=f"🗑️ Deleted {deleted} message(s).", color=EMBED_COLOR)
            await message.channel.send(embed=embed, delete_after=5)
            return
        
        # data report start
        if message.content.strip().lower() == 'data':
            embed = discord.Embed(description="🔄 Génération du rapport d'état...", color=EMBED_COLOR)
            await message.channel.send(embed=embed)
            
            # Create temp directory
            temp_dir = tempfile.mkdtemp()
            files_to_send: list[tuple[str, str]] = []  # (csv_path, image_path)
            
            try:
                for path, cols in FILES_TO_MONITOR.items():
                    try:
                        # Read file with error handling
                        try:
                            df = pd.read_csv(path, low_memory=False, usecols=cols, encoding='utf-8', sep=';')
                        except UnicodeDecodeError:
                            df = pd.read_csv(path, low_memory=False, usecols=cols, encoding='latin1', sep=';')

                        # Get last 3 rows
                        df_tail = df.tail(5)
                        
                        if df_tail.empty:
                            continue
                            
                        # Create table image
                        fig, ax = plt.subplots(figsize=(12, 4))
                        ax.axis('off')
                        
                        # Create table with auto column widths
                        table = ax.table(
                            cellText=df_tail.values,
                            colLabels=df_tail.columns,
                            loc='center',
                            cellLoc='left',
                            colColours=['#f0f0f0']*len(df_tail.columns)
                        )
                        # Style adjustments
                        table.auto_set_font_size(False)
                        table.set_fontsize(11)
                        table.scale(1, 1.7)  # Add row height
                        
                        # Auto-adjust column widths
                        for i, col in enumerate(df_tail.columns):
                            max_width = max(
                                [len(str(x)) for x in df_tail[col].values] + [len(col)]
                            )
                            table.auto_set_column_width(i)
                        
                        # Save to temp file
                        filename = os.path.join(temp_dir, f"{os.path.basename(path)}.png")
                        # Draw canvas to compute table extents, then save tightly around the table
                        fig.canvas.draw()
                        renderer = fig.canvas.get_renderer()
                        # get bounding box of the table in display coords, convert to inches
                        bbox = table.get_window_extent(renderer).transformed(fig.dpi_scale_trans.inverted())
                        fig.savefig(filename, bbox_inches=bbox, pad_inches=-0.1, dpi=150)
                        plt.close()
                        files_to_send.append((path, filename))
                        
                    except Exception as e:
                        logging.error(f"Error generating data for {path}: {e}")
            
                # Send each image one by one, indicating its source file
                if files_to_send:
                    for csv_path, img_path in files_to_send:
                        name = os.path.basename(csv_path)
                        await message.channel.send(
                            f"📊 **État de `{name}`** (3 dernières lignes) :",
                            file=discord.File(img_path)
                        )
                else:
                    embed = discord.Embed(description="❌ Aucun fichier valide trouvé pour le statut", color=EMBED_COLOR)
                    await message.channel.send(embed=embed)
                     
            finally:
                # Cleanup temp files after 5s
                await asyncio.sleep(5)
                shutil.rmtree(temp_dir, ignore_errors=True)

        # status command
        if message.content.strip().lower() == 'status':
            if active_alerts:
                lines = ["🚨 **Erreurs actuelles:**"]
                for path, col in active_alerts:
                    if col == "__read_error__":
                        desc = "Impossible de lire le fichier"
                    elif col == "__timestamp__":
                        desc = "Données obsolètes (timestamp > 45 minutes)"
                    else:
                        desc = f"Problème sur la colonne `{col}`"
                    lines.append(f"- `{path}`: {desc}")
                embed = discord.Embed(description="\n".join(lines), color=EMBED_COLOR)
                await message.channel.send(embed=embed)
            else:
                embed = discord.Embed(description="✅ Aucun problème détecté actuellement.", color=EMBED_COLOR)
                await message.channel.send(embed=embed)
            return

# Entrée du programme : lancer le bot et loguer démarrage/arrêt
logging.info("Monitoring started")
client.run(BOT_TOKEN)
logging.info("Monitoring stopped")
