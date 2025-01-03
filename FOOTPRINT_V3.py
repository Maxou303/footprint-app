import pandas as pd
import numpy as np
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, State
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import threading
import requests
import json
import time
import logging
import signal
import sys
from websocket import WebSocketApp
import uuid

# ============================================================
#                       Configuration
# ============================================================

# Configuration de base
DEFAULT_SYMBOL = "BTCUSDT"
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
BINANCE_API_URL = "https://api.binance.com"
HISTORICAL_LIMIT = 1000  # Nombre de trades historiques à récupérer
REFRESH_INTERVAL_MS = 1000  # Intervalle de rafraîchissement en millisecondes
INITIAL_DATA_RETENTION_MS = 2 * 3600 * 1000  # 2 heures en millisecondes
MAX_DATA_RETENTION_MS = 24 * 3600 * 1000  # 24 heures en millisecondes

# Configuration du journal
logging.basicConfig(
    level=logging.INFO,  # Niveau de journalisation à INFO
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger()

# Événement d'arrêt pour une fermeture propre
shutdown_event = threading.Event()

# Lock pour protéger l'accès aux données de trades
trades_lock = threading.Lock()

# DataFrame principal pour stocker les trades
trades_dict = pd.DataFrame(columns=["ts", "price", "qty", "is_buyer_maker"])

# ============================================================
#                       WebSocketManager
# ============================================================

class WebSocketManager:
    """Gestionnaire des WebSockets pour les données de trades et de profondeur de marché."""

    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.trade_ws = None
        self.depth_ws = None
        self.ws_thread_lock = threading.Lock()
        self.trade_url = f"{BINANCE_WS_URL}/{self.symbol.lower()}@aggTrade"
        self.depth_url = f"{BINANCE_WS_URL}/{self.symbol.lower()}@depth"

    def on_trade_message(self, ws, message):
        """Traitement des messages de trades."""
        try:
            trade = json.loads(message)
            new_trade = {
                "ts": trade["T"],
                "price": float(trade["p"]),
                "qty": float(trade["q"]),
                "is_buyer_maker": bool(trade["m"])
            }
            with trades_lock:
                trades_buffer.append(new_trade)
            logger.debug(f"Received trade: {new_trade}")
        except Exception as e:
            logger.error(f"Error processing trade message: {e}")

    def on_depth_message(self, ws, message):
        """Traitement des messages de profondeur de marché."""
        try:
            depth = json.loads(message)
            # Traitement éventuel des données de profondeur
            logger.debug("Received depth update.")
        except Exception as e:
            logger.error(f"Error processing depth message: {e}")

    def on_error(self, ws, error, ws_type="WS"):
        """Gestionnaire d'erreurs pour les WebSockets."""
        logger.error(f"### {ws_type} ERROR ### {error}")
        ws.close()

    def on_close(self, ws, close_status_code, close_msg, ws_type="WS"):
        """Gestionnaire de fermeture pour les WebSockets."""
        if close_status_code or close_msg:
            logger.warning(f"### {ws_type} CLOSED ### {close_status_code} - {close_msg}")
        else:
            logger.info(f"### {ws_type} CLOSED ###")
        # Reconnexion avec backoff exponentiel si l'événement de fermeture n'est pas dû à un arrêt
        if not shutdown_event.is_set():
            self.reconnect_with_backoff(ws_type)

    def reconnect_with_backoff(self, ws_type, max_attempts=5, base_delay=1):
        """Reconnexion avec une stratégie de backoff exponentiel."""
        delay = base_delay
        attempts = 0
        while attempts < max_attempts and not shutdown_event.is_set():
            try:
                logger.info(f"Reconnecting {ws_type} in {delay} seconds...")
                time.sleep(delay)
                if ws_type == "TRADES":
                    self.start_trade_ws()
                elif ws_type == "DEPTH":
                    self.start_depth_ws()
                logger.info(f"Reconnected {ws_type}.")
                return
            except Exception as e:
                logger.error(f"Reconnection attempt {attempts + 1} for {ws_type} failed: {e}")
                delay *= 2  # Exponentiel
                attempts += 1
        if attempts >= max_attempts:
            logger.error(f"Failed to reconnect {ws_type} after {max_attempts} attempts.")

    def run_trade_ws(self):
        """Lance le WebSocket pour les trades du symbole."""
        logger.info(f"[TRADES] Connecting WS for {self.symbol}: {self.trade_url}")
        self.trade_ws = WebSocketApp(
            self.trade_url,
            on_message=self.on_trade_message,
            on_error=lambda ws, err: self.on_error(ws, err, "TRADES"),
            on_close=lambda ws, code, msg: self.on_close(ws, code, msg, "TRADES")
        )
        self.trade_ws.run_forever()

    def run_depth_ws(self):
        """Lance le WebSocket pour la profondeur de marché du symbole."""
        logger.info(f"[DEPTH] Connecting WS for {self.symbol}: {self.depth_url}")
        self.depth_ws = WebSocketApp(
            self.depth_url,
            on_message=self.on_depth_message,
            on_error=lambda ws, err: self.on_error(ws, err, "DEPTH"),
            on_close=lambda ws, code, msg: self.on_close(ws, code, msg, "DEPTH")
        )
        self.depth_ws.run_forever()

    def start_trade_ws(self):
        """Démarre un thread pour le WebSocket des trades."""
        with self.ws_thread_lock:
            if self.trade_ws and self.trade_ws.keep_running:
                logger.info(f"[TRADES] WS thread for {self.symbol} already running.")
                return
            thread = threading.Thread(target=self.run_trade_ws, daemon=True)
            thread.start()
            logger.info(f"[TRADES] WS thread for {self.symbol} started.")

    def start_depth_ws(self):
        """Démarre un thread pour le WebSocket de profondeur de marché."""
        with self.ws_thread_lock:
            if self.depth_ws and self.depth_ws.keep_running:
                logger.info(f"[DEPTH] WS thread for {self.symbol} already running.")
                return
            thread = threading.Thread(target=self.run_depth_ws, daemon=True)
            thread.start()
            logger.info(f"[DEPTH] WS thread for {self.symbol} started.")

    def stop_websockets(self):
        """Arrête proprement les WebSockets."""
        logger.info("Shutting down WebSockets...")
        shutdown_event.set()
        if self.trade_ws:
            self.trade_ws.close()
        if self.depth_ws:
            self.depth_ws.close()
        logger.info("WebSockets shut down.")

# ============================================================
#           Données et Traitement
# ============================================================

def process_trades_buffer():
    """
    Ajoute les trades du buffer au DataFrame principal et purge les anciens trades.
    Limite les données à 24 heures pour optimiser les performances.
    """
    with trades_lock:
        if trades_buffer:
            df_new = pd.DataFrame(trades_buffer)
            # Supprimer les colonnes entièrement NA avant la concaténation
            df_new = df_new.dropna(axis=1, how='all')
            if not df_new.empty:
                global trades_dict
                trades_dict = pd.concat([trades_dict, df_new], ignore_index=True)
                trades_buffer.clear()
                # Limiter à 24 heures de données
                cutoff = int(time.time() * 1000) - MAX_DATA_RETENTION_MS
                trades_dict = trades_dict[trades_dict["ts"] > cutoff]
                logger.debug(f"Processed trades buffer. Total trades: {len(trades_dict)}")
            else:
                trades_buffer.clear()
                logger.warning("Trades buffer is empty after dropping all-NA columns. Skipping concatenation.")

def periodic_trade_processor():
    """
    Processus en arrière-plan pour traiter les buffers de trades et les ajouter au DataFrame principal.
    """
    logger.info("Started trade processor thread.")
    while not shutdown_event.is_set():
        process_trades_buffer()
        time.sleep(1)  # Traiter les buffers toutes les secondes
    logger.info("Stopped trade processor thread.")

# Liste tampon pour stocker les trades en attente de traitement
trades_buffer = []

def build_ohlc_df(symbol: str, timeframe_seconds: int, only_big_trades: bool=False, min_volume_filter: float=0.0) -> pd.DataFrame:
    """
    Construit un DataFrame OHLC (Open, High, Low, Close) pour un symbole donné,
    incluant le nombre de contrats longs et courts.
    """
    with trades_lock:
        df = trades_dict.copy()
    if df.empty:
        logger.debug(f"OHLC DataFrame for {symbol} is empty.")
        return pd.DataFrame()

    if only_big_trades:
        df = df[df["qty"] >= 1.0]
    if min_volume_filter > 0.0:
        df = df[df["qty"] >= min_volume_filter]
    if df.empty:
        logger.debug(f"OHLC DataFrame for {symbol} after filtering is empty.")
        return pd.DataFrame()

    df["time_block"] = (df["ts"] // 1000) - ((df["ts"] // 1000) % timeframe_seconds)

    grouped = df.groupby("time_block")
    ohlc = grouped["price"].agg(["first", "max", "min", "last"]).rename(
        columns={"first": "open", "max": "high", "min": "low", "last": "close"}
    )
    ohlc["volume"] = grouped["qty"].sum()
    ohlc["longs"] = grouped["is_buyer_maker"].apply(lambda x: (~x).sum())
    ohlc["shorts"] = grouped["is_buyer_maker"].apply(lambda x: x.sum())
    ohlc.reset_index(inplace=True)
    ohlc.sort_values("time_block", ascending=True, inplace=True)

    ohlc["datetime"] = pd.to_datetime(ohlc["time_block"], unit='s', utc=True)
    logger.debug(f"Built OHLC DataFrame for {symbol}: {ohlc.head()}")
    return ohlc

def build_footprint_df(symbol: str, timeframe_seconds: int, price_bin_size: float,
                       cluster_type: str="buy_vol", only_big_trades: bool=False, min_volume_filter: float=0.0) -> pd.DataFrame:
    """
    Construit un DataFrame pour le Footprint Orderflow en inférant la direction des trades.
    """
    with trades_lock:
        df = trades_dict.copy()
    if df.empty:
        logger.debug(f"Footprint DataFrame for {symbol} is empty.")
        return pd.DataFrame(), 'RdBu', ""

    if only_big_trades:
        df = df[df["qty"] >= 1.0]
    if min_volume_filter > 0.0:
        df = df[df["qty"] >= min_volume_filter]
    if df.empty:
        logger.debug(f"Footprint DataFrame for {symbol} after filtering is empty.")
        return pd.DataFrame(), 'RdBu', ""

    # Assurer l'ordre chronologique
    df = df.sort_values("ts")

    # Inférer la direction du trade en comparant avec le trade précédent
    df["prev_price"] = df["price"].shift(1)
    df["trade_direction"] = np.where(df["price"] > df["prev_price"], "buy",
                                     np.where(df["price"] < df["prev_price"], "sell", "neutral"))

    # Remplacer les "neutral" par l'utilisation de `is_buyer_maker`
    df["trade_direction"] = np.where(df["trade_direction"] == "neutral",
                                     np.where(df["is_buyer_maker"], "sell", "buy"),
                                     df["trade_direction"])

    df["time_block"] = (df["ts"] // 1000) - ((df["ts"] // 1000) % timeframe_seconds)
    df["price_bin"] = (df["price"] // price_bin_size) * price_bin_size

    # Attribution basée sur la direction du trade
    df["buy_vol"] = np.where(df["trade_direction"] == "buy", df["qty"], 0.0)
    df["sell_vol"] = np.where(df["trade_direction"] == "sell", df["qty"], 0.0)

    # Logs pour vérifier l'attribution
    total_buy_vol = df["buy_vol"].sum()
    total_sell_vol = df["sell_vol"].sum()
    logger.debug(f"Total Buy Volume (inferred): {total_buy_vol}, Total Sell Volume (inferred): {total_sell_vol}")

    grouped = df.groupby(["time_block", "price_bin"]).agg({
        "buy_vol": "sum",
        "sell_vol": "sum",
        "qty": "count"
    }).reset_index()
    grouped.rename(columns={"qty": "count_trades"}, inplace=True)

    # Logs pour vérifier l'agrégation
    logger.debug(f"Aggregated Footprint DataFrame:\n{grouped.head()}")

    # Calcul des valeurs selon le type de cluster
    if cluster_type != "delta_vol":
        if cluster_type == "buy_vol":
            grouped["value"] = grouped["buy_vol"]
            colorscale = 'Blues'
            colorbar_title = "Buy Volume"
        elif cluster_type == "sell_vol":
            grouped["value"] = grouped["sell_vol"]
            colorscale = 'Reds'
            colorbar_title = "Sell Volume"
        elif cluster_type == "count_trades":
            grouped["value"] = grouped["count_trades"]
            colorscale = 'Viridis'
            colorbar_title = "Count Trades"
        else:
            logger.error(f"Unknown cluster_type: {cluster_type}")
            raise ValueError(f"Unknown cluster_type: {cluster_type}")
    else:
        # Pour 'delta_vol', calculer la différence entre buy_vol et sell_vol
        grouped["delta_vol"] = grouped["buy_vol"] - grouped["sell_vol"]
        grouped["value"] = grouped["delta_vol"]
        colorscale = 'RdBu'
        colorbar_title = "Delta Volume"

    grouped["datetime"] = pd.to_datetime(grouped["time_block"], unit='s', utc=True)
    logger.debug(f"Built Footprint DataFrame for {symbol}: {grouped.head()}")
    return grouped, colorscale, colorbar_title

def build_volume_profile_df(symbol: str, timeframe_seconds: int, price_bin_size: float,
                            only_big_trades: bool=False, min_volume_filter: float=0.0) -> pd.DataFrame:
    """
    Construit un DataFrame pour le Volume Profile par bougie.
    """
    with trades_lock:
        df = trades_dict.copy()
    if df.empty:
        logger.debug(f"Volume Profile DataFrame for {symbol} is empty.")
        return pd.DataFrame()

    if only_big_trades:
        df = df[df["qty"] >= 1.0]
    if min_volume_filter > 0.0:
        df = df[df["qty"] >= min_volume_filter]
    if df.empty:
        logger.debug(f"Volume Profile DataFrame for {symbol} after filtering is empty.")
        return pd.DataFrame()

    df["time_block"] = (df["ts"] // 1000) - ((df["ts"] // 1000) % timeframe_seconds)
    df["price_bin"] = (df["price"] // price_bin_size) * price_bin_size

    grouped = df.groupby(["time_block", "price_bin"]).agg({
        "qty": "sum"
    }).reset_index()
    grouped.rename(columns={"qty": "volume"}, inplace=True)

    grouped["datetime"] = pd.to_datetime(grouped["time_block"], unit='s', utc=True)
    logger.debug(f"Built Volume Profile DataFrame for {symbol}: {grouped.head()}")
    return grouped

def calculate_poc_val_vah(vp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le POC, VAL et VAH pour chaque bougie dans le Volume Profile.
    """
    results = []
    grouped = vp_df.groupby("time_block")

    for time_block, group in grouped:
        total_volume = group["volume"].sum()
        sorted_group = group.sort_values("volume", ascending=False)

        # POC: price_bin avec le plus grand volume
        poc = sorted_group.iloc[0]["price_bin"]

        # Value Area (70% du volume total)
        cumulative_volume = sorted_group["volume"].cumsum()
        value_area_volume = 0.7 * total_volume
        val_mask = cumulative_volume <= value_area_volume
        if val_mask.any():
            val = sorted_group[val_mask]["price_bin"].max()
        else:
            val = sorted_group.iloc[0]["price_bin"]

        vah_mask = cumulative_volume <= value_area_volume
        if vah_mask.any():
            vah = sorted_group[vah_mask]["price_bin"].min()
        else:
            vah = sorted_group.iloc[0]["price_bin"]

        results.append({
            "time_block": time_block,
            "POC": poc,
            "VAL": val,
            "VAH": vah
        })

    poc_val_vah_df = pd.DataFrame(results)
    poc_val_vah_df["datetime"] = pd.to_datetime(poc_val_vah_df["time_block"], unit='s', utc=True)
    logger.debug(f"Calculated POC, VAL, VAH:\n{poc_val_vah_df.head()}")
    return poc_val_vah_df

# ============================================================
#   Backfill Historique
# ============================================================

def fetch_historical_trades(symbol: str, limit: int=HISTORICAL_LIMIT) -> pd.DataFrame:
    """
    Récupère les données historiques des trades via l'API REST de Binance.
    """
    endpoint = f"{BINANCE_API_URL}/api/v3/aggTrades"
    params = {
        "symbol": symbol,
        "limit": limit
    }
    try:
        response = requests.get(endpoint, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        trades = []
        for trade in data:
            trades.append({
                "ts": trade["T"],
                "price": float(trade["p"]),
                "qty": float(trade["q"]),
                "is_buyer_maker": bool(trade["m"])  # Force le booléen
            })
        historical_df = pd.DataFrame(trades)
        logger.info(f"Fetched {len(historical_df)} historical trades.")
        return historical_df
    except Exception as e:
        logger.error(f"Error fetching historical trades: {e}")
        return pd.DataFrame()

def initialize_historical_data(symbol: str):
    """
    Initialise le DataFrame de trades avec des données historiques des deux dernières heures.
    """
    historical_df = fetch_historical_trades(symbol)
    if not historical_df.empty and not historical_df.isna().all().all():
        global trades_dict
        with trades_lock:
            trades_dict = pd.concat([trades_dict, historical_df], ignore_index=True)
            # Limiter à 2 heures de données
            cutoff = int(time.time() * 1000) - INITIAL_DATA_RETENTION_MS
            trades_dict = trades_dict[trades_dict["ts"] > cutoff]
        logger.info(f"Initialized trades_dict with {len(trades_dict)} trades from historical data.")
    else:
        logger.warning("No valid historical data fetched. Starting with empty trades_dict.")

# ============================================================
#    Application Dash (Graphique principal, config store, etc.)
# ============================================================

# Ajout de la police "Orbitron" de Google Fonts pour un style futuriste
external_stylesheets = [
    dbc.themes.DARKLY,
    'https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700&display=swap'
]  # Utilisation d'un thème Bootstrap sombre et d'une police futuriste

app = dash.Dash(__name__, suppress_callback_exceptions=True, external_stylesheets=external_stylesheets)
app.title = "EXOLITE"

# Générateur d'UUID pour les éléments uniques (si nécessaire)
def generate_uuid() -> str:
    return str(uuid.uuid4())

# Mise en page de l'application avec Responsive Design
def create_control_bar():
    """Crée une barre de contrôle compacte et simplifiée."""
    return dbc.Card([
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    dbc.Label("TF (s)", className="mb-0"),
                    dcc.Dropdown(
                        id="main-timeframe-dropdown",
                        options=[
                            {"label": "15s", "value": 15},
                            {"label": "30s", "value": 30},
                            {"label": "1m", "value": 60},
                            {"label": "2m", "value": 120},
                            {"label": "5m", "value": 300},
                            {"label": "15m", "value": 900},
                            {"label": "30m", "value": 1800},
                            {"label": "1h", "value": 3600},
                            {"label": "2h", "value": 7200},
                            {"label": "4h", "value": 14400}
                        ],
                        value=60,
                        clearable=False,
                        style={
                            'color': '#000000',
                            'backgroundColor': '#ffffff',
                            'fontSize': '12px'
                        }
                    )
                ], width=2, className="mb-2"),

                dbc.Col([
                    dbc.Label("Bin", className="mb-0"),
                    dcc.Dropdown(
                        id="bin-dropdown",
                        options=[
                            {"label": "0.5", "value": 0.5},
                            {"label": "1", "value": 1},
                            {"label": "5", "value": 5},
                            {"label": "10", "value": 10},
                            {"label": "20", "value": 20},
                            {"label": "50", "value": 50}
                        ],
                        value=5,
                        clearable=False,
                        style={
                            'color': '#000000',
                            'backgroundColor': '#ffffff',
                            'fontSize': '12px'
                        }
                    )
                ], width=2, className="mb-2"),

                dbc.Col([
                    dbc.Label("Cluster", className="mb-0"),
                    dcc.Dropdown(
                        id="cluster-dropdown",
                        options=[
                            {"label": "Buy Vol", "value": "buy_vol"},
                            {"label": "Sell Vol", "value": "sell_vol"},
                            {"label": "Count Trades", "value": "count_trades"},
                            {"label": "Delta Vol", "value": "delta_vol"}  # Nouveau cluster
                        ],
                        value="buy_vol",
                        clearable=False,
                        style={
                            'color': '#000000',
                            'backgroundColor': '#ffffff',
                            'fontSize': '12px'
                        }
                    )
                ], width=3, className="mb-2"),

                dbc.Col([
                    dbc.Label("Min Vol (BTC)", className="mb-0"),
                    dcc.Dropdown(
                        id="min-vol-dropdown",
                        options=[
                            {"label": "0.0", "value": 0.0},
                            {"label": "1.0", "value": 1.0},
                            {"label": "2.0", "value": 2.0},
                            {"label": "5.0", "value": 5.0},
                            {"label": "10.0", "value": 10.0},
                            {"label": "20.0", "value": 20.0},
                            {"label": "50.0", "value": 50.0}
                        ],
                        value=0.0,
                        clearable=False,
                        style={
                            'color': '#000000',
                            'backgroundColor': '#ffffff',
                            'fontSize': '12px'
                        }
                    )
                ], width=3, className="mb-2"),

                dbc.Col([
                    dbc.Label("Options", className="mb-0"),
                    dbc.Checklist(
                        options=[
                            {"label": "POC", "value": "show_poc"},
                            {"label": "VAL", "value": "show_val"},
                            {"label": "VAH", "value": "show_vah"},
                            {"label": "Volume Profile", "value": "show_vp"},
                            {"label": "Heatmap", "value": "show_heatmap"},
                            {"label": "Delta Text", "value": "show_delta"}
                        ],
                        value=["show_poc", "show_val", "show_vah", "show_vp", "show_heatmap", "show_delta"],  # Sélection par défaut
                        id="options-checklist",
                        inline=True,
                        switch=True,  # Utilisation de switches pour plus de compacité
                        style={"fontSize": "10px"}
                    )
                ], width=3, className="mb-2"),
            ], align="center", className="gx-2")  # Réduction de l'espace entre les colonnes
        ])
    ], className="mb-3")

app.layout = dbc.Container(
    fluid=True,
    style={
        "backgroundColor": "#2a2a2a",
        "color": "white",
        "padding": "10px",
        "minHeight": "100vh"
    },
    children=[

        # Titre de l'application avec police futuriste
        dbc.Row([
            dbc.Col([
                html.H1(
                    "EXOLITE",
                    className="text-center mb-2",  # Réduit la marge inférieure
                    style={
                        'font-family': '"Orbitron", sans-serif',
                        'font-weight': '700',
                        'font-size': '2rem'  # Taille de police réduite
                    }
                )
            ], width=12)
        ], justify="center"),

        # Barre de contrôle compacte en une seule ligne
        dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    # Timeframe Dropdown
                    dbc.Col([
                        dbc.Label("TF:", html_for="main-timeframe-dropdown", className="mb-0", style={"font-size": "0.8rem"}),
                        dcc.Dropdown(
                            id="main-timeframe-dropdown",
                            options=[
                                {"label": "15s", "value": 15},
                                {"label": "30s", "value": 30},
                                {"label": "1m", "value": 60},
                                {"label": "2m", "value": 120},
                                {"label": "5m", "value": 300},
                                {"label": "15m", "value": 900},
                                {"label": "30m", "value": 1800},
                                {"label": "1h", "value": 3600},
                                {"label": "2h", "value": 7200},
                                {"label": "4h", "value": 14400}
                            ],
                            value=60,
                            clearable=False,
                            style={
                                'color': '#000000',
                                'backgroundColor': '#ffffff',
                                'width': '80px',
                                'fontSize': '12px'
                            }
                        )
                    ], width="auto", className="d-flex align-items-center me-2"),

                    # Price Bin Dropdown
                    dbc.Col([
                        dbc.Label("Bin:", html_for="bin-dropdown", className="mb-0", style={"font-size": "0.8rem"}),
                        dcc.Dropdown(
                            id="bin-dropdown",
                            options=[
                                {"label": "0.5", "value": 0.5},
                                {"label": "1", "value": 1},
                                {"label": "5", "value": 5},
                                {"label": "10", "value": 10},
                                {"label": "20", "value": 20},
                                {"label": "50", "value": 50}
                            ],
                            value=5,
                            clearable=False,
                            style={
                                'color': '#000000',
                                'backgroundColor': '#ffffff',
                                'width': '80px',
                                'fontSize': '12px'
                            }
                        )
                    ], width="auto", className="d-flex align-items-center me-2"),

                    # Cluster Type Dropdown
                    dbc.Col([
                        dbc.Label("Cluster:", html_for="cluster-dropdown", className="mb-0", style={"font-size": "0.8rem"}),
                        dcc.Dropdown(
                            id="cluster-dropdown",
                            options=[
                                {"label":"Buy Vol","value":"buy_vol"},
                                {"label":"Sell Vol","value":"sell_vol"},
                                {"label":"Count Trades","value":"count_trades"},
                                {"label":"Delta Vol","value":"delta_vol"}
                            ],
                            value="buy_vol",
                            clearable=False,
                            style={
                                'color': '#000000',
                                'backgroundColor': '#ffffff',
                                'width': '100px',
                                'fontSize': '12px'
                            }
                        )
                    ], width="auto", className="d-flex align-items-center me-2"),

                    # Min Volume Dropdown
                    dbc.Col([
                        dbc.Label("Min Vol:", html_for="min-vol-dropdown", className="mb-0", style={"font-size": "0.8rem"}),
                        dcc.Dropdown(
                            id="min-vol-dropdown",
                            options=[
                                {"label": "0.0", "value": 0.0},
                                {"label": "1.0", "value": 1.0},
                                {"label": "2.0", "value": 2.0},
                                {"label": "5.0", "value": 5.0},
                                {"label": "10.0", "value": 10.0},
                                {"label": "20.0", "value": 20.0},
                                {"label": "50.0", "value": 50.0}
                            ],
                            value=0.0,
                            clearable=False,
                            style={
                                'color': '#000000',
                                'backgroundColor': '#ffffff',
                                'width': '100px',
                                'fontSize': '12px'
                            }
                        )
                    ], width="auto", className="d-flex align-items-center me-2"),

                    # Checkboxes regroupés avec tooltip pour labels
                    dbc.Col([
                        dbc.Label("Options:", className="mb-0", style={"font-size": "0.8rem"}),
                        dbc.Tooltip(
                            "Afficher les options de graphique",
                            target="options-checklist"
                        ),
                        dbc.Checklist(
                            options=[
                                {"label": "POC", "value": "show_poc"},
                                {"label": "VAL", "value": "show_val"},
                                {"label": "VAH", "value": "show_vah"},
                                {"label": "VP", "value": "show_vp"},  # Volume Profile
                                {"label": "Heat", "value": "show_heatmap"},  # Heatmap
                                {"label": "Δ Text", "value": "show_delta"}  # Delta Text
                            ],
                            value=["show_poc", "show_val", "show_vah", "show_vp", "show_heatmap", "show_delta"],
                            id="options-checklist",
                            inline=True,
                            switch=True,
                            style={"font-size": "0.8rem"}
                        )
                    ], width="auto", className="d-flex align-items-center")
                    
                ], align="center", className="gx-1")  # Réduction de l'espace entre les colonnes
            ])
        ], className="mb-3"),

        # Contenu principal dans une nouvelle rangée
        dbc.Row([
            dbc.Col([
                # Graphique principal
                dbc.Card([
                    dbc.CardHeader("Main Chart"),
                    dbc.CardBody([
                        dcc.Graph(id="main-graph", config={
                            "displayModeBar": True,
                            "modeBarButtonsToAdd": ['zoom2d', 'pan2d', 'select2d', 'lasso2d', 'resetScale2d'],
                            "scrollZoom": True
                        })
                    ])
                ], className="mb-4"),

            ], width=12)
        ], className="mb-4"),

        # Interval pour rafraîchissement des données
        dcc.Interval(id="interval-refresh", interval=REFRESH_INTERVAL_MS, n_intervals=0),

    ]
)

# ============================================================
#           Callbacks Optimisés et Enrichis
# ============================================================

# Callback pour mettre à jour le graphique principal
@app.callback(
    Output("main-graph", "figure"),
    [
        Input("interval-refresh", "n_intervals"),
        Input("main-timeframe-dropdown", "value"),
        Input("bin-dropdown", "value"),
        Input("min-vol-dropdown", "value"),
        Input("cluster-dropdown", "value"),
        Input("options-checklist", "value"),
    ]
)
def render_main_graph(n, timeframe_s, bin_s, min_vol, cluster_type, options):
    """
    Génère le contenu du graphique principal basé sur les paramètres sélectionnés.
    """
    logger.info("Rendering main graph...")
    symbol = DEFAULT_SYMBOL.upper()
    ohlc_df = build_ohlc_df(symbol, timeframe_s, only_big_trades=False, min_volume_filter=min_vol)
    
    # Choisir la fonction d'agrégation en fonction du cluster_type
    fp_df, colorscale, colorbar_title = build_footprint_df(
        symbol, timeframe_s, bin_s, cluster_type=cluster_type, 
        only_big_trades=False, min_volume_filter=min_vol
    )
    
    vp_df = build_volume_profile_df(symbol, timeframe_s, bin_s, only_big_trades=False, min_volume_filter=min_vol)

    # Calcul des POC, VAL, VAH
    poc_val_vah_df = calculate_poc_val_vah(vp_df)

    # Log les totaux buy_vol, sell_vol ou delta_vol
    if not fp_df.empty:
        if cluster_type == "delta_vol":
            total_delta = fp_df["value"].sum()
            logger.debug(f"Total Delta Volume: {total_delta}")
        else:
            total_buy = fp_df["buy_vol"].sum()
            total_sell = fp_df["sell_vol"].sum()
            logger.debug(f"Total Buy Volume: {total_buy}, Total Sell Volume: {total_sell}")
    else:
        logger.debug("Footprint DataFrame is empty.")

    # Création de la figure avec Candlestick + Footprint et Volume Profile intégré
    fig = make_subplots(
        rows=1, cols=1,  # Une seule rangée et colonne
        shared_xaxes=True,
        vertical_spacing=0.15,
        subplot_titles=None
    )

    fig.update_layout(
        uirevision="constant",
        height=700,  # Réduit la hauteur pour maximiser l'espace
        template="plotly_dark",
        margin=dict(l=25, r=30, t=20, b=20),  # Réduit la marge du haut
        hovermode="closest",  # Désactiver l'affichage des données au survol
        showlegend=False,
        dragmode='pan',  # Permet le déplacement via clic gauche et glisser
        xaxis_rangeslider_visible=False,
    )

    # --- Candlestick
    if not ohlc_df.empty:
        fig.add_trace(
            go.Candlestick(
                x=ohlc_df["datetime"],
                open=ohlc_df["open"],
                high=ohlc_df["high"],
                low=ohlc_df["low"],
                close=ohlc_df["close"],
                name="Candles",
                showlegend=False,
                hoverinfo='skip'  # Désactiver les info-bulles sur les candlesticks
            ),
            row=1, col=1
        )
        logger.debug(f"Main Graph: Added Candlestick with {len(ohlc_df)} points.")
    else:
        logger.debug("Main Graph: OHLC DataFrame is empty.")

    # --- Footprint Heatmap
    if "show_heatmap" in options and not fp_df.empty:
        pivot_fp = fp_df.pivot(index="price_bin", columns="datetime", values="value")
        if pivot_fp is not None and not pivot_fp.empty:
            pivot_fp = pivot_fp.sort_index(ascending=False)
            x_vals = list(pivot_fp.columns)
            y_vals = list(pivot_fp.index)
            z_vals = pivot_fp.fillna(0).values

            # Normalisation des valeurs pour l'échelle de couleurs centrée sur 0 si delta_vol
            if cluster_type == "delta_vol":
                max_abs = np.nanmax(np.abs(z_vals))
                if max_abs != 0:
                    z_vals = z_vals / max_abs
                else:
                    z_vals = z_vals
                z_mid = 0  # Centre de l'échelle de couleurs
            else:
                max_val = np.nanmax(z_vals)
                min_val = np.nanmin(z_vals)
                if cluster_type in ["buy_vol", "sell_vol"]:
                    if max_val != 0:
                        z_vals = z_vals / max_val
                    else:
                        z_vals = z_vals
                z_mid = None  # Pas de centre spécifique

            # Définir l'échelle de couleurs personnalisée si nécessaire
            heatmap_colorscale = colorscale

            # Définir les paramètres du Heatmap
            heatmap_kwargs = {
                "x": x_vals,
                "y": y_vals,
                "z": z_vals,
                "colorscale": heatmap_colorscale,
                "colorbar": dict(
                    title=colorbar_title,  # Titre de la légende
                    x=-0.05,               # Place la légende à gauche du graphique
                    thickness=30           # Ajuste la largeur de la barre
                ),
                "opacity": 0.5,  # Ajuster l'opacité pour mieux voir les éléments sous-jacents
                "showscale": True,  # Afficher la légende
                "hovertemplate": "Time: %{x}<br>Price Bin: %{y}<br>Value: %{z:.2f}<extra></extra>",
                "zmid": z_mid  # Centrer l'échelle sur 0 si delta_vol
            }

            # Ajout de la Heatmap
            fig.add_trace(
                go.Heatmap(**heatmap_kwargs),
                row=1, col=1
            )
            logger.debug("Main Graph: Added Footprint Heatmap.")
        else:
            logger.debug("Main Graph: Footprint pivot table is empty.")
    else:
        logger.debug("Main Graph: Heatmap is hidden or Footprint DataFrame is empty.")

    # --- Volume Profile intégré en tant que Scatter
    if "show_vp" in options and not vp_df.empty and not ohlc_df.empty:
        # Calculer le volume total par price_bin pour ajuster la taille
        volume_total = vp_df.groupby("price_bin")["volume"].sum().reset_index()
        vp_df = vp_df.merge(volume_total, on="price_bin", suffixes=('', '_total'))
        fig.add_trace(
            go.Scatter(
                x=vp_df["datetime"],
                y=vp_df["price_bin"],
                mode='markers',
                marker=dict(
                    size=np.log1p(vp_df["volume_total"]) * 5,  # Taille proportionnelle au volume
                    color='orange',
                    opacity=0.2  # Réduit l'opacité à 0.2
                ),
                name="Volume Profile",
                showlegend=False,
                hoverinfo='skip'  # Désactiver les info-bulles sur le Volume Profile
            ),
            row=1, col=1
        )
        logger.debug("Main Graph: Added Volume Profile Scatter.")

    # --- POC, VAL, VAH Cercles (Indépendants du Volume Profile)
    if not poc_val_vah_df.empty:
        if "show_poc" in options:
            # POC en rouge
            fig.add_trace(
                go.Scatter(
                    x=poc_val_vah_df["datetime"],
                    y=poc_val_vah_df["POC"],
                    mode='markers',
                    marker=dict(
                        symbol='circle',
                        size=10,
                        color='red',  # Changement de couleur en rouge
                        line=dict(width=1, color='black')
                    ),
                    name="POC",
                    showlegend=False,
                    hoverinfo='skip'  # Désactiver les info-bulles
                ),
                row=1, col=1
            )
        if "show_val" in options:
            # VAL en jaune
            fig.add_trace(
                go.Scatter(
                    x=poc_val_vah_df["datetime"],
                    y=poc_val_vah_df["VAL"],
                    mode='markers',
                    marker=dict(
                        symbol='circle',
                        size=10,
                        color='yellow',
                        line=dict(width=1, color='black')
                    ),
                    name="VAL",
                    showlegend=False,
                    hoverinfo='skip'  # Désactiver les info-bulles
                ),
                row=1, col=1
            )
        if "show_vah" in options:
            # VAH en jaune
            fig.add_trace(
                go.Scatter(
                    x=poc_val_vah_df["datetime"],
                    y=poc_val_vah_df["VAH"],
                    mode='markers',
                    marker=dict(
                        symbol='circle',
                        size=10,
                        color='yellow',
                        line=dict(width=1, color='black')
                    ),
                    name="VAH",
                    showlegend=False,
                    hoverinfo='skip'  # Désactiver les info-bulles
                ),
                row=1, col=1
            )
        logger.debug("Main Graph: Added POC, VAL, VAH markers.")
    else:
        logger.debug("Main Graph: POC, VAL, VAH DataFrame is empty.")

    # --- Delta Texte (annotations centrées à l'intérieur des bougies)
    if "show_delta" in options and not ohlc_df.empty:
        # Calculer les deltas en vectorisé
        ohlc_df["delta"] = ohlc_df['close'] - ohlc_df['open']
        annotations = []
        for _, row in ohlc_df.iterrows():
            # Positionnement exact au centre de la bougie
            y_position = (row["open"] + row["close"]) / 2  # Recentrage exact
            annotations.append(dict(
                x=row["datetime"],
                y=y_position,
                text=f"Δ {row['delta']:.2f}",
                showarrow=False,
                xanchor='center',  # Centrer horizontalement
                yanchor='middle',  # Centrer verticalement
                font=dict(color="white", size=10),  # Taille de police adaptée
                align="center"
            ))
        # Récupérer les annotations existantes et les convertir en liste
        existing_annotations = list(fig.layout.annotations) if fig.layout.annotations else []
        # Ajouter les nouvelles annotations aux existantes
        fig.update_layout(annotations=existing_annotations + annotations)
        logger.debug("Main Graph: Added Delta annotations.")
    else:
        logger.debug("Main Graph: Delta text is hidden or OHLC DataFrame is empty.")

    # Mise à jour des axes
    fig.update_xaxes(title_text=None, row=1, col=1)
    fig.update_yaxes(title_text=None, row=1, col=1)
    fig.update_yaxes(side='right', tickmode='auto', nticks=10)  # Limiter le nombre de ticks

    # Optionnel : Définir les ticks de l'axe Y basés sur les price_bin disponibles
    if not fp_df.empty:
        unique_bins = sorted(fp_df["price_bin"].unique())
        fig.update_yaxes(
            tickmode='auto',
            nticks=10  # Limiter à 10 ticks maximum pour éviter la surcharge
        )

    fig.update_xaxes(showgrid=False)  # Désactiver la grille horizontale
    fig.update_yaxes(showgrid=False)  # Désactiver la grille verticale

    # Vérifier si la figure contient des données
    if not fig.data:
        logger.warning("Main Graph: Figure contains no data.")
    else:
        logger.debug(f"Main Graph: Figure contains {len(fig.data)} traces.")

    return fig

# ============================================================
#                       MAIN
# ============================================================

def signal_handler(sig, frame):
    """Gestionnaire de signal pour une fermeture propre."""
    logger.info("Received shutdown signal.")
    ws_manager.stop_websockets()
    shutdown_event.set()
    sys.exit(0)

if __name__ == "__main__":
    try:
        # Initialiser les données historiques avant de démarrer les WebSockets
        initialize_historical_data(DEFAULT_SYMBOL)

        # Créer une instance de WebSocketManager
        ws_manager = WebSocketManager(DEFAULT_SYMBOL)

        # Démarrer les WebSockets pour le symbole par défaut
        ws_manager.start_trade_ws()
        ws_manager.start_depth_ws()

        # Lancer le processeur de trades dans un thread séparé
        trade_processor_thread = threading.Thread(target=periodic_trade_processor, daemon=True)
        trade_processor_thread.start()

        # Enregistrer le gestionnaire de signal pour une fermeture propre
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Lancer le serveur Dash en mode débogage désactivé
        logger.info("Starting Dash server.")
        app.run_server(debug=False, host="0.0.0.0", port=8050)
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
    finally:
        ws_manager.stop_websockets()
        shutdown_event.set()
        trade_processor_thread.join(timeout=5)
        logger.info("Application has been shut down.")
