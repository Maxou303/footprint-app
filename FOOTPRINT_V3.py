import json
import time
import threading
import logging
import uuid
import signal
import sys
from datetime import datetime

import pandas as pd
import numpy as np
import requests

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import plotly.graph_objs as go
from plotly.subplots import make_subplots

from websocket import WebSocketApp  # Assurez-vous d'avoir installé websocket-client

# ============================================================
#                      CONFIGURATION
# ============================================================

BINANCE_FSTREAM_URL = "wss://fstream.binance.com"
BINANCE_API_URL = "https://api.binance.com"
DEFAULT_SYMBOL = "BTCUSDT"         # Symbole par défaut

REFRESH_INTERVAL_MS = 4000          # Intervalle de rafraîchissement en millisecondes
HISTORICAL_TIMEFRAME = 60            # Temps en secondes pour le backfill historique
HISTORICAL_LIMIT = 1000              # Nombre de bougies historiques à récupérer

# Structures de données pour stocker les trades et l'orderbook
trades_dict = pd.DataFrame(columns=["ts", "price", "qty", "is_buyer_maker"])
trades_buffer = []  # Buffer pour accumuler les trades avant de les ajouter au DataFrame

orderbook_dict = {}
orderbook_lock = threading.Lock()  # Verrou pour protéger l'orderbook lors des mises à jour
trades_lock = threading.Lock()     # Verrou pour protéger trades_dict et trades_buffer

# Événements pour gérer la fermeture des threads
shutdown_event = threading.Event()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,  # Passer à DEBUG pour plus de détails
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================================
#           1) WEBSOCKET TRADES & DEPTH (avec Optimisations)
# ============================================================

def aggtrade_endpoint(symbol: str) -> str:
    """Retourne l'URL du WebSocket pour les trades agrégés d'un symbole."""
    return f"{BINANCE_FSTREAM_URL}/ws/{symbol.lower()}@aggTrade"

def depth_endpoint(symbol: str) -> str:
    """Retourne l'URL du WebSocket pour la profondeur de marché d'un symbole."""
    return f"{BINANCE_FSTREAM_URL}/ws/{symbol.lower()}@depth20@100ms"

def on_trade_message(ws, message):
    """Gestionnaire d'événements pour les messages de trades."""
    try:
        data = json.loads(message)
        trade = {
            "ts": data["T"],
            "price": float(data["p"]),
            "qty": float(data["q"]),
            "is_buyer_maker": bool(data["m"])  # Force le booléen
        }
        with trades_lock:
            trades_buffer.append(trade)
        logger.debug(f"Received trade: {trade}")
    except Exception as e:
        logger.error(f"Error processing trade message: {e}")

def on_depth_message(ws, message):
    """Gestionnaire d'événements pour les messages de profondeur de marché."""
    try:
        data = json.loads(message)
        bids = data.get("b", [])
        asks = data.get("a", [])
        with orderbook_lock:
            df_bids = pd.DataFrame(bids, columns=["price", "qty"], dtype=float)
            df_asks = pd.DataFrame(asks, columns=["price", "qty"], dtype=float)
            # Les bids doivent être triés en DESC et les asks en ASC
            orderbook_dict["bids"] = df_bids.sort_values("price", ascending=False)
            orderbook_dict["asks"] = df_asks.sort_values("price", ascending=True)
        logger.debug(f"Updated orderbook")
    except Exception as e:
        logger.error(f"Error processing depth message: {e}")

def on_trade_error(ws, error):
    """Gestionnaire d'erreurs pour le WebSocket des trades."""
    logger.error(f"### TRADE WS ERROR ### {error}")
    ws.close()

def on_trade_close(ws, close_status_code, close_msg):
    """Gestionnaire de fermeture pour le WebSocket des trades."""
    if close_status_code or close_msg:
        logger.warning(f"### TRADE WS CLOSED ### {close_status_code} - {close_msg}")
    else:
        logger.info("### TRADE WS CLOSED ###")
    # Reconnexion avec backoff exponentiel si l'événement de fermeture n'est pas dû à un arrêt
    if not shutdown_event.is_set():
        reconnect_with_backoff(run_trade_ws)

def on_depth_error(ws, error):
    """Gestionnaire d'erreurs pour le WebSocket de profondeur."""
    logger.error(f"### DEPTH WS ERROR ### {error}")
    ws.close()

def on_depth_close(ws, close_status_code, close_msg):
    """Gestionnaire de fermeture pour le WebSocket de profondeur."""
    if close_status_code or close_msg:
        logger.warning(f"### DEPTH WS CLOSED ### {close_status_code} - {close_msg}")
    else:
        logger.info("### DEPTH WS CLOSED ###")
    # Reconnexion avec backoff exponentiel si l'événement de fermeture n'est pas dû à un arrêt
    if not shutdown_event.is_set():
        reconnect_with_backoff(run_depth_ws)

def reconnect_with_backoff(target_func, max_attempts=5, base_delay=1):
    """Reconnexion avec une stratégie de backoff exponentiel."""
    delay = base_delay
    attempts = 0
    while attempts < max_attempts and not shutdown_event.is_set():
        try:
            logger.info(f"Reconnecting to WebSocket in {delay} seconds...")
            time.sleep(delay)
            target_func()
            logger.info(f"Reconnected to WebSocket.")
            return
        except Exception as e:
            logger.error(f"Reconnection attempt {attempts + 1} failed: {e}")
            delay *= 2  # Exponentiel
            attempts += 1
    if attempts >= max_attempts:
        logger.error(f"Failed to reconnect to WebSocket after {max_attempts} attempts.")

def run_trade_ws():
    """Lance le WebSocket pour les trades du symbole par défaut."""
    url = aggtrade_endpoint(DEFAULT_SYMBOL)
    logger.info(f"[TRADES] Connecting WS for {DEFAULT_SYMBOL}: {url}")
    ws_app = WebSocketApp(
        url,
        on_message=on_trade_message,
        on_error=on_trade_error,
        on_close=on_trade_close
    )
    ws_app.run_forever()

def run_depth_ws():
    """Lance le WebSocket pour la profondeur de marché du symbole par défaut."""
    url = depth_endpoint(DEFAULT_SYMBOL)
    logger.info(f"[DEPTH] Connecting WS for {DEFAULT_SYMBOL}: {url}")
    ws_app = WebSocketApp(
        url,
        on_message=on_depth_message,
        on_error=on_depth_error,
        on_close=on_depth_close
    )
    ws_app.run_forever()

current_trade_ws_thread = None
current_depth_ws_thread = None
ws_thread_lock = threading.Lock()

def start_trade_ws_thread():
    """Démarre un thread pour le WebSocket des trades du symbole par défaut."""
    global current_trade_ws_thread
    with ws_thread_lock:
        if current_trade_ws_thread and current_trade_ws_thread.is_alive():
            logger.info(f"[TRADES] WS thread for {DEFAULT_SYMBOL} already running.")
            return
        t = threading.Thread(target=run_trade_ws, daemon=True)
        t.start()
        current_trade_ws_thread = t
        logger.info(f"[TRADES] WS thread for {DEFAULT_SYMBOL} started.")

def start_depth_ws_thread():
    """Démarre un thread pour le WebSocket de profondeur de marché du symbole par défaut."""
    global current_depth_ws_thread
    with ws_thread_lock:
        if current_depth_ws_thread and current_depth_ws_thread.is_alive():
            logger.info(f"[DEPTH] WS thread for {DEFAULT_SYMBOL} already running.")
            return
        t = threading.Thread(target=run_depth_ws, daemon=True)
        t.start()
        current_depth_ws_thread = t
        logger.info(f"[DEPTH] WS thread for {DEFAULT_SYMBOL} started.")

def stop_websockets():
    """Arrête proprement les WebSockets en signalant l'événement de shutdown."""
    logger.info("Shutting down WebSockets...")
    shutdown_event.set()
    # Les WebSocketApp ne peuvent pas être fermés directement depuis d'autres threads,
    # donc nous devons laisser les callbacks gérer la fermeture en vérifiant shutdown_event
    if current_trade_ws_thread:
        current_trade_ws_thread.join(timeout=5)
    if current_depth_ws_thread:
        current_depth_ws_thread.join(timeout=5)
    logger.info("WebSockets shut down.")

def process_trades_buffer():
    """
    Ajoute les trades du buffer au DataFrame principal et purge les anciens trades.
    Limite les données à 2 heures pour optimiser les performances.
    """
    with trades_lock:
        if trades_buffer:
            df_new = pd.DataFrame(trades_buffer)
            # Vérifier si df_new n'est pas vide et ne contient pas uniquement des NA
            if not df_new.empty and not df_new.isna().all().all():
                global trades_dict
                trades_dict = pd.concat([trades_dict, df_new], ignore_index=True)
                trades_buffer.clear()
                # Limiter à 2 heures de données
                cutoff = int(time.time() * 1000) - (2 * 3600 * 1000)
                trades_dict = trades_dict[trades_dict["ts"] > cutoff]
                logger.debug(f"Processed trades buffer. Total trades: {len(trades_dict)}")
            else:
                trades_buffer.clear()
                logger.warning("Trades buffer is empty or contains all-NA data. Skipping concatenation.")

# ============================================================
#   2) Fonctions d'agrégation : OHLC, Footprint, etc.
# ============================================================

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
    ohlc["longs"] = grouped["is_buyer_maker"].apply(lambda x: (x == False).sum())
    ohlc["shorts"] = grouped["is_buyer_maker"].apply(lambda x: (x == True).sum())
    ohlc.reset_index(inplace=True)
    ohlc.sort_values("time_block", ascending=True, inplace=True)

    ohlc["datetime"] = pd.to_datetime(ohlc["time_block"], unit='s', utc=True)
    logger.debug(f"Built OHLC DataFrame for {symbol}: {ohlc.head()}")
    return ohlc

def build_footprint_df(symbol: str, timeframe_seconds: int, price_bin_size: float,
                       cluster_type: str="buy_vol", only_big_trades: bool=False, min_volume_filter: float=0.0) -> pd.DataFrame:
    """
    Construit un DataFrame pour le Footprint Orderflow.
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

    df["time_block"] = (df["ts"] // 1000) - ((df["ts"] // 1000) % timeframe_seconds)
    df["price_bin"] = (df["price"] // price_bin_size) * price_bin_size

    # Séparer les buy_vol et sell_vol
    df["buy_vol"] = np.where(~df["is_buyer_maker"], df["qty"], 0.0)
    df["sell_vol"] = np.where(df["is_buyer_maker"], df["qty"], 0.0)

    grouped = df.groupby(["time_block", "price_bin"]).agg({
        "buy_vol": "sum",
        "sell_vol": "sum",
        "qty": "count"
    }).reset_index()
    grouped.rename(columns={"qty": "count_trades"}, inplace=True)

    logger.debug(f"Aggregated Footprint DataFrame:\n{grouped.head()}")

    # Calculer les valeurs selon le type de cluster
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
    elif cluster_type == "net_vol":
        grouped["value"] = grouped["buy_vol"] - grouped["sell_vol"]
        colorscale = 'RdBu'
        colorbar_title = "Net Volume"
    else:
        logger.error(f"Unknown cluster_type: {cluster_type}")
        raise ValueError(f"Unknown cluster_type: {cluster_type}")

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
#   2.1) Ajout du Backfill Historique
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

def initialize_historical_data():
    """
    Initialise le DataFrame de trades avec des données historiques.
    """
    historical_df = fetch_historical_trades(DEFAULT_SYMBOL)
    if not historical_df.empty and not historical_df.isna().all().all():
        global trades_dict
        with trades_lock:
            trades_dict = pd.concat([trades_dict, historical_df], ignore_index=True)
            # Limiter à 2 heures de données
            cutoff = int(time.time() * 1000) - (2 * 3600 * 1000)
            trades_dict = trades_dict[trades_dict["ts"] > cutoff]
        logger.info(f"Initialized trades_dict with {len(trades_dict)} trades from historical data.")
    else:
        logger.warning("No valid historical data fetched. Starting with empty trades_dict.")

# ============================================================
#    3) Application Dash (Graphique principal, config store, etc.)
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

# Mise en page de l'application
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
                    className="text-center mb-3",  # Centré horizontalement
                    style={
                        'font-family': '"Orbitron", sans-serif',  # Police futuriste
                        'font-weight': '700'  # Utilisation de la graisse de police disponible
                    }
                )
            ], width=12)
        ]),

        # Panneau de contrôle en barre horizontale compacte
        dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    # Timeframe Dropdown
                    dbc.Col([
                        dbc.Label("TF (s):", html_for="main-timeframe-dropdown", className="mb-0"),
                        dcc.Dropdown(
                            id="main-timeframe-dropdown",
                            options=[
                                {"label": "15s", "value": 15},
                                {"label": "30s", "value": 30},
                                {"label": "1m", "value": 60},
                                {"label": "2m", "value": 120},
                                {"label": "5m", "value": 300},
                                {"label": "15m", "value": 900},
                                {"label": "30m", "value": 1800},  # Nouvelle option
                                {"label": "1h", "value": 3600},   # Nouvelle option
                                {"label": "2h", "value": 7200},   # Nouvelle option
                                {"label": "4h", "value": 14400}   # Nouvelle option
                            ],
                            value=60,
                            clearable=False,
                            style={
                                'color': '#000000',
                                'backgroundColor': '#ffffff',
                                'width': '80px'
                            }
                        )
                    ], width="auto", className="mx-1"),

                    # Price Bin Dropdown
                    dbc.Col([
                        dbc.Label("Bin:", html_for="bin-dropdown", className="mb-0"),
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
                                'width': '100px'
                            }
                        )
                    ], width="auto", className="mx-1"),

                    # Cluster Type Dropdown
                    dbc.Col([
                        dbc.Label("Cluster:", html_for="cluster-dropdown", className="mb-0"),
                        dcc.Dropdown(
                            id="cluster-dropdown",
                            options=[
                                {"label":"Buy Vol","value":"buy_vol"},
                                {"label":"Sell Vol","value":"sell_vol"},
                                {"label":"Count Trades","value":"count_trades"},
                                {"label":"Net Vol","value":"net_vol"}  # Nouveau type de cluster
                            ],
                            value="buy_vol",
                            clearable=False,
                            style={
                                'color': '#000000',
                                'backgroundColor': '#ffffff',
                                'width': '150px'
                            }
                        )
                    ], width="auto", className="mx-1"),

                    # Min Volume Dropdown
                    dbc.Col([
                        dbc.Label("Min Vol (BTC):", html_for="min-vol-dropdown", className="mb-0"),
                        dcc.Dropdown(
                            id="min-vol-dropdown",
                            options=[
                                {"label": "0.0", "value": 0.0},
                                {"label": "1.0", "value": 1.0},
                                {"label": "2.0", "value": 2.0},
                                {"label": "5.0", "value": 5.0},
                                {"label": "10.0", "value": 10.0},
                                {"label": "20.0", "value": 20.0},  # Nouvelle option
                                {"label": "50.0", "value": 50.0}   # Nouvelle option
                            ],
                            value=0.0,
                            clearable=False,
                            style={
                                'color': '#000000',
                                'backgroundColor': '#ffffff',
                                'width': '100px'
                            }
                        )
                    ], width="auto", className="mx-1"),

                    # Show Text Checklist
                    dbc.Col([
                        dbc.Label("Show Text:", html_for="show-text-checklist", className="mb-0"),
                        dbc.Checklist(
                            options=[{"label": "", "value": "show_text"}],
                            value=[],  # Désactivé par défaut
                            id="show-text-checklist",
                            switch=True
                        )
                    ], width="auto", className="mx-1"),
                ], align="center", className="gx-2")
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
                            "scrollZoom": True  # Permet le zoom via la molette
                        })
                    ])
                ], className="mb-4"),

            ], width=12)  # Graphique principal occupant toute la largeur
        ], className="mb-4"),

        # Interval pour rafraîchissement des données
        dcc.Interval(id="interval-refresh", interval=REFRESH_INTERVAL_MS, n_intervals=0),
    ]
)

# ============================================================
#           4) Callbacks Optimisés et Enrichis
# ============================================================

# Callback pour mettre à jour le graphique principal
@app.callback(
    Output("main-graph", "figure"),
    [
        Input("interval-refresh", "n_intervals")
    ],
    [
        State("main-timeframe-dropdown", "value"),
        State("bin-dropdown", "value"),
        State("min-vol-dropdown", "value"),
        State("cluster-dropdown", "value"),
        State("show-text-checklist", "value")  # Nouvel état ajouté
    ]
)
def render_main_graph(n, timeframe_s, bin_s, min_vol, cluster_type, show_text):
    """
    Génère le contenu du graphique principal basé sur les paramètres sélectionnés.
    """
    logger.debug("Rendering main graph...")
    only_big = False  # Big Trades a été supprimé
    ohlc_df = build_ohlc_df(DEFAULT_SYMBOL, timeframe_s, only_big_trades=only_big, min_volume_filter=min_vol)
    fp_df, colorscale, colorbar_title = build_footprint_df(
        DEFAULT_SYMBOL, timeframe_s, bin_s, cluster_type=cluster_type, 
        only_big_trades=only_big, min_volume_filter=min_vol
    )
    vp_df = build_volume_profile_df(DEFAULT_SYMBOL, timeframe_s, bin_s, only_big_trades=only_big, min_volume_filter=min_vol)

    # Calcul des POC, VAL, VAH
    poc_val_vah_df = calculate_poc_val_vah(vp_df)

    # Log les totaux buy_vol et sell_vol
    if not fp_df.empty:
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
        margin=dict(l=50, r=50, t=30, b=50),  # Réduit la marge du haut
        hovermode="x unified",
        showlegend=False,
        dragmode='pan'  # Permet le déplacement via clic gauche et glisser
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
                showlegend=False
            ),
            row=1, col=1
        )
        logger.debug(f"Main Graph: Added Candlestick with {len(ohlc_df)} points.")
    else:
        logger.debug("Main Graph: OHLC DataFrame is empty.")

    # --- Footprint Heatmap
    if not fp_df.empty:
        pivot_fp = fp_df.pivot(index="price_bin", columns="datetime", values="value")
        if pivot_fp is not None and not pivot_fp.empty:
            pivot_fp = pivot_fp.sort_index(ascending=False)
            x_vals = list(pivot_fp.columns)
            y_vals = list(pivot_fp.index)
            z_vals = pivot_fp.fillna(0).values

            # Remplacer les z_vals où le cluster est 0 par None pour ne pas colorer
            z_vals = np.where(pivot_fp.values == 0, None, pivot_fp.values)

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
                    x=-0.15,               # Place la légende à gauche du graphique
                    thickness=60            # Ajuste la largeur de la barre
                ),
                "opacity": 0.3,  # Ajuster l'opacité pour mieux voir les carrés rouges
                "showscale": True,  # Afficher la légende
                "hovertemplate": "Time: %{x}<br>Price Bin: %{y}<br>Value: %{z}<extra></extra>"
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
        logger.debug("Main Graph: Footprint DataFrame is empty.")

    # --- Volume Profile intégré en tant que Scatter
    if not vp_df.empty and not ohlc_df.empty:
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
                    opacity=0.1  # Réduit l'opacité à 0.1
                ),
                name="Volume Profile",
                showlegend=False
            ),
            row=1, col=1
        )
        logger.debug("Main Graph: Added Volume Profile Scatter.")

        # --- POC, VAL, VAH Cercles
        if not poc_val_vah_df.empty:
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
                    hovertemplate='POC: %{y}<extra></extra>'
                ),
                row=1, col=1
            )
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
                    hovertemplate='VAL: %{y}<extra></extra>'
                ),
                row=1, col=1
            )
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
                    hovertemplate='VAH: %{y}<extra></extra>'
                ),
                row=1, col=1
            )
            logger.debug("Main Graph: Added POC, VAL, VAH markers.")
    else:
        logger.debug("Main Graph: Volume Profile DataFrame is empty or OHLC DataFrame is empty.")

    # --- Delta et Comptes Longs/Courts Texte (annotations centrées à l'intérieur des bougies)
    if not ohlc_df.empty:
        # Calculer les deltas en vectorisé
        ohlc_df["delta"] = ohlc_df['close'] - ohlc_df['open']
        annotations = []
        for _, row in ohlc_df.iterrows():
            # Positionnement exact au centre de la bougie
            y_position = (row["open"] + row["close"]) / 2
            annotations.append(dict(
                x=row["datetime"],
                y=y_position,
                text=f"Δ {row['delta']:.2f}<br>L: {row['longs']} S: {row['shorts']}",
                showarrow=False,
                xanchor='center',  # Centrer horizontalement
                yanchor='middle',  # Centrer verticalement
                font=dict(color="white", size=10),  # Taille de police adaptée
                align="center"
            ))
        fig.update_layout(annotations=annotations)
        logger.debug("Main Graph: Added Delta and Long/Short annotations.")

    # --- Footprint Texte (Acheteur / Vendeur)
    if "show_text" in show_text and not fp_df.empty:
        # Filtrer les données pour éviter la surcharge
        text_df = fp_df.copy()
        text_df = text_df[(text_df["buy_vol"] > 0) | (text_df["sell_vol"] > 0)]

        # Préparer les textes pour les acheteurs et les vendeurs
        buy_text_df = text_df[text_df["buy_vol"] > 0].copy()
        sell_text_df = text_df[text_df["sell_vol"] > 0].copy()

        # Positionner les textes des acheteurs à gauche de la bougie
        buy_text_df["x"] = buy_text_df["datetime"] - pd.Timedelta(seconds=timeframe_s / 2)
        buy_text_df["y"] = buy_text_df["price_bin"]

        # Positionner les textes des vendeurs à droite de la bougie
        sell_text_df["x"] = sell_text_df["datetime"] + pd.Timedelta(seconds=timeframe_s / 2)
        sell_text_df["y"] = sell_text_df["price_bin"]

        # Limiter le nombre de textes affichés pour des performances optimales
        MAX_TEXTS = 1000

        if len(buy_text_df) > MAX_TEXTS:
            buy_text_df = buy_text_df.sample(n=MAX_TEXTS//2, random_state=1)
        if len(sell_text_df) > MAX_TEXTS:
            sell_text_df = sell_text_df.sample(n=MAX_TEXTS//2, random_state=1)

        # Ajouter une trace Scatter pour les textes des acheteurs
        if not buy_text_df.empty:
            fig.add_trace(
                go.Scatter(
                    x=buy_text_df["x"],
                    y=buy_text_df["y"],
                    mode='text',
                    text=buy_text_df["buy_vol"].apply(lambda x: f"{x:.1f}"),
                    textposition='middle right',
                    textfont=dict(
                        size=10,
                        color='green'  # Acheteurs en vert
                    ),
                    showlegend=False,
                    hoverinfo='none'  # Désactiver les info-bulles pour le texte
                ),
                row=1, col=1
            )
            logger.debug("Main Graph: Added Buy Footprint Text annotations.")

        # Ajouter une trace Scatter pour les textes des vendeurs
        if not sell_text_df.empty:
            fig.add_trace(
                go.Scatter(
                    x=sell_text_df["x"],
                    y=sell_text_df["y"],
                    mode='text',
                    text=sell_text_df["sell_vol"].apply(lambda x: f"{x:.1f}"),
                    textposition='middle left',
                    textfont=dict(
                        size=10,
                        color='red'  # Vendeurs en rouge
                    ),
                    showlegend=False,
                    hoverinfo='none'  # Désactiver les info-bulles pour le texte
                ),
                row=1, col=1
            )
            logger.debug("Main Graph: Added Sell Footprint Text annotations.")

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

    # Supprimer le range slider de l'axe des X
    fig.update_layout(xaxis_rangeslider_visible=False)

    # Vérifier si la figure contient des données
    if not fig.data:
        logger.warning("Main Graph: Figure contains no data.")
    else:
        logger.debug(f"Main Graph: Figure contains {len(fig.data)} traces.")

    return fig

# Callback pour traiter les buffers de trades régulièrement
def periodic_trade_processor():
    """
    Processus en arrière-plan pour traiter les buffers de trades et les ajouter au DataFrame principal.
    """
    logger.info("Started trade processor thread.")
    while not shutdown_event.is_set():
        process_trades_buffer()
        time.sleep(1)  # Traiter les buffers toutes les secondes
    logger.info("Stopped trade processor thread.")

# Lancer le processeur de trades dans un thread séparé
trade_processor_thread = threading.Thread(target=periodic_trade_processor, daemon=True)
trade_processor_thread.start()

# ============================================================
#                       MAIN
# ============================================================

def signal_handler(sig, frame):
    """Gestionnaire de signal pour une fermeture propre."""
    logger.info("Received shutdown signal.")
    stop_websockets()
    shutdown_event.set()
    sys.exit(0)

# Enregistrer le gestionnaire de signal pour une fermeture propre
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    try:
        # Initialiser les données historiques avant de démarrer les WebSockets
        initialize_historical_data()

        # Démarrer les WebSockets pour le symbole par défaut
        start_trade_ws_thread()
        start_depth_ws_thread()

        # Lancer le serveur Dash en mode débogage pour plus d'informations
        logger.info("Starting Dash server.")
        app.run_server(debug=False, host="0.0.0.0", port=8050)
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
    finally:
        stop_websockets()
        shutdown_event.set()
        trade_processor_thread.join(timeout=5)
        logger.info("Application has been shut down.")
