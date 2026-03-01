"""
Tableau de Bord BI - Ventes & Achats
Lancez avec : streamlit run dashboard.py
"""

import pandas as pd
import sqlite3
import plotly.express as px
import streamlit as st
from pathlib import Path

# ─────────────────────────────────────────────────────────────
# CONFIGURATION PAGE
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Tableau de Bord BI",
    page_icon="BI",
    layout="wide",
    initial_sidebar_state="expanded",
)

BASE_DIR = Path(__file__).parent

# ─────────────────────────────────────────────────────────────
# ETL — VENTES (donnees en memoire -> SQLite)
# ─────────────────────────────────────────────────────────────
@st.cache_resource
def build_warehouse_ventes():
    raw_data = [
        {"Num_CMD": "SLSD/0001", "Date_CMD": "2024-12-28", "Client": "SARL ABC",    "Adresse": "Cite 20 Aout, Alger",         "Code_Produit": "LAP.0120", "Produit": "Laptop HP Probook G4", "Qte": 4,  "Montant_HT": 500000.00},
        {"Num_CMD": "SLSD/0001", "Date_CMD": "2024-12-28", "Client": "SARL ABC",    "Adresse": "Cite 20 Aout, Alger",         "Code_Produit": "PRI.0020", "Produit": "Printer Canon 6030",   "Qte": 1,  "Montant_HT":  49000.00},
        {"Num_CMD": "SLSD/0001", "Date_CMD": "2024-12-28", "Client": "SARL ABC",    "Adresse": "Cite 20 Aout, Alger",         "Code_Produit": "INK.0034", "Produit": "Toner Canon 6030",     "Qte": 1,  "Montant_HT":   1800.00},
        {"Num_CMD": "SLSR/0002", "Date_CMD": "2025-02-22", "Client": "EURL XYZ",    "Adresse": "Cooperative Rym, Blida",      "Code_Produit": "LAP.0011", "Produit": "Laptop Lenovo 110",    "Qte": 1,  "Montant_HT":  89000.00},
        {"Num_CMD": "SLSR/0002", "Date_CMD": "2025-02-22", "Client": "EURL XYZ",    "Adresse": "Cooperative Rym, Blida",      "Code_Produit": "PRI.0020", "Produit": "Printer Canon 6030",   "Qte": 2,  "Montant_HT":  98000.00},
        {"Num_CMD": "SLSR/0002", "Date_CMD": "2025-02-22", "Client": "EURL XYZ",    "Adresse": "Cooperative Rym, Blida",      "Code_Produit": "INK.0004", "Produit": "Toner Canon 6030",     "Qte": 2,  "Montant_HT":   3600.00},
        {"Num_CMD": "SLSR/0002", "Date_CMD": "2025-02-22", "Client": "EURL XYZ",    "Adresse": "Cooperative Rym, Blida",      "Code_Produit": "SCA.0002", "Produit": "Scanner Epson 1600",   "Qte": 1,  "Montant_HT":  21000.00},
        {"Num_CMD": "SLSD/0003", "Date_CMD": "2025-03-15", "Client": "SARL AGRODZ", "Adresse": "Cite 310 logt Kouba, Alger",  "Code_Produit": "PRI.0011", "Produit": "Printer EPSON 3010",   "Qte": 2,  "Montant_HT":  64000.00},
        {"Num_CMD": "SLSD/0003", "Date_CMD": "2025-03-15", "Client": "SARL AGRODZ", "Adresse": "Cite 310 logt Kouba, Alger",  "Code_Produit": "LAP.0120", "Produit": "Laptop HP Probook G4", "Qte": 1,  "Montant_HT": 125000.00},
        {"Num_CMD": "SLSG/0004", "Date_CMD": "2025-03-28", "Client": "SNC Wiffak",  "Adresse": "Boulevard Nord, Setif",        "Code_Produit": "INK.0001", "Produit": "INK Canon 3210",       "Qte": 10, "Montant_HT":  18000.00},
        {"Num_CMD": "SLSD/0005", "Date_CMD": "2025-03-28", "Client": "EURL XYZ",    "Adresse": "Cooperative Rym, Oran",        "Code_Produit": "LAP.0011", "Produit": "Laptop Lenovo 110",    "Qte": 3,  "Montant_HT": 267000.00},
        {"Num_CMD": "SLSD/0005", "Date_CMD": "2025-03-28", "Client": "EURL XYZ",    "Adresse": "Cooperative Rym, Oran",        "Code_Produit": "PRI.0011", "Produit": "Printer EPSON 3010",   "Qte": 1,  "Montant_HT":  32000.00},
        {"Num_CMD": "SLSD/0005", "Date_CMD": "2025-03-28", "Client": "EURL XYZ",    "Adresse": "Cooperative Rym, Oran",        "Code_Produit": "INK.0005", "Produit": "INK Epson 110",        "Qte": 10, "Montant_HT":   8000.00},
        {"Num_CMD": "SLSG/0006", "Date_CMD": "2025-05-02", "Client": "SARL ABC",    "Adresse": "Cite 20 Aout, Alger",          "Code_Produit": "LAP.0120", "Produit": "Laptop HP Probook G4", "Qte": 2,  "Montant_HT": 250000.00},
        {"Num_CMD": "SLSD/0007", "Date_CMD": "2025-05-04", "Client": "EURL HAMIDI", "Adresse": "Promotion Bahia, Oran",        "Code_Produit": "PRI.0020", "Produit": "Printer Canon 6030",   "Qte": 2,  "Montant_HT":  98000.00},
    ]
    df = pd.DataFrame(raw_data)
    df["Date_CMD"]        = pd.to_datetime(df["Date_CMD"])
    df["Wilaya"]          = df["Adresse"].apply(lambda x: x.split(",")[-1].strip())
    df["Forme_Juridique"] = df["Client"].apply(lambda x: x.split(" ")[0])
    df["Categorie"]       = df["Code_Produit"].apply(lambda x: x.split(".")[0])
    df["Type_Vente"]      = df["Num_CMD"].apply(lambda x: x.split("/")[0])

    dim_client  = df[["Client", "Wilaya", "Forme_Juridique"]].drop_duplicates().reset_index(drop=True)
    dim_client["ID_Client"] = dim_client.index + 1
    dim_produit = df[["Code_Produit", "Produit", "Categorie"]].drop_duplicates().reset_index(drop=True)
    dim_produit["ID_Produit"] = dim_produit.index + 1
    dim_temps   = df[["Date_CMD"]].drop_duplicates().sort_values("Date_CMD").reset_index(drop=True)
    dim_temps["ID_Temps"] = dim_temps.index + 1
    dim_temps["Annee"]    = dim_temps["Date_CMD"].dt.year
    dim_temps["Mois"]     = dim_temps["Date_CMD"].dt.month
    dim_temps["Nom_Mois"] = dim_temps["Date_CMD"].dt.month_name()
    dim_type    = df[["Type_Vente"]].drop_duplicates().reset_index(drop=True)
    dim_type["ID_Type"] = dim_type.index + 1

    fait = df.copy()
    fait = fait.merge(dim_client,  on=["Client", "Wilaya", "Forme_Juridique"])
    fait = fait.merge(dim_produit, on=["Code_Produit", "Produit", "Categorie"])
    fait = fait.merge(dim_temps[["Date_CMD", "ID_Temps"]], on="Date_CMD")
    fait = fait.merge(dim_type, on="Type_Vente")
    fait_final = fait[["ID_Client", "ID_Produit", "ID_Temps", "ID_Type", "Qte", "Montant_HT"]]

    db   = str(BASE_DIR / "warehouse_ventes.db")
    conn = sqlite3.connect(db)
    dim_client.to_sql("Dim_Client",   conn, if_exists="replace", index=False)
    dim_produit.to_sql("Dim_Produit", conn, if_exists="replace", index=False)
    dim_temps.to_sql("Dim_Temps",     conn, if_exists="replace", index=False)
    dim_type.to_sql("Dim_Type",       conn, if_exists="replace", index=False)
    fait_final.to_sql("Fait_Ventes",  conn, if_exists="replace", index=False)
    conn.close()
    return db


@st.cache_data
def load_ventes(db):
    conn = sqlite3.connect(db)
    df = pd.read_sql_query("""
        SELECT t.Date_CMD, t.Annee, t.Mois, t.Nom_Mois,
               p.Code_Produit, p.Produit, p.Categorie,
               c.Client, c.Wilaya, c.Forme_Juridique,
               typ.Type_Vente,
               f.Qte, f.Montant_HT
        FROM Fait_Ventes f
        JOIN Dim_Temps   t   ON f.ID_Temps  = t.ID_Temps
        JOIN Dim_Produit p   ON f.ID_Produit = p.ID_Produit
        JOIN Dim_Client  c   ON f.ID_Client  = c.ID_Client
        JOIN Dim_Type    typ ON f.ID_Type    = typ.ID_Type
    """, conn)
    conn.close()
    df["Date_CMD"] = pd.to_datetime(df["Date_CMD"])
    return df


# ─────────────────────────────────────────────────────────────
# ETL — ACHATS (CSV -> SQLite)
# ─────────────────────────────────────────────────────────────
@st.cache_resource
def build_warehouse_achats():
    csv_path = BASE_DIR / "data" / "achats.csv"
    df = pd.read_csv(csv_path)
    df.columns = [c.strip().replace(" ", "_").replace(".", "_") for c in df.columns]
    df.rename(columns={"QTY": "Qte"}, inplace=True)
    df["Date_CMD"]   = pd.to_datetime(df["Date_CMD"])
    df["Categorie"]  = df["Code_Produit"].apply(lambda x: x.split(".")[0])
    df["Type_Achat"] = df["Num_CMD"].apply(lambda x: x.split("/")[0])

    dim_fourn  = df[["Fournisseur"]].drop_duplicates().reset_index(drop=True)
    dim_fourn["ID_Fournisseur"] = dim_fourn.index + 1
    dim_prod   = df[["Code_Produit", "Produit", "Categorie"]].drop_duplicates().reset_index(drop=True)
    dim_prod["ID_Produit"] = dim_prod.index + 1
    dim_temps  = df[["Date_CMD"]].drop_duplicates().sort_values("Date_CMD").reset_index(drop=True)
    dim_temps["ID_Temps"] = dim_temps.index + 1
    dim_temps["Annee"]    = dim_temps["Date_CMD"].dt.year
    dim_temps["Mois"]     = dim_temps["Date_CMD"].dt.month
    dim_temps["Nom_Mois"] = dim_temps["Date_CMD"].dt.month_name()
    dim_type   = df[["Type_Achat"]].drop_duplicates().reset_index(drop=True)
    dim_type["ID_Type"] = dim_type.index + 1

    fait = df.copy()
    fait = fait.merge(dim_fourn, on="Fournisseur")
    fait = fait.merge(dim_prod,  on=["Code_Produit", "Produit", "Categorie"])
    fait = fait.merge(dim_temps[["Date_CMD", "ID_Temps"]], on="Date_CMD")
    fait = fait.merge(dim_type, on="Type_Achat")
    fait_final = fait[["ID_Fournisseur", "ID_Produit", "ID_Temps", "ID_Type",
                        "Qte", "Montant_HT", "Taxe", "Montant_TTC"]].reset_index(drop=True)

    db   = str(BASE_DIR / "warehouse_achats.db")
    conn = sqlite3.connect(db)
    dim_fourn.to_sql("Dim_Fournisseur", conn, if_exists="replace", index=False)
    dim_prod.to_sql("Dim_Produit",      conn, if_exists="replace", index=False)
    dim_temps.to_sql("Dim_Temps",       conn, if_exists="replace", index=False)
    dim_type.to_sql("Dim_Type",         conn, if_exists="replace", index=False)
    fait_final.to_sql("Fait_Achats",    conn, if_exists="replace", index=False)
    conn.close()
    return db


@st.cache_data
def load_achats(db):
    conn = sqlite3.connect(db)
    df = pd.read_sql_query("""
        SELECT t.Date_CMD, t.Annee, t.Mois, t.Nom_Mois,
               p.Produit, p.Code_Produit, p.Categorie,
               f.Fournisseur, typ.Type_Achat,
               a.Qte, a.Montant_HT, a.Taxe, a.Montant_TTC
        FROM Fait_Achats a
        JOIN Dim_Temps       t   ON a.ID_Temps       = t.ID_Temps
        JOIN Dim_Produit     p   ON a.ID_Produit      = p.ID_Produit
        JOIN Dim_Fournisseur f   ON a.ID_Fournisseur  = f.ID_Fournisseur
        JOIN Dim_Type        typ ON a.ID_Type         = typ.ID_Type
    """, conn)
    conn.close()
    df["Date_CMD"] = pd.to_datetime(df["Date_CMD"])
    return df


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
MONTH_ORDER = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def fmt(n):
    return f"{n:,.2f} DA"


# ─────────────────────────────────────────────────────────────
# FUSION — MARGES : warehouse_marges.db + Vue_Marges (VIEW SQL)
# ─────────────────────────────────────────────────────────────
@st.cache_resource
def build_warehouse_marges(db_v, db_a):
    """Cree warehouse_marges.db :
    - Copie les tables des deux entrepots (prefixes V_ et A_)
    - Cree la Vue_Marges (VIEW) qui fait la jointure et calcule
      le prix unitaire par ligne de commande (pas en moyenne globale)
    """
    db = str(BASE_DIR / "warehouse_marges.db")
    conn = sqlite3.connect(db)

    # -- Copier tables Ventes --
    conn_v = sqlite3.connect(db_v)
    for tbl in ["Fait_Ventes", "Dim_Client", "Dim_Produit", "Dim_Temps", "Dim_Type"]:
        pd.read_sql_query(f"SELECT * FROM {tbl}", conn_v).to_sql(
            f"V_{tbl}", conn, if_exists="replace", index=False
        )
    conn_v.close()

    # -- Copier tables Achats --
    conn_a = sqlite3.connect(db_a)
    for tbl in ["Fait_Achats", "Dim_Fournisseur", "Dim_Produit", "Dim_Temps", "Dim_Type"]:
        pd.read_sql_query(f"SELECT * FROM {tbl}", conn_a).to_sql(
            f"A_{tbl}", conn, if_exists="replace", index=False
        )
    conn_a.close()

    # -- Creer la Vue_Marges --
    conn.execute("DROP VIEW IF EXISTS Vue_Marges")
    conn.execute("""
    CREATE VIEW Vue_Marges AS
    SELECT
        vt.Date_CMD,
        vt.Annee,
        vt.Mois,
        vt.Nom_Mois,
        vp.Code_Produit,
        vp.Produit,
        vp.Categorie,
        vc.Client,
        vc.Wilaya,
        vc.Forme_Juridique,
        vtyp.Type_Vente,
        fv.Qte,
        -- Prix unitaire de VENTE calcule par ligne de commande
        ROUND(CAST(fv.Montant_HT AS REAL) / fv.Qte, 2)                         AS Prix_Vente_U,
        -- Prix unitaire d'ACHAT : moyenne ponderee toutes commandes du produit
        COALESCE(pa.Prix_Achat_U, 0.0)                                          AS Prix_Achat_U,
        -- Marge unitaire et totale
        ROUND(CAST(fv.Montant_HT AS REAL) / fv.Qte
              - COALESCE(pa.Prix_Achat_U, 0.0), 2)                              AS Marge_U,
        ROUND((CAST(fv.Montant_HT AS REAL) / fv.Qte
               - COALESCE(pa.Prix_Achat_U, 0.0)) * fv.Qte, 2)                  AS Marge_Totale,
        -- Taux de marge
        ROUND(
            CASE WHEN fv.Montant_HT = 0 THEN 0
                 ELSE (CAST(fv.Montant_HT AS REAL) / fv.Qte
                       - COALESCE(pa.Prix_Achat_U, 0.0))
                      / (CAST(fv.Montant_HT AS REAL) / fv.Qte) * 100
            END, 2
        )                                                                        AS Taux_Marge
    FROM V_Fait_Ventes  fv
    JOIN V_Dim_Temps    vt   ON fv.ID_Temps   = vt.ID_Temps
    JOIN V_Dim_Produit  vp   ON fv.ID_Produit = vp.ID_Produit
    JOIN V_Dim_Client   vc   ON fv.ID_Client  = vc.ID_Client
    JOIN V_Dim_Type     vtyp ON fv.ID_Type    = vtyp.ID_Type
    -- Sous-requete : prix d'achat moyen pondere par produit (toutes commandes)
    LEFT JOIN (
        SELECT
            ap.Code_Produit,
            ROUND(SUM(CAST(fa.Montant_HT AS REAL)) / SUM(fa.Qte), 2) AS Prix_Achat_U
        FROM A_Fait_Achats fa
        JOIN A_Dim_Produit ap ON fa.ID_Produit = ap.ID_Produit
        GROUP BY ap.Code_Produit
    ) pa ON vp.Code_Produit = pa.Code_Produit
    """)
    conn.commit()
    conn.close()
    return db


@st.cache_data
def load_marges(db):
    """Charge la Vue_Marges depuis warehouse_marges.db."""
    conn = sqlite3.connect(db)
    df = pd.read_sql_query("SELECT * FROM Vue_Marges", conn)
    conn.close()
    df["Date_CMD"] = pd.to_datetime(df["Date_CMD"])
    return df


# ─────────────────────────────────────────────────────────────
# BUILD WAREHOUSES ONCE
# ─────────────────────────────────────────────────────────────
db_v = build_warehouse_ventes()
db_a = build_warehouse_achats()
db_m = build_warehouse_marges(db_v, db_a)
df_v = load_ventes(db_v)
df_a = load_achats(db_a)
df_m = load_marges(db_m)


# ─────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────
st.sidebar.title("Tableau de Bord BI")
st.sidebar.markdown("---")

section = st.sidebar.radio(
    "Choisir la section :",
    ["Ventes", "Achats", "Marges"],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.caption("Data Warehouse - SQLite - Plotly - Streamlit")


# =============================================================
# SECTION MARGES  (ajoutee apres la section Achats)
# =============================================================
def section_marges(df_m):
    st.title("Analyse des Marges")
    st.markdown(
        """
        **Indicateur** : Marge = Prix de vente unitaire - Prix d'achat unitaire  
        Personnalisez librement les axes X, la couleur et les facettes ci-dessous.
        """
    )
    st.markdown("---")

    # ── Filtres dynamiques dans la sidebar ──────────────────
    with st.sidebar.expander("Filtres Marges", expanded=True):
        # Filtres multi-selects
        all_ann = sorted(df_m["Annee"].dropna().unique().tolist())
        sel_ann = st.multiselect("Annee(s)", all_ann, default=all_ann, key="m_ann")

        all_cat = sorted(df_m["Categorie"].dropna().unique().tolist())
        sel_cat = st.multiselect("Categorie(s)", all_cat, default=all_cat, key="m_cat")

        all_wil = sorted(df_m["Wilaya"].dropna().unique().tolist())
        sel_wil = st.multiselect("Wilaya(s)", all_wil, default=all_wil, key="m_wil")

        all_prod = sorted(df_m["Produit"].dropna().unique().tolist())
        sel_prod = st.multiselect("Produit(s)", all_prod, default=all_prod, key="m_prod")

    # Appliquer les filtres
    mask = (
        df_m["Annee"].isin(sel_ann if sel_ann else all_ann) &
        df_m["Categorie"].isin(sel_cat if sel_cat else all_cat) &
        df_m["Wilaya"].isin(sel_wil if sel_wil else all_wil) &
        df_m["Produit"].isin(sel_prod if sel_prod else all_prod)
    )
    dff = df_m[mask].copy()

    if dff.empty:
        st.warning("Aucune donnee pour les filtres selectionnes.")
        return

    # ── KPIs ────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Marge Totale (HT)",   fmt(dff["Marge_Totale"].sum()))
    k2.metric("Marge Unitaire Moy.", fmt(dff["Marge_U"].mean()))
    k3.metric("Taux de Marge Moy.",  f"{dff['Taux_Marge'].mean():.1f} %")
    k4.metric("Lignes analysees",    len(dff))

    st.markdown("---")

    # ── Parametres du graphique ──────────────────────────────
    DIMS = ["Produit", "Categorie", "Wilaya", "Nom_Mois", "Annee", "Type_Vente"]
    INDICATEURS = {
        "Marge Totale (DA)":    "Marge_Totale",
        "Marge Unitaire (DA)": "Marge_U",
        "Taux de Marge (%)": "Taux_Marge",
    }
    CHARTS = ["Barres", "Ligne", "Dispersion", "Boite a moustaches", "Sunburst", "Treemap"]

    with st.expander("Parametres de l'analyse", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            indicateur = st.selectbox("Indicateur", list(INDICATEURS.keys()), key="m_ind")
            chart_type = st.selectbox("Type de graphique", CHARTS, key="m_chart")
        with col2:
            axe_x   = st.selectbox("Axe X",   DIMS, index=0, key="m_x")
            couleur = st.selectbox("Couleur",  ["(aucune)"] + DIMS, index=2, key="m_col")
        with col3:
            facet_col = st.selectbox("Facette colonne", ["(aucune)"] + DIMS, index=0, key="m_fc")
            facet_row = st.selectbox("Facette ligne",   ["(aucune)"] + DIMS, index=0, key="m_fr")

    ind_col   = INDICATEURS[indicateur]
    color_col = None if couleur  == "(aucune)" else couleur
    fc        = None if facet_col == "(aucune)" else facet_col
    fr        = None if facet_row == "(aucune)" else facet_row

    # Agregation
    grp_cols = list(dict.fromkeys(
        [axe_x]
        + ([color_col] if color_col else [])
        + ([fc]        if fc        else [])
        + ([fr]        if fr        else [])
    ))

    if ind_col == "Taux_Marge":
        # taux = marge_totale / (prix_vente * qte) * 100
        agg = dff.groupby(grp_cols).apply(
            lambda g: pd.Series({
                "Taux_Marge": (g["Marge_Totale"].sum() / (g["Prix_Vente_U"] * g["Qte"]).sum() * 100)
                              if (g["Prix_Vente_U"] * g["Qte"]).sum() != 0 else 0
            })
        ).reset_index()
    else:
        agg = dff.groupby(grp_cols)[ind_col].sum().reset_index()

    # Tri
    if axe_x in agg.columns and ind_col in agg.columns:
        if axe_x == "Nom_Mois":
            agg["_mois_order"] = agg["Nom_Mois"].map({m: i for i, m in enumerate(MONTH_ORDER)})
            agg = agg.sort_values("_mois_order").drop(columns="_mois_order")
        else:
            agg = agg.sort_values(ind_col, ascending=False)

    # ── Tracé ───────────────────────────────────────────────
    title = f"{indicateur} par {axe_x}"
    if color_col:
        title += f" / {color_col}"

    cat_orders = {"Nom_Mois": MONTH_ORDER} if "Nom_Mois" in [axe_x, color_col, fc, fr] else None

    if chart_type == "Barres":
        fig = px.bar(
            agg, x=axe_x, y=ind_col,
            color=color_col, facet_col=fc, facet_row=fr,
            title=title, text_auto=".2s", barmode="group",
            category_orders=cat_orders,
            labels={ind_col: indicateur},
        )
        fig.update_layout(xaxis_tickangle=-30)

    elif chart_type == "Ligne":
        fig = px.line(
            agg, x=axe_x, y=ind_col,
            color=color_col, facet_col=fc, facet_row=fr,
            title=title, markers=True,
            category_orders=cat_orders,
            labels={ind_col: indicateur},
        )

    elif chart_type == "Dispersion":
        fig = px.scatter(
            dff, x=axe_x, y=ind_col,
            color=color_col, facet_col=fc, facet_row=fr,
            title=title, size="Qte", hover_data=["Produit", "Client", "Wilaya"],
            labels={ind_col: indicateur},
        )

    elif chart_type == "Boite a moustaches":
        fig = px.box(
            dff, x=axe_x, y=ind_col,
            color=color_col, facet_col=fc, facet_row=fr,
            title=title, points="all",
            labels={ind_col: indicateur},
        )

    elif chart_type == "Sunburst":
        path = list(dict.fromkeys(filter(None, [color_col, axe_x])))
        if not path:
            path = ["Categorie", "Produit"]
        fig = px.sunburst(
            agg, path=path, values=ind_col,
            title=title,
            color=ind_col, color_continuous_scale="RdYlGn",
        )

    elif chart_type == "Treemap":
        path = list(dict.fromkeys(filter(None, [color_col, axe_x])))
        if not path:
            path = ["Categorie", "Produit"]
        fig = px.treemap(
            agg, path=path, values=ind_col,
            title=title,
            color=ind_col, color_continuous_scale="RdYlGn",
        )

    st.plotly_chart(fig, use_container_width=True)

    # ── Tableau de detail ────────────────────────────────────
    with st.expander("Voir le tableau de donnees"):
        show_cols = ["Produit", "Categorie", "Wilaya", "Annee", "Nom_Mois",
                        "Type_Vente", "Qte", "Prix_Vente_U", "Prix_Achat_U",
                        "Marge_U", "Marge_Totale", "Taux_Marge"]
        st.dataframe(
            dff[[c for c in show_cols if c in dff.columns]]
            .sort_values("Marge_Totale", ascending=False)
            .reset_index(drop=True),
            use_container_width=True,
        )


# =============================================================
# SECTION VENTES
# =============================================================
if section == "Ventes":
    st.title("Analyse des Ventes")

    QUESTIONS_V = {
        "Q1 — Produits vendus apres le 01/02/2025":
            "Liste filtree des lignes de vente posterieures au 1er fevrier 2025.",
        "Q2 — Classement produits par CA, Type Vente et Annee":
            "Chiffre d'affaires cumule par produit, segmente par type de vente et annee.",
        "Q3 — Classement clients par Wilaya et Forme Juridique":
            "Montant total des ventes par client, organise par wilaya et statut juridique.",
        "Q4 — Ventes quantitatives par Categorie, Type, Mois et Annee":
            "Quantites vendues par categorie de produit sur l'axe temporel.",
        "Q5 — Categorie la plus rentable":
            "Categorie de produit ayant genere le plus gros chiffre d'affaires.",
    }

    q_label = st.selectbox("Selectionner une question :", list(QUESTIONS_V.keys()))
    st.caption(QUESTIONS_V[q_label])

    run = st.button("Lancer l'analyse", type="primary")

    if run:
        st.markdown("---")

        # Q1
        if q_label.startswith("Q1"):
            res = (
                df_v[df_v["Date_CMD"] > "2025-02-01"]
                [["Date_CMD", "Produit", "Categorie", "Client", "Wilaya",
                  "Type_Vente", "Qte", "Montant_HT"]]
                .sort_values("Date_CMD")
                .reset_index(drop=True)
            )
            res["Date_CMD"] = res["Date_CMD"].dt.date

            st.subheader(f"{len(res)} ligne(s) trouvee(s)")
            st.dataframe(res, use_container_width=True)

            col1, col2, col3 = st.columns(3)
            col1.metric("Lignes", len(res))
            col2.metric("CA total (HT)", fmt(res["Montant_HT"].sum()))
            col3.metric("Quantites totales", int(res["Qte"].sum()))

        # Q2
        elif q_label.startswith("Q2"):
            st.subheader("Classement des produits par Chiffre d'Affaires")

            col_g, col_f = st.columns(2)
            with col_g:
                group_by_type = st.checkbox("Segmenter par Type Vente", value=True)
            with col_f:
                group_by_year = st.checkbox("Segmenter par Annee", value=True)

            grp = ["Produit"]
            color_col = "Type_Vente" if group_by_type else None
            facet_col = "Annee"      if group_by_year  else None
            if color_col:
                grp.append(color_col)
            if facet_col and facet_col not in grp:
                grp.append(facet_col)

            df_q2 = df_v.groupby(grp)["Montant_HT"].sum().reset_index()
            df_q2 = df_q2.sort_values("Montant_HT", ascending=False)

            fig = px.bar(
                df_q2, x="Produit", y="Montant_HT",
                color=color_col, facet_col=facet_col,
                title="Chiffre d'Affaires par Produit",
                text_auto=".2s", barmode="group",
                labels={"Montant_HT": "CA (DA)"},
            )
            fig.update_layout(xaxis_tickangle=-35)
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df_q2, use_container_width=True)

        # Q3
        elif q_label.startswith("Q3"):
            st.subheader("Classement des clients par Wilaya et Forme Juridique")

            chart_type = st.radio(
                "Type de graphique :", ["Sunburst", "Barres groupees"], horizontal=True
            )

            df_q3 = (
                df_v.groupby(["Wilaya", "Forme_Juridique", "Client"])["Montant_HT"]
                .sum()
                .reset_index()
            )

            if chart_type == "Sunburst":
                fig = px.sunburst(
                    df_q3,
                    path=["Wilaya", "Forme_Juridique", "Client"],
                    values="Montant_HT",
                    title="Ventes par Wilaya > Forme Juridique > Client",
                    color="Montant_HT",
                    color_continuous_scale="RdBu",
                )
            else:
                fig = px.bar(
                    df_q3.sort_values("Montant_HT", ascending=False),
                    x="Client", y="Montant_HT",
                    color="Wilaya", facet_col="Forme_Juridique",
                    title="CA par Client (Wilaya / Forme Juridique)",
                    text_auto=".2s", barmode="group",
                    labels={"Montant_HT": "CA (DA)"},
                )
                fig.update_layout(xaxis_tickangle=-30)

            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(
                df_q3.sort_values("Montant_HT", ascending=False).reset_index(drop=True),
                use_container_width=True,
            )

        # Q4
        elif q_label.startswith("Q4"):
            st.subheader("Ventes quantitatives par Categorie, Type, Mois et Annee")

            df_q4 = (
                df_v.groupby(["Annee", "Nom_Mois", "Mois", "Categorie", "Type_Vente"])["Qte"]
                .sum()
                .reset_index()
                .sort_values(["Annee", "Mois"])
            )
            mois_presents = [m for m in MONTH_ORDER if m in df_q4["Nom_Mois"].values]

            fig = px.bar(
                df_q4,
                x="Nom_Mois", y="Qte",
                color="Categorie",
                facet_row="Annee", facet_col="Type_Vente",
                category_orders={"Nom_Mois": mois_presents},
                title="Quantites vendues — Multidimensionnel",
                text_auto=True, barmode="group",
            )
            fig.update_xaxes(tickangle=-30)
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df_q4, use_container_width=True)

        # Q5
        elif q_label.startswith("Q5"):
            st.subheader("Categorie de produit la plus rentable")

            df_q5 = (
                df_v.groupby("Categorie")["Montant_HT"]
                .sum()
                .reset_index()
                .sort_values("Montant_HT", ascending=False)
                .reset_index(drop=True)
            )
            top = df_q5.iloc[0]

            col1, col2 = st.columns([1, 2])
            with col1:
                st.success(f"**Categorie championne : {top['Categorie']}**")
                st.metric("CA total (HT)", fmt(top["Montant_HT"]))
                st.dataframe(df_q5, use_container_width=True)
            with col2:
                fig = px.pie(
                    df_q5, names="Categorie", values="Montant_HT",
                    title="Repartition du CA par Categorie",
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                st.plotly_chart(fig, use_container_width=True)

# =============================================================
# SECTION MARGES
# =============================================================
elif section == "Marges":
    section_marges(df_m)

# =============================================================
# SECTION ACHATS
# =============================================================
else:
    st.title("Analyse des Achats")

    QUESTIONS_A = {
        "Q1 — Produits achetes en 2024":
            "Liste de toutes les lignes d'achat dont la date est en 2024.",
        "Q2 — Achats quantitatifs par Type, Mois et Annee":
            "Quantites achetees par produit, segmentees par type d'achat et periode.",
        "Q3 — Fournisseur champion par Categorie":
            "Fournisseur ayant le plus grand volume d'achat pour chaque categorie de produit.",
        "Q4 — Categorie la plus couteuse":
            "Categorie de produit qui a coute le plus d'argent a l'entreprise.",
    }

    q_label = st.selectbox("Selectionner une question :", list(QUESTIONS_A.keys()))
    st.caption(QUESTIONS_A[q_label])

    run = st.button("Lancer l'analyse", type="primary")

    if run:
        st.markdown("---")

        # Q1
        if q_label.startswith("Q1"):
            res = (
                df_a[df_a["Annee"] == 2024]
                [["Date_CMD", "Produit", "Categorie", "Fournisseur", "Type_Achat",
                  "Qte", "Montant_HT", "Montant_TTC"]]
                .sort_values("Date_CMD")
                .drop_duplicates()
                .reset_index(drop=True)
            )
            res["Date_CMD"] = res["Date_CMD"].dt.date

            st.subheader(f"{len(res)} ligne(s) trouvee(s) en 2024")
            st.dataframe(res, use_container_width=True)

            col1, col2, col3 = st.columns(3)
            col1.metric("Lignes", len(res))
            col2.metric("Cout total HT", fmt(res["Montant_HT"].sum()))
            col3.metric("Quantites totales", int(res["Qte"].sum()))

        # Q2
        elif q_label.startswith("Q2"):
            st.subheader("Achats quantitatifs par Produit, Type d'achat, Mois et Annee")

            col_g, col_f = st.columns(2)
            with col_g:
                by_type = st.checkbox("Segmenter par Type Achat", value=True)
            with col_f:
                by_year = st.checkbox("Facette par Annee", value=True)

            grp = ["Produit", "Nom_Mois", "Mois"]
            color_col = "Type_Achat" if by_type else None
            facet_row = "Annee"      if by_year  else None
            if color_col:
                grp.append(color_col)
            if facet_row and facet_row not in grp:
                grp.append(facet_row)

            df_q2 = df_a.groupby(grp)["Qte"].sum().reset_index().sort_values("Mois")
            mois_p = [m for m in MONTH_ORDER if m in df_q2["Nom_Mois"].values]

            fig = px.bar(
                df_q2, x="Produit", y="Qte",
                color=color_col, facet_row=facet_row,
                title="Quantites achetees par Produit",
                text_auto=True, barmode="group",
                category_orders={"Nom_Mois": mois_p},
            )
            fig.update_layout(xaxis_tickangle=-35)
            st.plotly_chart(fig, use_container_width=True)

            df_q2b = (
                df_a.groupby(["Annee", "Nom_Mois", "Mois", "Type_Achat"])["Qte"]
                .sum().reset_index().sort_values(["Annee", "Mois"])
            )
            mois_p2 = [m for m in MONTH_ORDER if m in df_q2b["Nom_Mois"].values]

            fig2 = px.line(
                df_q2b, x="Nom_Mois", y="Qte",
                color="Type_Achat", facet_row="Annee",
                markers=True,
                title="Evolution mensuelle des quantites achetees",
                category_orders={"Nom_Mois": mois_p2},
            )
            st.plotly_chart(fig2, use_container_width=True)
            st.dataframe(df_q2, use_container_width=True)

        # Q3
        elif q_label.startswith("Q3"):
            st.subheader("Fournisseur champion par Categorie de produit")

            indicateur = st.radio(
                "Indicateur :",
                ["Montant HT (DA)", "Quantites achetees"],
                horizontal=True,
            )
            ind_col = "Montant_HT" if "Montant" in indicateur else "Qte"

            df_q3 = df_a.groupby(["Categorie", "Fournisseur"])[ind_col].sum().reset_index()
            df_q3 = df_q3.sort_values(["Categorie", ind_col], ascending=[True, False])
            top_f  = df_q3.loc[df_q3.groupby("Categorie")[ind_col].idxmax()].reset_index(drop=True)

            col1, col2 = st.columns([1, 2])
            with col1:
                st.markdown("**Top Fournisseur par Categorie**")
                st.dataframe(top_f, use_container_width=True)
            with col2:
                fig_bar = px.bar(
                    df_q3, x="Categorie", y=ind_col,
                    color="Fournisseur",
                    title=f"{indicateur} par Categorie et Fournisseur",
                    text_auto=".2s", barmode="group",
                )
                st.plotly_chart(fig_bar, use_container_width=True)

            fig_sun = px.sunburst(
                df_q3, path=["Categorie", "Fournisseur"], values=ind_col,
                title=f"Repartition {indicateur} - Categorie > Fournisseur",
                color=ind_col, color_continuous_scale="Blues",
            )
            st.plotly_chart(fig_sun, use_container_width=True)

        # Q4
        elif q_label.startswith("Q4"):
            st.subheader("Categorie de produit la plus couteuse")

            indicateur = st.radio(
                "Indicateur :",
                ["Montant HT (DA)", "Montant TTC (DA)", "Quantites achetees"],
                horizontal=True,
            )
            ind_map = {
                "Montant HT (DA)":    "Montant_HT",
                "Montant TTC (DA)":   "Montant_TTC",
                "Quantites achetees": "Qte",
            }
            ind_col = ind_map[indicateur]

            df_q4 = (
                df_a.groupby("Categorie")[ind_col]
                .sum().reset_index()
                .sort_values(ind_col, ascending=False)
                .reset_index(drop=True)
            )
            top = df_q4.iloc[0]

            col1, col2 = st.columns([1, 2])
            with col1:
                st.error(f"**Categorie la plus couteuse : {top['Categorie']}**")
                if "Montant" in indicateur:
                    st.metric(indicateur, fmt(top[ind_col]))
                else:
                    st.metric(indicateur, int(top[ind_col]))
                st.dataframe(df_q4, use_container_width=True)
            with col2:
                fig = px.pie(
                    df_q4, names="Categorie", values=ind_col,
                    title=f"Repartition {indicateur} par Categorie",
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                )
                st.plotly_chart(fig, use_container_width=True)
