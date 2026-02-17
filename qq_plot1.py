
import logging
import os
import tempfile
import traceback

from flask import Flask, send_file, Response
from google.cloud import bigquery
import pandas as pd
import numpy as np
import plotly.graph_objects as go

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("qq_plot")

app = Flask(__name__)

# -------------------------------------------------------
# ✅ Health Check Route (FAST)
# -------------------------------------------------------
@app.route("/")
def health():
    return "QQ Plot Service is running"


# -------------------------------------------------------
# ✅ QQ Plot Route (Heavy Processing)
# -------------------------------------------------------
@app.route("/qqplot")
def generate_qq_plot():
    try:
        bq_client = bigquery.Client()

        query = """
        SELECT
            subtype,
            control_percent,
            percent_diff,
            (control_percent + percent_diff) AS case_percent
        FROM `shc-variants.igg_dev.variant_subtype_metrics`
        WHERE subtype IN ('AIS','IIS','JIS')
          AND control_percent IS NOT NULL
          AND percent_diff IS NOT NULL
          AND control_percent >= 0
          AND (control_percent + percent_diff) >= 0
          QUALIFY ROW_NUMBER() OVER (PARTITION BY subtype ORDER BY RAND()) <= 20000
        """

        df = bq_client.query(query).to_dataframe()

        if df.empty:
            return Response("No valid data returned from BigQuery", mimetype="text/plain")

        df = df.replace([np.inf, -np.inf], np.nan).dropna()

        fig = go.Figure()

        colors = {
            "AIS": "#1f77b4",
            "IIS": "#ff7f0e",
            "JIS": "#2ca02c"
        }

        # Plot each subtype
        for subtype in ["AIS", "IIS", "JIS"]:
            sub_df = df[df["subtype"] == subtype]

            if sub_df.empty:
                continue

            control_sorted = np.sort(sub_df["control_percent"].values)
            case_sorted = np.sort(sub_df["case_percent"].values)

            min_len = min(len(control_sorted), len(case_sorted))
            control_sorted = control_sorted[:min_len]
            case_sorted = case_sorted[:min_len]

            fig.add_trace(go.Scattergl(
                x=control_sorted,
                y=case_sorted,
                mode="markers",
                marker=dict(
                    size=4,
                    opacity=0.6,
                    color=colors[subtype]
                ),
                name=subtype
            ))

        # Diagonal reference line
        min_val = min(df["control_percent"].min(), df["case_percent"].min())
        max_val = max(df["control_percent"].max(), df["case_percent"].max())

        fig.add_trace(go.Scatter(
            x=[min_val, max_val],
            y=[min_val, max_val],
            mode="lines",
            line=dict(dash="dash", color="black"),
            name="y = x"
        ))

        fig.update_layout(
            title="QQ Plot: Control vs Case Frequency (AIS, IIS, JIS)",
            xaxis=dict(title="Control Frequency", showline=True),
            yaxis=dict(title="Case Frequency", showline=True),
            template="plotly_white",
            width=1000,
            height=700
        )

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
        fig.write_html(tmp.name, include_plotlyjs="cdn")

        return send_file(tmp.name, mimetype="text/html")

    except Exception:
        logger.exception("Error generating QQ plot")
        return Response(traceback.format_exc(), mimetype="text/plain", status=500)


# -------------------------------------------------------
# ✅ Required for local testing (Cloud Run uses gunicorn)
# -------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
