from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from google.cloud import bigquery
import pandas as pd
import plotly.express as px

app = FastAPI()

@app.get("/", response_class=HTMLResponse)
def scatter_plot():

    client = bigquery.Client()

    query = """
    WITH filtered_annotations AS (
    SELECT
        ID,
        symbol,
        SAFE_CAST(AF AS FLOAT64) AS af,
        SAFE_CAST(gnomADg_AF AS FLOAT64) AS gnomad_af,
        CANONICAL,
        Consequence,
        VARIANT_CLASS,
        BIOTYPE,
        SIFT,
        CHROM,
        ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CANONICAL DESC) AS rn
    FROM `shc-variants.igg_dev.combined_annotations`
)

SELECT
    a.ID,
    a.symbol,
    c.subtype,
    a.af,
    a.gnomad_af,
    (a.gnomad_af - a.af) AS mean_diff,
    a.CANONICAL AS canonical,
    a.Consequence AS consequence,
    a.VARIANT_CLASS AS variant_class,
    a.BIOTYPE AS biotype,
    a.SIFT AS sift,
    a.CHROM AS chromosome
FROM filtered_annotations a
JOIN `shc-variants.igg_dev.compare_subtype_control_clean` c
    ON a.ID = c.id
WHERE a.rn = 1
AND a.af IS NOT NULL
AND a.gnomad_af IS NOT NULL
LIMIT 5000

    """

    df = client.query(query).to_dataframe()

    fig = px.scatter(
        df,
        x="af",
        y="gnomad_af",
        color="subtype",
        hover_data=["ID", "symbol", "mean_diff"],
        title="AF vs gnomAD AF Scatter Plot"
    )

    return fig.to_html()
