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
    WITH annotations_dedup AS (
        SELECT
            ID,
            ANY_VALUE(symbol) AS symbol,
            ANY_VALUE(SAFE_CAST(AF AS FLOAT64)) AS af,
            ANY_VALUE(SAFE_CAST(gnomADg_AF AS FLOAT64)) AS gnomad_af,
            ANY_VALUE(CANONICAL) AS canonical,
            ANY_VALUE(Consequence) AS consequence,
            ANY_VALUE(VARIANT_CLASS) AS variant_class,
            ANY_VALUE(BIOTYPE) AS biotype,
            ANY_VALUE(SIFT) AS sift,
            ANY_VALUE(CHROM) AS chromosome
        FROM `shc-variants.igg_dev.combined_annotations`
        GROUP BY ID
    )

    SELECT
        a.ID,
        a.symbol,
        c.subtype,
        a.af,
        a.gnomad_af,
        (a.gnomad_af - a.af) AS mean_diff,
        a.canonical,
        a.consequence,
        a.variant_class,
        a.biotype,
        a.sift,
        a.chromosome
    FROM annotations_dedup a
    INNER JOIN `shc-variants.igg_dev.compare_subtype_control_clean` c
    ON a.ID = c.id
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
