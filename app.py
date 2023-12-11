import pandas as pd
import dash

from dash import Dash, dcc, html, Input, Output

from dash import dcc
import plotly.express as px
import plotly.graph_objects as go
import geojson
import json
import datetime
import rasff


# -----------------------------------------------------------------------------
# Identifiers and constants
# -----------------------------------------------------------------------------
CATEGORY_DROPDOWN = "category_dropdown"
PRODUCT_DROPDOWN = "product_dropdown"

ALERTS_FIGURE = "alerts_map"
ORIGINS_FIGURE = "origins_map"
INTERVAL_SLIDER = "interval_slider"
INTERVAL_TEXT = "interval_text"

ALL = -1
categories, products = rasff.get_product_categories()

# Constants: time slider
def time_slider_to_interval(slider_value):
    """
    Converts a month interval to datetimes
    """
    values = [
        slider_value[i] + rasff.START_MONTH - i - 1 for i in range(len(slider_value))
    ]
    return [
        datetime.datetime(v // 12 + rasff.START_YEAR, v % 12 + 1, 1) for v in values
    ]


slider_interval = [
    0,
    (rasff.END_YEAR - rasff.START_YEAR) * 12 - rasff.START_MONTH + rasff.END_MONTH,
]
slider_marks = {0: "1979"}
for year in range(1982, 2021, 2):
    key = (year - rasff.START_YEAR) * 12 - rasff.START_MONTH + 1
    slider_marks[key] = str(year)


# -----------------------------------------------------------------------------
# Layout
# -----------------------------------------------------------------------------
app = dash.Dash(__name__)
app.layout = html.Div(
    className="container",
    children=[
        html.Div(
            id="product-control",
            children=[
                html.Div(
                    className="control-body",
                    children=[
                        html.Div(
                            className="control-group",
                            children=[
                                html.Label("Product Category:"),
                                dcc.Dropdown(id=CATEGORY_DROPDOWN, options=categories),
                            ],
                        ),
                        html.Div(
                            className="control-group",
                            children=[
                                html.Label("Product:"),
                                dcc.Dropdown(id=PRODUCT_DROPDOWN),
                            ],
                        ),
                        html.Div(
                            className="control-group",
                            children=[
                                html.Button("Clear View", id="control-reset-button")
                            ],
                        ),
                        html.Div(
                            className="control-group",
                            children=[
                                html.P(
                                    "Filter alerts visualized by seleting product category, product type or chaining the time-interval."
                                )
                            ],
                        ),
                    ],
                )
            ],
        ),
        dcc.Graph(id=ALERTS_FIGURE, config={"displayModeBar": False}),
        dcc.Graph(id=ORIGINS_FIGURE, config={"displayModeBar": False}),
        html.Label(id=INTERVAL_TEXT),
        dcc.RangeSlider(
            id=INTERVAL_SLIDER,
            min=slider_interval[0],
            max=slider_interval[1],
            value=slider_interval,
            marks=slider_marks,
            pushable=1,
        ),
    ],
)  # .container


# -----------------------------------------------------------------------------
# Layout
# -----------------------------------------------------------------------------
def create_europe_figure(interval, category, product):
    """
    Creates a Europe figure showing number of alerts by country in the given time interval.
    """
    countries = []
    with open("data/europe.json") as file:
        file_data = file.read()
        json_data = json.loads(file_data)
        geo_data = geojson.loads(file_data)

        for feature in json_data["features"]:
            countries.append(feature["properties"]["name"])

    alerts = rasff.group_by_country(
        rasff.select_alerts(interval=interval, category=category, product=product)
    )
    # This creates a series with value 0 for all countries not in alerts
    no_data = pd.Series(
        {}, index=[c for c in countries if c not in alerts.index], dtype="int64"
    )

    fig = go.Figure(
        data=[
            go.Choropleth(
                geojson=geo_data,
                featureidkey="properties.name",
                locations=alerts.index,
                z=alerts.values,
                colorscale=px.colors.sequential.YlGn,
                colorbar_title="Alerts",
                hovertemplate="%{z} alerts by %{location}<extra></extra>",
            ),
            go.Choropleth(
                geojson=geo_data,
                featureidkey="properties.name",
                locations=no_data.index,
                z=no_data.values,
                # Constant color: The default map color by plotly
                colorscale=[(0, "#e5ecf6"), (1, "#e5ecf6")],
                colorbar_title="Alerts",
                zmin=0,
                zmax=1,
                hoverinfo="skip",
                showlegend=False,
                showscale=False,
            ),
        ]
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        autosize=True,
        clickmode="event+select",
        width=640,
        height=400,
    )
    fig.update_geos(
        visible=False,
        scope="europe",
        projection_scale=1.6471820345351462,
        center=dict(lon=19.685758684364536, lat=54.29566481152172),
    )
    return fig


def create_world_map(countries, interval, category, product):
    alerts = rasff.select_alerts(
        countries=countries, interval=interval, category=category, product=product
    )
    origins = rasff.select_origins(alerts.index)
    by_country = rasff.group_by_country(origins)

    with open("data/world.json") as file:
        geo_data = geojson.load(file)

    fig = go.Figure(
        [
            go.Choropleth(
                geojson=geo_data,
                featureidkey="properties.name",
                locations=by_country.index,
                z=by_country.values,
                colorscale=px.colors.sequential.Reds,
                colorbar_title="Origins",
                hovertemplate="%{z} origins in %{location}<extra></extra>",
            )
        ]
    )
    fig.update_geos(
        scope="world",
        showframe=False,
        projection=dict(type="miller", rotation=dict(lon=11.187179476512853)),
        center=dict(lon=11.187179476512853, lat=25.874073582328716),
        lataxis=dict(range=[-40, 90]),
    )
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), width=960, height=400)
    return fig


# -----------------------------------------------------------------------------
# Callbacks
# -----------------------------------------------------------------------------
@app.callback(Output(PRODUCT_DROPDOWN, "options"), Input(CATEGORY_DROPDOWN, "value"))
def update_product_dropdown(category):
    # print('trace: update_product_dropdown', category)
    if category is None:
        return dash.no_update
    return products[category]


@app.callback(Output(INTERVAL_TEXT, "children"), [Input(INTERVAL_SLIDER, "value")])
def update_interval_text(slider_value):
    # print('trace: update_interval_text', slider_value)
    interval = time_slider_to_interval(slider_value)
    return interval[0].strftime(r"%b %Y") + " - " + interval[1].strftime(r"%b %Y")


@app.callback(
    Output(ALERTS_FIGURE, "figure"),
    [
        Input(INTERVAL_SLIDER, "value"),
        Input(CATEGORY_DROPDOWN, "value"),
        Input(PRODUCT_DROPDOWN, "value"),
    ],
)
def update_europe_map(slider_value, category, product):
    print("trace: update_europe_map", slider_value, category, product)
    interval = time_slider_to_interval(slider_value)
    return create_europe_figure(interval, category, product)


@app.callback(
    Output(ORIGINS_FIGURE, "figure"),
    [
        Input(ALERTS_FIGURE, "selectedData"),
        Input(INTERVAL_SLIDER, "value"),
        Input(CATEGORY_DROPDOWN, "value"),
        Input(PRODUCT_DROPDOWN, "value"),
    ],
)
def update_world_map(selected_data, slider_value, category, product):
    print("trace: update_graphs", selected_data, slider_value, category, product)
    interval = time_slider_to_interval(slider_value)
    if selected_data == None:
        return create_world_map(None, interval, category, product)  # show all origins

    countries = [x["location"] for x in selected_data["points"]]
    return create_world_map(countries, interval, category, product)


if __name__ == "__main__":
    app.run_server(debug=True)
