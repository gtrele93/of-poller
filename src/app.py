import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime
from config import BIN_LENGTH_S, data_file_path, log_file_path
from utils import get_creator_config
from dateutil import relativedelta
import logging
from logger import logger as default_logger


module_logger = logging.getLogger(f"{default_logger.name}.app")


def get_creators_list():
    return get_creator_config().creators


@st.cache_data
def compute_date_bins(start_date, end_date):
    date_bins = pd.date_range(start_date, end_date, freq=f"{BIN_LENGTH_S}s", inclusive="both")
    date_bins = date_bins.append(pd.DatetimeIndex([end_date]))
    date_bins = list(sorted(list(set(date_bins))))
    return date_bins


@st.cache_data(ttl=300, max_entries=20)
def get_creator_df(creator: str):
    df = pd.read_hdf(data_file_path, f"/{creator}/data", mode="r")
    df['datetime'] = pd.to_datetime(df['datetime'], unit='s')
    return df


@st.cache_data(ttl=300, max_entries=20)
def compute_stats_df(creator: str, start_date: datetime, end_date: datetime):
    creator_df = get_creator_df(creator)
    creator_df = creator_df[(creator_df['datetime'] >= start_date) & (creator_df['datetime'] < end_date)]
    module_logger.debug(f"Computing stats dataframe for creator {creator}\nStart date: {start_date}\nEnd date: {end_date}\nDataframe rows: {len(creator_df)}\n")
    bins = compute_date_bins(creator_df['datetime'].min(), creator_df['datetime'].max())
    module_logger.debug(f"Bins len {len(bins)}\n")
    creator_df["date_bin"] = pd.cut(creator_df["datetime"], bins=bins, right=True, include_lowest=True)
    stats_df = creator_df.groupby("date_bin", as_index=False, observed=True).agg(
        num_checks=pd.NamedAgg(column="page", aggfunc="count"),
        online_checks=pd.NamedAgg(column="online", aggfunc="sum"),
        offline_checks=pd.NamedAgg(column="online", aggfunc=lambda x: len(x) - np.sum(x)),
        is_online=pd.NamedAgg(column="online", aggfunc="any"),
    )
    stats_df['date_bin_start'] = stats_df['date_bin'].apply(lambda x: x.left)
    return stats_df


def pie_chart(stats_df: pd.DataFrame, creator: str):
    cmap = {False: "red", True:"green"}
    binned_df = stats_df.groupby("is_online", as_index=False, observed=True)['date_bin'].count()
    binned_df.rename({"date_bin": "# Bins"}, axis=1, inplace=True)
    fig = px.pie(
        binned_df,
        values="# Bins",
        names="is_online",
        title=f"{creator} overall online status",
        color="is_online",
        color_discrete_map=cmap
    )
    fig.update_traces(sort=False)
    module_logger.debug(f"Created pie chart for creator {creator}")
    return fig


def online_time_chart(stats_df: pd.DataFrame, creator: str):
    fig = px.scatter(
        data_frame=stats_df,
        x="date_bin_start",
        y="is_online",
        title=f"{creator} online status by time bin",
        custom_data = ["num_checks"],
        range_y = (False, True),
        category_orders={"is_online": (True, False)}
    )
    fig.update_yaxes(title="Online")
    fig.update_xaxes(title="Time bin")
    fig.update_layout(
        hovermode="x unified",
    )
    fig.update_traces(
        hovertemplate = 'Online: %{y}<br># total checks: %{customdata[0]}<extra></extra>',
    )
    return fig


def start_app():
    module_logger.info(f"Starting app with config:\ndata path: {data_file_path}\nlog path: {log_file_path}")
    st.title('OF Online Stats')
    st.write("This app shows the online status of creators over multiple consecutive time bins. For every time bin, the user is considered online if there's at least one online check.")
    creator = st.selectbox("Select creator", get_creators_list())
    try:
        creator_df = get_creator_df(creator)
        module_logger.debug(f"Got dataframe for creator {creator}, len {len(creator_df)}")
    except FileNotFoundError:
        st.error(f"No data found for creator {creator}")
        creator_df = pd.DataFrame([])
    if not creator_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Start date",
                creator_df['datetime'].min(),
                min_value=creator_df['datetime'].min(),
                max_value=creator_df['datetime'].max(),
                help=f"Overall first check for creator: {creator_df['datetime'].min()}"
            )
            start_date = datetime.combine(start_date, datetime.min.time())
        with col2:
            end_date = st.date_input(
                "End date",
                creator_df['datetime'].max(),
                min_value=creator_df['datetime'].min(),
                max_value=creator_df['datetime'].max(),
                help=f"Overall last check for creator: {creator_df['datetime'].max()}"
            )
            end_date = datetime.combine(end_date, datetime.min.time()) + relativedelta.relativedelta(days=1)
        creator_df = creator_df[(creator_df['datetime'] >= start_date) & (creator_df['datetime'] < end_date)]
        stats_df = compute_stats_df(creator, start_date, end_date)
        with col1:
            st.metric(label="\# Pages", value=creator_df['page'].nunique())
            st.metric(label="\# Bins", value=stats_df['date_bin'].nunique())
            st.metric(label="First check for creator", value=creator_df['datetime'].min().strftime("%Y-%m-%d %H:%M:%S"))
        with col2:
            st.metric(label="\# Total events", value=stats_df['num_checks'].sum())
            st.metric(label="Bin length", value=f"{BIN_LENGTH_S}s")
            st.metric(label="Last check for creator", value=creator_df['datetime'].max().strftime("%Y-%m-%d %H:%M:%S"))
        st.info("In the following stats, we consider only time bins that have at least 1 check. Eventual voids are time bins with no checks.")
        st.plotly_chart(pie_chart(stats_df, creator))
        st.plotly_chart(online_time_chart(stats_df, creator))
        with st.container():
            st.subheader("Last 10 checks")
            st.dataframe(creator_df.sort_values("datetime", ascending=False).head(10), hide_index=True)
            

if __name__ == "__main__":
    start_app()
