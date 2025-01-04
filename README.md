# of-poller

This project is composed of two apps written in Python:
- A poller app: based on Selenium, this creates one poller for each creator, which check the online status at every time bin and saves the status on a `data/data.h5` file locally
- A visualization app: written with Streamlit, a small app which loads the `data/data.h5` file in memory and visualized each creator online stats

## Stats

The pollers constantly checks all creators specified and save all checks on the data file `data/data.h5`.

In the visualization app, time is binned in 1-minute slots and if a single check is positive the creator is considered online for that time bin. The graphs only consider the bins which have at least 1 check.
  
## Running locally

Before running the app locally, you should do the following:
- Check that there's a `data` directory in the root directory inside this repository
- If you want to run the app with Docker (suggested approach), insert your email and password in the `docker-compose.yml` file in this directory
- Instead, if you want to run the app without Docker, open a terminal and define two environment variable called `EMAIL` and `PASSWORD`

### Docker Compose

The suggested way to run this app locally is with docker compose, the repository already has a `docker-compose.yml` file and two Dockerfile: one for the poller app and the other for the Streamlit visualization app.

Open a terminal in this directory and just run
```
docker compose up -d
```
This will instantiate two docker processes
- `streamlit-app` is the Streamlit based visualization app, hosted at `http://localhost:8501`
- `poller` is the poller app, which will start gathering data about the creators you want to monitor

### Local env
   
If you don't want to run the apps on docker, first install [poetry](https://python-poetry.org/), create a Python virtual env and install all dependencies by running
```
poetry install --sync --with app --with poller
```

Then you can run the poller with
```
poetry run python src/creator_poller.py
```
and the visualization app with
```
poetry run streamlit run src/app.py
```