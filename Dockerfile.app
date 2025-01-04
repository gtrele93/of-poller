FROM python:3.12-slim

RUN apt update
RUN apt install pipx -y
RUN pipx ensurepath
RUN pipx install poetry
ENV PATH="${PATH}:/root/.local/bin"
RUN mkdir -p $HOME/.streamlit
RUN echo '[general]\nemail=""' > $HOME/.streamlit/credentials.toml

WORKDIR /app
RUN mkdir ./src
RUN mkdir ./logs
RUN mkdir ./data
COPY ./src ./src
COPY pyproject.toml ./
RUN poetry install --sync --with app

EXPOSE 8501
CMD ["poetry", "run", "streamlit", "run", "src/app.py"]