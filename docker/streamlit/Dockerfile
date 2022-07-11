FROM python:3.8-slim

WORKDIR /app

RUN pip3 install streamlit

COPY ./dashboard.py /app/dashboard.py
COPY ./config.toml /app/.streamlit/config.toml

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "dashboard.py"]