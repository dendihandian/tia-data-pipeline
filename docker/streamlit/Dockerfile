FROM python:3.8-slim

WORKDIR /app

RUN pip3 install streamlit

COPY ./dashboard.py /app/dashboard.py 

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "dashboard.py"]