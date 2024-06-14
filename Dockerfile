
FROM python:3.10.13

WORKDIR /calls_visualizer
COPY . .

RUN pip3 install --progress-bar off --quiet -r requirements.txt

CMD ["python3", "webapp.py"]

