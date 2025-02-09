FROM python:alpine
LABEL authors="negash"
CMD ["python3", "-u", "carbide.py"]
ENV PORT=8888
WORKDIR /app
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
ADD carbide.py carbide.py