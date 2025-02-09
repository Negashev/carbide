FROM python:alpine
LABEL authors="negash"
CMD ["python3", "-u", "carbide.py"]
ENV PORT=8080
WORKDIR /app
RUN pip install fastapi[standard]
RUN pip install aiohttp
RUN pip install pyyaml
RUN pip install uvicorn
RUN pip install minio
ADD carbide.py carbide.py