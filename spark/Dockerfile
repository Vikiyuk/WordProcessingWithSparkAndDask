FROM apache/spark:latest
COPY main.py /
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[*]", "/main.py"]
CMD []
