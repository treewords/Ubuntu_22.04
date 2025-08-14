FROM ubuntu:24.04
LABEL logging.driver="json-file"
LABEL logging.options.max-size="10m"
LABEL logging.options.max-file="3"
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
RUN useradd -m -u 1000 app && mkdir -p /app && chown -R app:app /app
USER app
WORKDIR /app
CMD ["bash"]
