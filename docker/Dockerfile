# One image for every node: Python 3.11 + OpenMPI + mpi4py + PySpark 3.5 + Java 17 + pandas/pyarrow.
FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk \
    OMPI_ALLOW_RUN_AS_ROOT=1 OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1 \
    OMPI_MCA_btl_vader_single_copy_mechanism=none \
    OMPI_MCA_btl_tcp_if_include=eth0 OMPI_MCA_oob_tcp_if_include=eth0 \
    PYTHONUNBUFFERED=1 PYSPARK_PYTHON=python3

RUN apt-get update && apt-get install -y --no-install-recommends \
        openmpi-bin libopenmpi-dev openssh-server openssh-client openjdk-17-jre-headless \
        build-essential procps iputils-ping curl \
    && ln -sf /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17-openjdk \
    && rm -rf /var/lib/apt/lists/*

# Passwordless SSH between containers (MPI launches remote ranks over ssh).
# The key is generated at build time and shared by all nodes - fine for a lab cluster, not for production.
RUN mkdir -p /run/sshd /root/.ssh && ssh-keygen -t ed25519 -N "" -f /root/.ssh/id_ed25519 \
    && cat /root/.ssh/id_ed25519.pub >> /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys \
    && printf "Host *\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  LogLevel ERROR\n" > /root/.ssh/config \
    && sed -i 's/#PermitRootLogin.*/PermitRootLogin prohibit-password/' /etc/ssh/sshd_config

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "pyspark==3.5.3" \
    && grep -v -E "^(pyspark|streamlit)" /tmp/requirements.txt > /tmp/req.txt \
    && pip install --no-cache-dir -r /tmp/req.txt streamlit

COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
WORKDIR /app
ENTRYPOINT ["/entrypoint.sh"]
