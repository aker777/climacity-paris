# ClimaCity Paris : image JupyterLab + PySpark 3.5 + Delta Lake 3.2
#
# L'image de base de Jupyter fournit déjà Java, Spark 3.5.3 et JupyterLab.
# On y ajoute les JARs Delta (pour travailler hors ligne) et les paquets Python du projet.
FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

USER root

# Delta Lake 3.2.x est la version compatible avec Spark 3.5.x
ARG DELTA_VERSION=3.2.0
RUN cd "${SPARK_HOME}/jars" \
 && curl -fsSLO "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"

# Configuration Spark commune à toutes les sessions (extensions Delta, partitions...)
COPY conf/spark-defaults.conf "${SPARK_HOME}/conf/spark-defaults.conf"

USER ${NB_UID}

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /home/jovyan
