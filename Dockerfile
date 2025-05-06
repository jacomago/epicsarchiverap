# Stage 1: Expand WARs and prepare shared libraries
FROM eclipse-temurin:21-jdk AS expand-wars

WORKDIR /wars

# Copy all WARs into their respective subdirectories first.
# Ensure these paths (build/libs/*) are correct relative to your Docker build context.
COPY build/libs/mgmt.war /wars/mgmt/mgmt.war
COPY build/libs/etl.war /wars/etl/etl.war
COPY build/libs/engine.war /wars/engine/engine.war
COPY build/libs/retrieval.war /wars/retrieval/retrieval.war

RUN \
    set -e; \
    \
    # Process mgmt.war: extract, move its libs to a shared /wars/lib, then remove original libs directory
    echo "Processing mgmt.war..."; \
    cd mgmt && jar -xvf mgmt.war && rm mgmt.war && cd .. && \
    mkdir -p lib && \
    if [ -d mgmt/WEB-INF/lib ]; then \
        if [ "$(ls -A mgmt/WEB-INF/lib)" ]; then \
            echo "Moving libraries from mgmt/WEB-INF/lib to /wars/lib"; \
            mv mgmt/WEB-INF/lib/* lib/; \
        fi; \
        rm -rf mgmt/WEB-INF/lib; \
    fi && \
    echo "Finished processing mgmt.war." && \
    \
    # Process etl.war: extract, then remove its libs directory (to use shared ones)
    echo "Processing etl.war..."; \
    cd etl && jar -xvf etl.war && rm etl.war && \
    if [ -d WEB-INF/lib ]; then rm -rf WEB-INF/lib; fi && \
    cd .. && \
    echo "Finished processing etl.war." && \
    \
    # Process engine.war: extract, then remove its libs directory
    echo "Processing engine.war..."; \
    cd engine && jar -xvf engine.war && rm engine.war && \
    if [ -d WEB-INF/lib ]; then rm -rf WEB-INF/lib; fi && \
    cd .. && \
    echo "Finished processing engine.war." && \
    \
    # Process retrieval.war: extract, then remove its libs directory
    echo "Processing retrieval.war..."; \
    cd retrieval && jar -xvf retrieval.war && rm retrieval.war && \
    if [ -d WEB-INF/lib ]; then rm -rf WEB-INF/lib; fi && \
    cd .. && \
    echo "Finished processing retrieval.war."

# Stage 2: Base Tomcat image with common configurations
FROM tomcat:9 AS tomcat-base

ENV ARCHAPPL_APPLIANCES=/usr/local/tomcat/archappl_conf/appliances.xml \
    ARCHAPPL_POLICIES=/usr/local/tomcat/archappl_conf/policies.py \
    ARCHAPPL_SHORT_TERM_FOLDER=/usr/local/tomcat/storage/sts \
    ARCHAPPL_MEDIUM_TERM_FOLDER=/usr/local/tomcat/storage/mts \
    ARCHAPPL_LONG_TERM_FOLDER=/usr/local/tomcat/storage/lts \
    ARCHAPPL_MYIDENTITY=archappl0 \
    CATALINA_OUT=/dev/stdout

# Apply common labels here as they are intended for all deployable images from this Dockerfile.
LABEL org.opencontainers.image.source=https://github.com/archiver-appliance/epicsarchiverap
LABEL org.opencontainers.image.description="Docker image for the Archiver Appliance, supporting singletomcat or single WAR deployments."

# Stage 3: Intermediate stage for copying webapp configurations and shared libraries
FROM tomcat-base AS copy-webapp
# Ensure the source path 'docker/archappl/copy_conf/context.xml' is correct relative to the build context.
COPY docker/archappl/copy_conf/context.xml /usr/local/tomcat/conf/context.xml

COPY --from=expand-wars /wars/lib /usr/local/tomcat/lib

# Stage 4: Final image targets

# Target for a single Tomcat instance with all webapps
FROM copy-webapp AS singletomcat
COPY --from=expand-wars /wars/mgmt /usr/local/tomcat/webapps/mgmt
COPY --from=expand-wars /wars/etl /usr/local/tomcat/webapps/etl
COPY --from=expand-wars /wars/engine /usr/local/tomcat/webapps/engine
COPY --from=expand-wars /wars/retrieval /usr/local/tomcat/webapps/retrieval

# Target for mgmt webapp only
FROM copy-webapp AS mgmt
COPY --from=expand-wars /wars/mgmt /usr/local/tomcat/webapps/mgmt

# Target for etl webapp only
FROM copy-webapp AS etl
COPY --from=expand-wars /wars/etl /usr/local/tomcat/webapps/etl

# Target for engine webapp only
FROM copy-webapp AS engine
COPY --from=expand-wars /wars/engine /usr/local/tomcat/webapps/engine

# Target for retrieval webapp only
FROM copy-webapp AS retrieval
COPY --from=expand-wars /wars/retrieval /usr/local/tomcat/webapps/retrieval