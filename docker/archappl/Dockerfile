FROM tomcat:9 AS tomcat-base

ENV ARCHAPPL_APPLIANCES=/usr/local/tomcat/archappl_conf/appliances.xml
ENV ARCHAPPL_POLICIES=/usr/local/tomcat/archappl_conf/policies.py
ENV ARCHAPPL_SHORT_TERM_FOLDER=/usr/local/tomcat/storage/sts
ENV ARCHAPPL_MEDIUM_TERM_FOLDER=/usr/local/tomcat/storage/mts
ENV ARCHAPPL_LONG_TERM_FOLDER=/usr/local/tomcat/storage/lts
ENV ARCHAPPL_MYIDENTITY=archappl0
ENV CATALINA_OUT=/dev/stdout

FROM tomcat-base AS copy-webapp
COPY docker/archappl/copy_conf/context.xml /usr/local/tomcat/conf

ARG WEBAPP
COPY $WEBAPP /usr/local/tomcat/webapps
