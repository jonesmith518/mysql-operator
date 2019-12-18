FROM mysql/mysql-server:8.0.16

COPY bin/linux_amd64/mysql-agent /

USER mysql

ENTRYPOINT ["/mysql-agent"]
