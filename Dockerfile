FROM debian:10 AS build

RUN mkdir -p /opt/periods

RUN apt-get update && \
  apt-get install -y --no-install-recommends devscripts

RUN apt-get install -y --no-install-recommends equivs

ADD . /opt/periods/

WORKDIR /opt/periods

RUN mk-build-deps --install --tool='apt-get -o Debug::pkgProblemResolver=yes --no-install-recommends --yes' debian/control

RUN make

RUN mkdir -p build-11 && \
  mv periods.bc periods.so build-11/
RUN debuild --no-tgz-check -us -uc -b

FROM postgres:11 AS periods

COPY --from=build /opt/postgresql-11-periods_*.deb /tmp/

RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  postgresql-11

RUN dpkg -i /tmp/*.deb

ADD 90-create-extension-periods.sh /docker-entrypoint-initdb.d/
