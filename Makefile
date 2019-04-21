CFLAGS+=-Wall -Wextra -g -std=c99 `mysql_config --cflags` `pkg-config glib-2.0 gthread-2.0 --cflags` -Wno-implicit-fallthrough
LDLIBS+=`mysql_config --libs_r | sed -e s/zlib/z/` `pkg-config glib-2.0 gthread-2.0 --libs` -lcrypto

all: pmysql

install: pmysql
	install -D pmysql ${DESTDIR}/usr/bin/pmysql

pmysql: pmysql.o threadpool.o

clean:
	rm -f pmysql *.o
