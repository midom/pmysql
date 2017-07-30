CFLAGS+=-Wall -Wextra -Werror -g -O3 -std=c99 `mysql_config --cflags` `pkg-config glib-2.0 gthread-2.0 --cflags` -Wno-deprecated-declarations
LDFLAGS+=`mysql_config --libs_r` `pkg-config glib-2.0 gthread-2.0 --libs`

all: pmysql

install: pmysql
	install -D pmysql ${DESTDIR}/usr/bin/pmysql

clean:
	rm -f pmysql
