MYSQL_CONFIG?=mysql_config
MYSQL_LIBDIR:=$(shell $(MYSQL_CONFIG) --variable=pkglibdir)

CFLAGS+=-Wall -Wextra -g -std=c11 `$(MYSQL_CONFIG) --cflags` `pkg-config glib-2.0 gthread-2.0 --cflags`
LDLIBS+=`$(MYSQL_CONFIG) --libs_r | sed -e s/zlib/z/` `pkg-config glib-2.0 gthread-2.0 --libs`
LDFLAGS+=-Wl,-rpath,$(MYSQL_LIBDIR)

GIT_VERSION:=$(shell git describe --always --dirty --tags 2>/dev/null || echo unknown)
GIT_DATE:=$(shell git log -1 --format=%ci 2>/dev/null || echo unknown)

all: pmysql

install: pmysql
	install -D pmysql ${DESTDIR}/usr/bin/pmysql

pmysql: pmysql.o threadpool.o
pmysql.o: version.h

# Regenerated on every build, but only rewritten when the git state actually
# changed, so a rebuild is triggered by new commits rather than by every make
version.h: FORCE
	@printf '#define PMYSQL_GIT_VERSION "%s"\n#define PMYSQL_GIT_DATE "%s"\n' \
		'$(GIT_VERSION)' '$(GIT_DATE)' > $@.tmp
	@cmp -s $@.tmp $@ || mv $@.tmp $@
	@rm -f $@.tmp

FORCE:
.PHONY: all install clean format FORCE

clean:
	rm -f pmysql *.o version.h

format:
	clang-format -i *.[ch]
