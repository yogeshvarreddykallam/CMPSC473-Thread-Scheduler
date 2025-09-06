CC = gcc
CPPFLAGS = -I.
CFLAGS = -Wall -std=gnu17
LDFLAGS = -L.
LDLIBS = -pthread -lm
export CC CPPFLAGS CFLAGS LDFLAGS LDLIBS

SUBDIRS = libscheduler
.PHONY: default clean $(SUBDIRS)

default: tester

debug: export CFLAGS += -g -fsanitize=thread
debug: default

$(SUBDIRS):
	$(MAKE) -C $@

tester: main.c libscheduler
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ main.c -Ilibscheduler -Llibscheduler -lscheduler $(LDFLAGS) $(LDLIBS)

clean:
	rm -rf tester output
	@for d in $(SUBDIRS); do $(MAKE) -C $$d clean; done
