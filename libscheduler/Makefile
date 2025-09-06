.PHONY: default clean

default: libscheduler.a

libscheduler.a: scheduler.o interface.o init.o
	$(AR) rcs $@ $^

%.o: %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ -c $< $(LDFLAGS) $(LDLIBS)

clean:
	rm -f libscheduler.a *.o
