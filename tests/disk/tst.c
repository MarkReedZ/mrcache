#include "tst.h"

#include <zstd.h>
#include <signal.h>
#include <getopt.h>
#include <stdarg.h>
#include <netinet/in.h>
#include <assert.h>
#include <unistd.h>
#include <errno.h>

static void sig_handler(const int sig) {
  printf("Signal handled: %s.\n", strsignal(sig));
  exit(0);
}

void on_write_done( void *iov ) {
  printf("on_write_done called\n");
  free(((struct iovec*)iov)->iov_base);
  free(iov);
}
int filetest( void *user_data ) {

  printf("filetest called\n");

  char fn[128];
  sprintf( fn, "delme" );
  int fd = open(fn, O_RDWR | O_CREAT | O_TRUNC | O_APPEND, 0600);

  char *p = malloc(1024);
  strcpy(p, "Lets create a text file\n\nWith some stuff in it");

  struct iovec *iov = malloc(sizeof(struct iovec));
  iov->iov_base = p;
  iov->iov_len = 256;

  mr_writevcb( loop, fd, iov, 1, (void*)iov, on_write_done  );
  mr_flush(loop);
  p = malloc(1024);
  strcpy(p, "More text here\n\nYay");
  struct iovec *iov2 = malloc(sizeof(struct iovec));
  iov->iov_base = p;
  iov->iov_len = 256;
  mr_writevcb( loop, fd, iov2, 1, (void*)iov2, on_write_done  );
  mr_flush(loop);

    //int rc = ftruncate( fsblock_fds[fsblock_index], 0 );// TODO test return code?
}

int main (int argc, char **argv) {
  loop = mr_create_loop(sig_handler);
  mr_call_soon(loop, filetest, NULL);
  mr_run(loop);
  mr_free(loop);
}

