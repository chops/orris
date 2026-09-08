/* Linux fact-reader probe: compiles the guardian's Linux (/proc) branch on any host by
 * taking __APPLE__ away and substituting fake proc readers. It measures the parser and
 * its error discipline, NOT Linux kernel execution. Imported from Codex's review probe. */
#define _GNU_SOURCE
#define _DARWIN_C_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>

static int mode, dir_count;
static FILE *fake_fopen(const char *, const char *);
static DIR *fake_opendir(const char *);
static struct dirent *fake_readdir(DIR *);
static int fake_closedir(DIR *);
#undef __APPLE__
#define main guardian_main
#define fopen fake_fopen
#define opendir fake_opendir
#define readdir fake_readdir
#define closedir fake_closedir
#include "gate_guardian.c"
#undef main
#undef fopen
#undef opendir
#undef readdir
#undef closedir

/* modes: 1 fread error (EBADF); 2 wrong pid + negative ticks; 3 readdir EIO after one entry;
 * 4 wrong pid only; 5 negative ticks only; 6 good fact; 7 fopen ENOENT (vanished); 8 signed pgid;
 * 9 pgid overflowing pid_t; 10 non-numeric directory entry; 11 overlong numeric directory entry;
 * 12 member fact naming another pid; 13 malformed stat prefix; 14 member with pgid 0 */
static DIR *fake_opendir(const char *p) { (void)p; dir_count = 0; return (DIR *)1; }
static struct dirent *fake_readdir(DIR *d) {
  (void)d;
  static struct dirent e;
  if (dir_count++) { errno = mode == 3 ? EIO : 0; return NULL; }
  memset(&e, 0, sizeof e);
  strcpy(e.d_name, mode == 10 ? "4242self" : mode == 11 ? "99999999999999999999" : "4242"); return &e;
}
static int fake_closedir(DIR *d) { (void)d; return 0; }
static FILE *fake_fopen(const char *p, const char *m) {
  (void)p; (void)m;
  if (mode == 7) { errno = ENOENT; return NULL; }
  FILE *f = tmpfile();
  if (mode == 1) { close(fileno(f)); return f; }
  if (mode == 13) fprintf(f, "4242(worker) S 1 4242");
  else fprintf(f, "%s (worker) S 1 %s", (mode == 2 || mode == 4 || mode == 12) ? "999999" : "4242",
               mode == 8 ? "+4242" : mode == 9 ? "99999999999" : mode == 14 ? "0" : "4242");
  for (int field = 6; field < 22; field++) fprintf(f, " 0");
  fprintf(f, " %s\n", (mode == 2 || mode == 5) ? "-1" : "123");
  rewind(f); return f;
}
int main(void) {
  char fact[80];
  mode = 1; printf("members_fread_error=%d\n", members_of(4242));
  mode = 3; printf("members_readdir_error=%d\n", members_of(4242));
  mode = 7; printf("members_vanished=%d\n", members_of(4242));
  mode = 8; printf("members_signed_pgid=%d\n", members_of(4242));
  mode = 6; printf("members_good=%d\n", members_of(4242));
  mode = 9; printf("members_pgid_overflow=%d\n", members_of(4242));
  mode = 10; printf("members_non_numeric_entry=%d\n", members_of(4242));
  mode = 11; printf("members_overlong_entry=%d\n", members_of(4242));
  mode = 12; printf("members_wrong_pid=%d\n", members_of(4242));
  mode = 13; printf("members_malformed_prefix=%d\n", members_of(4242));
  mode = 14; printf("members_pgid_zero=%d\n", members_of(4242));
  mode = 13; fact[0] = 0; printf("identity_malformed_prefix=%d\n", identity_of(4242, fact, sizeof fact));
  mode = 2; fact[0] = 0; printf("identity_wrong_pid_negative_ticks=%d\n", identity_of(4242, fact, sizeof fact));
  mode = 4; fact[0] = 0; printf("identity_wrong_pid=%d\n", identity_of(4242, fact, sizeof fact));
  mode = 5; fact[0] = 0; printf("identity_negative_ticks=%d\n", identity_of(4242, fact, sizeof fact));
  mode = 6; fact[0] = 0; printf("identity_good=%d value=%s\n", identity_of(4242, fact, sizeof fact), fact);
  return 0;
}
