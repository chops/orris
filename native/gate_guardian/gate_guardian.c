/* gate_guardian -- owned process control for orchestrator gates (R4).
 *
 * Entry point and protocol are pinned (docs/contracts/gate-guardian-protocol.org).
 *
 *   gate_guardian --stdout PATH --stderr PATH --cwd DIR [--ready-ms N] [--settle-ms N]
 *                 [--rounds N] [--timeout-ms N] -- ARGV...
 *   gate_guardian --probe PID PGID          (read-only kernel facts; never signals)
 *
 * The guardian's own stdin/stdout are the CONTROL channel (an OTP Port). The worker's
 * stdin is /dev/null and its stdout/stderr are private files the guardian creates with
 * O_EXCL and mode 0600 before the fork: ordinary gate output can never reach the control
 * channel, so a command cannot forge a READY/EXIT/DEAD record.
 *
 * Control lines (stdout): READY guardian=P worker=P pgid=G start=<identity>
 *                         RELEASED
 *                         EXIT kind=exited status=N | kind=signaled signal=N
 *                              settled=0|1 leftovers=N|unknown proof=gone|alive|unknown escaped=unknown
 *                         DEAD reason=command|parent_gone kind=... status/signal=... settled=... leftovers=... proof=...
 *                         SETUP_FAILED class=<open_stdout|open_stderr|pipe|fork|chdir|setsid|ready_timeout|identity>
 * Commands (stdin):       GO   -- release the worker once
 *                         TERM -- settle the owned group by force
 * Anything else on stdin: PROTOCOL_ERROR (ignored). EOF on stdin = parent gone = forced settle.
 *
 * Identity: macOS sysctl kinfo_proc p_starttime (sec.usec); Linux /proc/<pid>/stat starttime
 * (start=ticks:N). Leader exit is OBSERVED with waitid(WNOWAIT) and the exact child is not
 * reaped until the group is settled or the escalation rounds are exhausted -- authority is
 * the unreaped child and then the still-non-empty group (a pgid cannot be reused while a
 * member exists); it ends at the reap of an empty group. A descendant that leaves the
 * session (setsid again) is outside any group proof: escaped=unknown always.
 */
#define _GNU_SOURCE
#define _DARWIN_C_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <poll.h>
#include <time.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#ifdef __APPLE__
#include <sys/sysctl.h>
#else
#include <dirent.h>
#endif

static int out_fd = -1, err_fd = -1;
static const char *out_created = NULL, *err_created = NULL; /* output objects this guardian created */
static pid_t worker = 0, pgid = 0;
static int group_proven = 0; /* phase: 0 = only the exact child pid is authority; 1 = the owned group */

/* Compile-time test seam (never in the production build): GATE_GUARDIAN_FAULT names one
 * injected fault; GATE_GUARDIAN_FAULT_DIR receives facts the tests need (the child pid). */
#ifdef GATE_GUARDIAN_TESTING
static const char *fault(void) { return getenv("GATE_GUARDIAN_FAULT"); }
static int faulting(const char *name) { const char *f = fault(); return f && !strcmp(f, name); }
#else
static int faulting(const char *name) { (void)name; return 0; }
#endif
static int leader_exited = 0, leader_kind = 0, leader_code = 0; /* kind: 1 exited, 2 signaled */

static long now_ms(void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L; }
static volatile sig_atomic_t got_term = 0;
static void on_term(int sig) { (void)sig; got_term = 1; }
static int control_broken = 0;
/* write all of buf, retrying EINTR and short writes; -1 on any error */
static int write_all(int fd, const char *buf, size_t n) {
  long deadline = now_ms() + 2000; /* a parent that stopped draining is an error, never a hang */
  while (n > 0) {
    ssize_t w = write(fd, buf, n);
    if (w < 0) {
      if (errno == EINTR) continue;
      if ((errno == EAGAIN || errno == EWOULDBLOCK) && now_ms() < deadline) { usleep(5000); continue; }
      return -1;
    }
    buf += w; n -= (size_t)w;
  }
  return 0;
}
/* a control record; a broken control channel is remembered and treated as parent gone */
static void say(const char *line) {
  if (control_broken) return;
  size_t n = strlen(line); char buf[512]; if (n > sizeof buf - 2) n = sizeof buf - 2;
  memcpy(buf, line, n); buf[n] = '\n';
  if (write_all(1, buf, n + 1) != 0) control_broken = 1;
}

/* ---- kernel-derived facts ---- */
#ifndef __APPLE__
/* a numeric stat-prefix fact is decimal digits up to the field delimiter: no sign, no prefix */
static int digits_only(const char *t) {
  if (!t || !*t) return 0;
  for (; *t && *t != ' '; t++) if (*t < '0' || *t > '9') return 0;
  return 1;
}
/* the one rule for a /proc stat prefix: "<pid> (" with pid exactly the requested one, decimal
 * digits only; *after points at the '(' so both readers parse the rest the same way */
static int stat_prefix_is(const char *line, long want, char **after) {
  if (!digits_only(line)) return 0;
  errno = 0; char *pend = NULL; long fpid = strtol(line, &pend, 10);
  if (errno != 0 || pend == line || fpid != want || pend[0] != ' ' || pend[1] != '(') return 0;
  *after = pend + 1; return 1;
}
#endif
/* Strict kernel facts: exact sizes, complete parses, pid agreement; anything else is a
 * failure (-1), never a guess. The seam can shorten the fact to prove that path. */
static int identity_of(pid_t pid, char *buf, size_t n) {
#ifdef __APPLE__
  struct kinfo_proc kp; size_t len = sizeof kp; int mib[4] = {CTL_KERN, KERN_PROC, KERN_PROC_PID, pid};
  if (sysctl(mib, 4, &kp, &len, NULL, 0) != 0) return -1;
  if (faulting("identity_short")) len = 8;
  if (len != sizeof kp) return -1;                       /* short or oversized fact: not an identity */
  if (kp.kp_proc.p_pid != pid) return -1;                 /* the fact must be about this pid */
  if (kp.kp_proc.p_starttime.tv_usec < 0 || kp.kp_proc.p_starttime.tv_usec > 999999) return -1;
  snprintf(buf, n, "%ld.%06ld", (long)kp.kp_proc.p_starttime.tv_sec, (long)kp.kp_proc.p_starttime.tv_usec);
  return 0;
#else
  char path[64]; snprintf(path, sizeof path, "/proc/%d/stat", (int)pid);
  FILE *f = fopen(path, "r"); if (!f) return -1;
  char line[4096]; size_t got = fread(line, 1, sizeof line - 1, f); int eof = feof(f), err = ferror(f); fclose(f);
  if (err || got == 0 || !eof) return -1;                 /* read error, truncated or oversize: not a fact */
  line[got] = 0;
  if (faulting("identity_short")) line[got > 8 ? 8 : got] = 0;
  char *after = NULL; if (!stat_prefix_is(line, (long)pid, &after)) return -1; /* the fact must be about this pid */
  char *p = strrchr(after, ')'); if (!p || p[1] != ' ') return -1; p += 2;
  int field = 3; char *tok = strtok(p, " \n");
  while (tok && field < 22) { field++; tok = strtok(NULL, " \n"); }
  if (!tok || field != 22) return -1;
  if (!digits_only(tok)) return -1;                        /* a sign or blank is not a tick count */
  errno = 0; char *end = NULL; unsigned long long start = strtoull(tok, &end, 10);
  if (errno != 0 || end == tok || *end != 0) return -1;
  snprintf(buf, n, "ticks:%llu", start); return 0;
#endif
}
/* members of the group (the unreaped leader counts while it exists); -1 when unprovable */
static int members_of(pid_t g) {
  if (faulting("members_fail")) return -1;
#ifdef __APPLE__
  int mib[4] = {CTL_KERN, KERN_PROC, KERN_PROC_PGRP, g}; size_t len = 0;
  if (sysctl(mib, 4, NULL, &len, NULL, 0) != 0) return -1;
  if (len == 0) return 0;
  size_t cap = len + 4 * sizeof(struct kinfo_proc);
  struct kinfo_proc *kps = malloc(cap); if (!kps) return -1;
  if (sysctl(mib, 4, kps, &cap, NULL, 0) != 0) { free(kps); return -1; }
  if (cap % sizeof(struct kinfo_proc) != 0) { free(kps); return -1; }   /* partial record: unknown */
  int n = 0; for (size_t k = 0; k < cap / sizeof(struct kinfo_proc); k++) if (kps[k].kp_eproc.e_pgid == g) n++;
  free(kps); return n;
#else
  DIR *d = opendir("/proc"); if (!d) return -1;
  int n = 0, unknown = 0; struct dirent *e;
  for (;;) {
    errno = 0; e = readdir(d);
    if (!e) { if (errno != 0) unknown = 1; break; }           /* an enumeration error is not the end of the group */
    size_t nl = strlen(e->d_name);                                     /* only a bounded all-digit name is a process entry */
    if (nl == 0 || nl > 10 || !digits_only(e->d_name)) continue;
    errno = 0; char *nend = NULL; long npid = strtol(e->d_name, &nend, 10);
    if (errno != 0 || *nend != 0 || npid <= 0 || (long)(pid_t)npid != npid) continue;
    char path[64]; int pl = snprintf(path, sizeof path, "/proc/%s/stat", e->d_name);
    if (pl < 0 || (size_t)pl >= sizeof path) { unknown = 1; continue; }
    FILE *f = fopen(path, "r");
    if (!f) { if (errno == ENOENT || errno == ESRCH) continue; unknown = 1; continue; } /* only disappearance is absence */
    char line[4096]; size_t got = fread(line, 1, sizeof line - 1, f);
    int eof = feof(f), err = ferror(f), rerr = errno; fclose(f);
    if (err) { if (rerr == ENOENT || rerr == ESRCH) continue; unknown = 1; continue; } /* only disappearance is absence */
    if (got == 0) { continue; }                                        /* vanished between opendir and read */
    if (!eof) { unknown = 1; continue; }                               /* oversize: cannot parse */
    line[got] = 0;
    char *after = NULL;
    if (!stat_prefix_is(line, npid, &after)) { unknown = 1; continue; } /* a fact naming another pid is not this entry's fact */
    char *p = strrchr(after, ')'); if (!p || p[1] != ' ') { unknown = 1; continue; }
    int field = 3; char *tok = strtok(p + 2, " \n");
    while (tok && field < 5) { field++; tok = strtok(NULL, " \n"); }
    if (!tok || field != 5) { unknown = 1; continue; }
    if (!digits_only(tok)) { unknown = 1; continue; }
    errno = 0; char *end = NULL; long pg = strtol(tok, &end, 10);
    if (errno != 0 || end == tok || *end != 0 || (long)(pid_t)pg != pg) { unknown = 1; continue; } /* no silent truncation */
    /* pgid 0 is a valid kernel-exported group (kernel tasks); it can never match our positive g */
    if ((pid_t)pg == g) n++;
  }
  closedir(d); return unknown ? -1 : n;
#endif
}
/* group state by errno, never guessed: 0 alive, 1 gone (ESRCH), -1 unknown (EPERM/other) */
static int group_state(pid_t g) { errno = 0; if (kill(-g, 0) == 0) return 0; return errno == ESRCH ? 1 : -1; }
/* observe the leader's exit WITHOUT reaping */
static void observe_leader(void) {
  if (leader_exited) return;
  siginfo_t si; memset(&si, 0, sizeof si);
  if (waitid(P_PID, worker, &si, WEXITED | WNOHANG | WNOWAIT) == 0 && si.si_pid == worker) {
    leader_exited = 1;
    if (si.si_code == CLD_EXITED) { leader_kind = 1; leader_code = si.si_status; }
    else { leader_kind = 2; leader_code = si.si_status; }
  }
}
static const char *proof_word(int gs) { return gs == 1 ? "gone" : gs == 0 ? "alive" : "unknown"; }

/* Settle the owned group: bounded rounds, TERM then KILL; leader liveness included; reap only
 * after the leader is observed exited; proof and leftovers from the kernel after the reap. */
static void settle(int force, long settle_ms, long rounds, int *settled, char *leftovers, size_t ln, const char **proof) {
  for (long r = 0; r < rounds; r++) {
    int sig = r == 0 ? SIGTERM : SIGKILL;
    /* phase-aware authority: until setsid is proven, only the exact child pid is ours to
     * signal (it is still in the inherited group); after proof, the owned group. */
    if (force || r > 0) { if (group_proven) kill(-pgid, sig); else kill(worker, sig); }
    long deadline = now_ms() + settle_ms;
    for (;;) {
      observe_leader();
      int m = group_proven ? members_of(pgid) : (leader_exited ? 1 : 2);
      /* settled when the leader has exited and nothing else is in the group (the zombie is 1) */
      if (leader_exited && m == 1) break;
      if (leader_exited && m == 0) break;
      if (now_ms() > deadline) break;
      usleep(20000);
    }
    observe_leader();
    int m = group_proven ? members_of(pgid) : (leader_exited ? 1 : 2);
    if (leader_exited && m >= 0 && m <= 1) break;
  }
  observe_leader();
  if (!leader_exited) { /* rounds exhausted with a live leader: never block on a live child */
    *settled = 0; snprintf(leftovers, ln, "unknown"); *proof = "alive"; return;
  }
  int st; waitpid(worker, &st, 0); /* authority boundary: the exact child is reaped here */
  if (!group_proven) { /* no group was ever ours: settled means the exact child is gone */
    *settled = 1; snprintf(leftovers, ln, "0"); *proof = "gone"; return;
  }
  int m = members_of(pgid); int gs = group_state(pgid);
  *settled = (gs == 1 && m == 0) ? 1 : 0;
  if (m < 0) snprintf(leftovers, ln, "unknown"); else snprintf(leftovers, ln, "%d", m);
  *proof = proof_word(gs);
}
static void report(const char *head, int settled, const char *leftovers, const char *proof) {
  char buf[256];
  if (leader_kind == 1) snprintf(buf, sizeof buf, "%s kind=exited status=%d settled=%d leftovers=%s proof=%s escaped=unknown", head, leader_code, settled, leftovers, proof);
  else if (leader_kind == 2) snprintf(buf, sizeof buf, "%s kind=signaled signal=%d settled=%d leftovers=%s proof=%s escaped=unknown", head, leader_code, settled, leftovers, proof);
  else snprintf(buf, sizeof buf, "%s kind=unknown settled=%d leftovers=%s proof=%s escaped=unknown", head, settled, leftovers, proof);
  say(buf);
}
/* Setup rejection: bounded cleanup FIRST (the prepared child under phase-aware authority, then
 * the output objects this guardian itself created), and only then the record, carrying the
 * cleanup outcome truthfully. */
static void setup_failed(const char *cls, long settle_ms) {
  char buf[200]; const char *cleanup = "none";
  int s = 1; char l[32] = "0"; const char *p = "gone";
  if (worker > 0) settle(1, settle_ms, 2, &s, l, sizeof l, &p);
  if (out_created || err_created) {
    int removed = 1;
    if (out_created && unlink(out_created) != 0 && errno != ENOENT) removed = 0;
    if (err_created && unlink(err_created) != 0 && errno != ENOENT) removed = 0;
    cleanup = removed ? "removed" : "left";
  }
  if (worker > 0) snprintf(buf, sizeof buf, "SETUP_FAILED class=%s settled=%d leftovers=%s proof=%s cleanup=%s", cls, s, l, p, cleanup);
  else snprintf(buf, sizeof buf, "SETUP_FAILED class=%s cleanup=%s", cls, cleanup);
  say(buf); exit(1);
}

/* a decimal option: digits only, fully consumed, within [lo, hi]; -1 otherwise */
static long bounded(const char *text, long lo, long hi) {
  if (!text || !*text) return -1;
  for (const char *c = text; *c; c++) if (*c < '0' || *c > '9') return -1;
  if (strlen(text) > 9) return -1;
  long v = strtol(text, NULL, 10); return (v < lo || v > hi) ? -1 : v;
}

/* monotonic milliseconds since an arbitrary origin: the backstop's clock */
static long long mono_ms(void) {
  struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
  return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* signal 0 as a fact: 0 alive, ESRCH gone, anything else (EPERM included) unknown */
static const char *signal_zero_fact(pid_t target) {
  errno = 0;
  if (kill(target, 0) == 0) return "alive";
  return errno == ESRCH ? "gone" : "unknown";
}

/* an exact decimal token: EVERY character a digit, 1..10 of them (digits_only stops at a
 * blank by design for stat prefixes; an argument admits no blank, sign or prefix at all) */
static int all_decimal(const char *t) {
  size_t n = 0;
  if (!t) return 0;
  for (; *t; t++, n++) if (*t < '0' || *t > '9') return 0;
  return n >= 1 && n <= 10;
}

/* a positive pid_t from an exact decimal token, or 0 */
static pid_t parse_pid(const char *t) {
  if (!all_decimal(t)) return 0;
  errno = 0; char *end = NULL; long v = strtol(t, &end, 10);
  if (errno != 0 || *end != 0 || v <= 0 || (long)(pid_t)v != v) return 0;
  return (pid_t)v;
}

/* --probe PID PGID: READ-ONLY. One bounded record, exit 0; never TERM/KILL, no authority.
 * leader/start from kill(pid,0) + the identity reader; group independently from kill(-pgid,0);
 * members from the enumeration reader. Every unprovable fact is "unknown" and propagates. */
static int probe(int argc, char **argv) {
  if (argc != 4) { say("SETUP_FAILED class=usage"); return 2; }
  pid_t pid = parse_pid(argv[2]), pg = parse_pid(argv[3]);
  if (pid == 0 || pg == 0) { say("SETUP_FAILED class=usage"); return 2; }
  const char *leader = signal_zero_fact(pid);
  char ident[64] = "-";
  if (!strcmp(leader, "alive") && identity_of(pid, ident, sizeof ident) != 0) { leader = "unknown"; strcpy(ident, "-"); }
  const char *group = signal_zero_fact(-pg);
  if (faulting("probe_group_eperm")) group = "unknown";
  int n = members_of(pg);
  char members[24]; if (n < 0) strcpy(members, "unknown"); else snprintf(members, sizeof members, "%d", n);
  char line[192]; snprintf(line, sizeof line, "PROBE leader=%s start=%s group=%s members=%s", leader, ident, group, members);
  say(line);
  return 0;
}

int main(int argc, char **argv) {
  const char *out_path = NULL, *err_path = NULL, *cwd = NULL; long ready_ms = 5000, settle_ms = 3000; long rounds = 2; int i = 1;
  long timeout_ms = 0; long long started_mono = mono_ms(); /* the backstop clock starts with the guardian, never on GO */
  signal(SIGPIPE, SIG_IGN);
  { int fl = fcntl(1, F_GETFL); if (fl >= 0) fcntl(1, F_SETFL, fl | O_NONBLOCK); } /* control writes are bounded, the probe's too */
  if (argc >= 2 && !strcmp(argv[1], "--probe")) return probe(argc, argv);
  struct sigaction sa; memset(&sa, 0, sizeof sa); sa.sa_handler = on_term; sigaction(SIGTERM, &sa, NULL); sigaction(SIGINT, &sa, NULL); sigaction(SIGHUP, &sa, NULL);
  for (; i < argc; i++) {
    if (!strcmp(argv[i], "--")) { i++; break; }
    if (!strcmp(argv[i], "--stdout") && i + 1 < argc) out_path = argv[++i];
    else if (!strcmp(argv[i], "--stderr") && i + 1 < argc) err_path = argv[++i];
    else if (!strcmp(argv[i], "--cwd") && i + 1 < argc) cwd = argv[++i];
    /* finite maxima and cleanup-capable minima; anything malformed fails closed before any side effect */
    else if (!strcmp(argv[i], "--ready-ms") && i + 1 < argc) ready_ms = bounded(argv[++i], 100, 600000);
    else if (!strcmp(argv[i], "--settle-ms") && i + 1 < argc) settle_ms = bounded(argv[++i], 100, 600000);
    /* at least two rounds so the escalation always reaches KILL */
    else if (!strcmp(argv[i], "--rounds") && i + 1 < argc) rounds = bounded(argv[++i], 2, 10);
    /* the cleanup backstop: bounded like every option (its own range: up to 24 h, since it
     * covers a gate's whole remaining deadline plus grace), measured from the guardian's own start */
    else if (!strcmp(argv[i], "--timeout-ms") && i + 1 < argc) timeout_ms = bounded(argv[++i], 100, 86400000);
    else { say("SETUP_FAILED class=usage"); return 2; }
  }
  if (i >= argc || !out_path || !err_path || !cwd || ready_ms < 0 || settle_ms < 0 || rounds < 0 || timeout_ms < 0) { say("SETUP_FAILED class=usage"); return 2; }
  char **cmd = argv + i;
  out_fd = open(out_path, O_WRONLY | O_CREAT | O_EXCL, 0600); if (out_fd < 0) setup_failed("open_stdout", settle_ms); out_created = out_path;
  err_fd = open(err_path, O_WRONLY | O_CREAT | O_EXCL, 0600); if (err_fd < 0) setup_failed("open_stderr", settle_ms); err_created = err_path;
  int token[2], status[2];
  if (pipe(token) != 0 || pipe(status) != 0) setup_failed("pipe", settle_ms);
  fcntl(status[1], F_SETFD, FD_CLOEXEC); fcntl(status[0], F_SETFD, FD_CLOEXEC); fcntl(token[1], F_SETFD, FD_CLOEXEC); fcntl(token[0], F_SETFD, FD_CLOEXEC);
  worker = fork(); if (worker < 0) setup_failed("fork", settle_ms);
  if (worker == 0) { /* ---- worker ---- */
    close(token[1]); close(status[0]);
    signal(SIGPIPE, SIG_DFL); signal(SIGTERM, SIG_DFL); signal(SIGINT, SIG_DFL); signal(SIGHUP, SIG_DFL);
#ifdef GATE_GUARDIAN_TESTING
    if (faulting("setsid_delay")) { /* record the pre-setsid child pid for the test, then stall */
      const char *dir = getenv("GATE_GUARDIAN_FAULT_DIR"); if (dir) { char fp[512]; snprintf(fp, sizeof fp, "%s/fault-child-pid", dir); FILE *pf = fopen(fp, "w"); if (pf) { fprintf(pf, "%d\n", (int)getpid()); fclose(pf); } }
      sleep(5);
    }
#endif
    if (setsid() < 0) { (void)!write(status[1], "F setsid\n", 9); _exit(111); }
    if (chdir(cwd) != 0) { (void)!write(status[1], "F chdir\n", 8); _exit(111); }
    int devnull = open("/dev/null", O_RDONLY); if (devnull < 0 || dup2(devnull, 0) < 0 || dup2(out_fd, 1) < 0 || dup2(err_fd, 2) < 0) { (void)!write(status[1], "F redirect\n", 11); _exit(111); }
    close(devnull); close(out_fd); close(err_fd);
    (void)!write(status[1], "R\n", 2);
    char t[4]; ssize_t n = read(token[0], t, 3); /* blocked until released; EOF = guardian gone */
    if (n != 3 || memcmp(t, "GO\n", 3) != 0) _exit(112);
    execvp(cmd[0], cmd); _exit(127);
  }
  close(token[0]); close(status[1]); close(out_fd); close(err_fd);
  pgid = worker; /* setsid semantics; cross-checked by the caller's tests against the kernel */
  /* bounded readiness: the worker reports R only after setsid+chdir+redirection succeeded */
  struct pollfd rp = { status[0], POLLIN, 0 }; char rbuf[32]; ssize_t rn = 0;
  if (poll(&rp, 1, (int)ready_ms) <= 0) setup_failed("ready_timeout", settle_ms);
  rn = read(status[0], rbuf, sizeof rbuf - 1); close(status[0]);
  if (rn <= 0 || rbuf[0] != 'R') { if (rn > 0 && rbuf[0] == 'F') { rbuf[rn] = 0; char *c = rbuf + 2; c[strcspn(c, "\n")] = 0; setup_failed(c, settle_ms); } setup_failed("ready", settle_ms); }
  group_proven = 1; /* setsid succeeded in the child: from here the owned group is the authority */
  char ident[64]; if (identity_of(worker, ident, sizeof ident) != 0) setup_failed("identity", settle_ms);
  out_created = err_created = NULL; /* from READY on, the output objects belong to the gate run */
  char line[256]; snprintf(line, sizeof line, "READY guardian=%d worker=%d pgid=%d start=%s", (int)getpid(), (int)worker, (int)pgid, ident); say(line);
  int released = 0;
  /* incremental, bounded, exact-line framing: a command acts only when its newline arrives;
   * several lines in one read are handled in order; an overlong line is one PROTOCOL_ERROR
   * and is discarded up to its newline. */
  char acc[128]; size_t acc_len = 0; int overlong = 0;
  for (;;) {
    if (got_term) { int s; char l[32]; const char *p; settle(1, settle_ms, rounds, &s, l, sizeof l, &p); report("DEAD reason=guardian_signaled", s, l, p); return s ? 0 : 3; }
    if (control_broken) { int s; char l[32]; const char *p; settle(1, settle_ms, rounds, &s, l, sizeof l, &p); report("DEAD reason=parent_gone", s, l, p); return s ? 0 : 3; }
    /* backstop: elapsed since the guardian started (not since GO); settles released or blocked alike */
    if (timeout_ms > 0 && mono_ms() - started_mono >= timeout_ms) {
      int s; char l[32]; const char *p; settle(1, settle_ms, rounds, &s, l, sizeof l, &p); report("DEAD reason=deadline", s, l, p); return s ? 0 : 3;
    }
    struct pollfd cp = { 0, POLLIN, 0 }; int pr = poll(&cp, 1, 50);
    if (pr < 0 && errno != EINTR) control_broken = 1;
    if (pr > 0) {
      char chunk[64]; ssize_t n = read(0, chunk, sizeof chunk);
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) { /* parent gone */
        int s; char l[32]; const char *p; settle(1, settle_ms, rounds, &s, l, sizeof l, &p);
        report("DEAD reason=parent_gone", s, l, p); return s ? 0 : 3;
      }
      for (ssize_t k = 0; k < n; k++) {
        char c = chunk[k];
        if (c != '\n') { if (acc_len < sizeof acc - 1) acc[acc_len++] = c; else overlong = 1; continue; }
        acc[acc_len] = 0;
        if (overlong) { say("PROTOCOL_ERROR"); }
        else if (acc_len == 2 && memcmp(acc, "GO", 2) == 0) {   /* exact bytes and exact length: an embedded NUL is not a command */
          /* the irreversible boundary: a GO that arrives after the backstop elapsed (even one read
           * in the same poll window as the expiry) is never honoured; the blocked worker is settled */
          int expired = timeout_ms > 0 && mono_ms() - started_mono >= timeout_ms;
          if (faulting("go_after_deadline")) expired = 1;
          if (expired) { int s; char l[32]; const char *p; settle(1, settle_ms, rounds, &s, l, sizeof l, &p); report("DEAD reason=deadline", s, l, p); return s ? 0 : 3; }
          if (!released) {
            if (write_all(token[1], "GO\n", 3) == 0) { close(token[1]); released = 1; say("RELEASED"); }
            else { int s; char l[32]; const char *p; settle(1, settle_ms, rounds, &s, l, sizeof l, &p); report("DEAD reason=release_failed", s, l, p); return 3; }
          }
        }
        else if (acc_len == 4 && memcmp(acc, "TERM", 4) == 0) { int s; char l[32]; const char *p; settle(1, settle_ms, rounds, &s, l, sizeof l, &p); report("DEAD reason=command", s, l, p); return s ? 0 : 3; }
        else say("PROTOCOL_ERROR");
        acc_len = 0; overlong = 0;
      }
    }
    observe_leader();
    if (leader_exited) { int s; char l[32]; const char *p; settle(0, settle_ms, rounds, &s, l, sizeof l, &p); report("EXIT", s, l, p); return s ? 0 : 3; }
  }
}
