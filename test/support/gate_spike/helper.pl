#!/usr/bin/env perl
# Gate guardian (spike, revision 3 -- findings m_1788578468382_gate_spike_findings).
# The guardian stays alive and owns the control channel; the worker is a forked child that
# leads its own process group. Authority = the guardian's unreaped exact child: the worker is
# NOT reaped until the whole group is settled and the final signal has been sent, so its pid
# and group cannot be reused underneath a signal. Authority ENDS at that reap.
#
# Protocol (one line each). stdout: "IDENT guardian=<pid> worker=<pid> pgid=<pgid>" only after
# the worker reports READY (setsid succeeded); "SETUP_FAILED <why>" otherwise. stdin: "GO"
# releases the worker once; "TERM" settles the group by force. Completion is reported only
# with the group settled: "EXIT status=<n>|signal=<NAME> group_settled=1 leftovers=<k>".
# Forced settle reports "DEAD pgid=<pgid> proven=<1|0|unknown> reason=<why>". stdin EOF
# (parent gone) is a forced settle with reason=parent_gone.
use strict; use warnings; use POSIX qw(setsid WNOHANG :sys_wait_h :errno_h);
$| = 1;
pipe(my $token_r, my $token_w) or die "pipe: $!";
pipe(my $status_r, my $status_w) or die "pipe: $!";
my $worker = fork(); defined $worker or die "fork: $!";
if ($worker == 0) {                                   # ---- worker
  close $token_w; close $status_r;
  my $sid = setsid();
  if ($sid < 0) { print $status_w "SETUP_FAILED setsid\n"; exit 111 }
  print $status_w "READY\n"; close $status_w;         # ready only after a successful setsid
  my $token = <$token_r>;
  if (!defined $token) { exit 112 }                   # never released: guardian gone
  chomp $token; $token eq "GO" or exit 113;
  exec @ARGV or do { exit 114 };
}
close $token_r; close $status_w;
my $ready = <$status_r>; close $status_r;
if (!defined $ready || $ready !~ /^READY/) {
  my $why = defined $ready ? $ready : "no_ready"; chomp $why;
  waitpid($worker, 0); print "SETUP_FAILED $why\n"; exit 1;
}
my $pgid = $worker;                                   # setsid: the worker leads its own group
print "IDENT guardian=$$ worker=$worker pgid=$pgid\n";
my $released = 0;
use Config; my %signame; { my @names = split ' ', $Config{sig_name} // ''; @signame{0..$#names} = @names if @names; }
sub sig_name { my $n = shift; return $signame{$n} // "SIG$n" }
# group_state: "gone" (ESRCH), "alive", or "unknown" (EPERM/other): errno is decoded, never guessed.
sub group_state { my $g = shift; return "alive" if kill(0, -$g); return $! == ESRCH ? "gone" : "unknown" }
# members of the group other than the unreaped worker; undef when ps itself failed (never an empty set).
# ps contract, exact: exit 0 with every line a decimal pid -> the member list; exactly exit 1
# with EMPTY output -> no match (an empty group); anything else -> undef (unknown). A
# membership check that cannot run is never an empty set.
sub other_members { my $g = shift;
  my $out = `ps -o pid= -g $g 2>&1`; my $st = $? >> 8;
  return [] if $st == 1 && $out !~ /\S/;
  return undef if $st != 0;
  my @lines = grep { /\S/ } split /\n/, $out;
  return undef if grep { !/^\s*\d+\s*$/ } @lines;
  my @m = grep { $_ != $worker } map { /(\d+)/ ? $1 : () } @lines; return \@m }
sub worker_zombie { my $st = `ps -o stat= -p $worker 2>/dev/null`; return defined $st && $st =~ /^\s*Z/ }
# leader_exited: the worker has exited (a zombie, still unreaped -- identity retained) or is
# gone; a live leader is never waited on with a blocking waitpid.
sub leader_exited { return 1 if worker_zombie(); return kill(0, $worker) ? 0 : 1 }
sub settled_now { my $m = other_members($pgid); return (defined $m && !@$m && leader_exited()) }
sub settle_and_reap { my ($force, $reason) = @_;   # returns (settled, leftovers, exit_desc, proof)
  my $leftovers = 0;
  if ($force) { kill 'TERM', -$pgid }
  # bounded escalation on the whole owned group, the leader INCLUDED: TERM, then KILL once.
  for my $round (0..1) {
    for (1..60) { last if settled_now(); select(undef,undef,undef,0.05) }
    last if settled_now();
    my $m = other_members($pgid); $leftovers = (defined $m ? scalar @$m : 0) + (leader_exited() ? 0 : 1);
    kill 'KILL', -$pgid;
  }
  my $m = other_members($pgid); my $settled = (defined $m && !@$m && leader_exited()) ? 1 : 0;
  my ($desc, $proof) = ("unknown", "unknown");
  if (leader_exited()) {
    # authority ends here: the exact child is reaped only after the group is settled, signalled,
    # and the leader observed exited -- never a blocking wait on a live child.
    waitpid($worker, 0); my $raw = $?;
    $desc = WIFSIGNALED($raw) ? "signal=" . sig_name(WTERMSIG($raw)) : "status=" . WEXITSTATUS($raw);
    my $gs = group_state($pgid); $proof = $gs eq "gone" ? 1 : $gs eq "alive" ? 0 : "unknown";
  } else { $proof = 0 }
  return ($settled, $leftovers, $desc, $proof);
}
while (1) {
  my $rin = ''; vec($rin, fileno(STDIN), 1) = 1;
  my $n = select(my $rout = $rin, undef, undef, 0.1);
  if ($n > 0) {
    my $cmd = <STDIN>;
    if (!defined $cmd) { my (undef, $left, undef, $proof) = settle_and_reap(1, "parent_gone"); print "DEAD pgid=$pgid proven=$proof reason=parent_gone leftovers=$left\n"; exit 0 }
    chomp $cmd;
    if ($cmd eq "GO" && !$released) { print $token_w "GO\n"; close $token_w; $released = 1; print "RELEASED\n" }
    elsif ($cmd eq "TERM") { my (undef, $left, $desc, $proof) = settle_and_reap(1, "command"); print "DEAD pgid=$pgid proven=$proof reason=command worker=$desc leftovers=$left\n"; exit 0 }
  }
  if ($released && worker_zombie()) {                  # leader exited: complete only once the group is settled
    my ($settled, $left, $desc, $proof) = settle_and_reap(0, "leader_exit");
    print "EXIT $desc group_settled=$settled leftovers=$left proven=$proof\n"; exit 0;
  }
}
