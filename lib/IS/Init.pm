package IS::Init;
use strict;
use IO::Socket;
use IO::Select;
use POSIX qw(:signal_h :errno_h :sys_wait_h);

my $debug=0;

BEGIN {
	use vars qw ($VERSION);
	$VERSION     = 0.92;
}

=head1 NAME

IS::Init - Clusterwide "init", spawn cluster applications

=head1 SYNOPSIS

  use IS::Init;

  my $init = new IS::Init;

  # spawn all apps for resource group "foo", runlevel "run"
  $init->tell("foo","run");

  # spawn all apps for resource group "foo", runlevel "runmore"
  # (this stops everything started by runlevel "run")
  $init->tell("foo","runmore");

=head1 DESCRIPTION

This module provides basic "init" functionality, giving you a single
inittab-like file to manage initialization and daemon startup across a
cluster or collection of machines.

=head1 USAGE

This module's package includes a script 'isinit', which is intended to
be a bolt-in cluster init tool, calling IS::Init.  The script is
called like 'init', with the addition of a new "resource group"
argument.

This module is intended to be used like 'init' and 'telinit' -- the
first execution runs as a daemon, spawning and managing processes.
Later executions talk to the first, requesting it to switch to
different runlevels.

The module references a configuration file, /etc/isinittab by default,
which is identical in format to /etc/inittab, with a new "resource
group" column added.  This file must be replicated across all hosts in
the cluster by some means.

A "resource group" is a collection of applications and physical
resources which together make up a coherent function.  For example,
sendmail, /etc/sendmail.cf, and the /var/spool/mqueue directory might
make up a resource group. From /etc/isinittab you could spawn the
scripts which update sendmail.cf, mount mqueue, and then start
sendmail itself.

=head1 PUBLIC METHODS

=head2 new

=cut

sub new
{
  my $class=shift;
  $class = (ref $class || $class);
  my $self={};
  bless $self, $class;

=pod

The constructor accepts an optional hash containing the paths to the
configuration file and to the socket, like this:

  my $init = new IS::Init (
      'config' => '/etc/isinittab',
      'socket' => '/var/run/is/init.s'
			  );

=cut

  my %parms=@_;
  
  $self->{'config'} = $parms{'config'} || "/etc/isinittab";
  $self->{'socket'} = $parms{'socket'} || "/var/run/is/init.s";

  ($self->{'group'}, $self->{'level'}) = ("NULL", "NULL");

=pod

The first time this method is executed on a machine, it opens a UNIX
domain socket, /var/run/is/init.s by default.  Subsequent executions
communicate with the first via this socket.  

=cut

  $self->_open_socket() || $self->_start_daemon() || die $!;

  return $self;
}

=head2 tell($resource_group,$runlevel)

This method talks to a running IS::Init daemon, telling it to switch
the given resource group to the given runlevel.  

All processes listed in the configuration file (normally
/etc/isinittab) which belong to the new runlevel will be started if
they aren't already running.

All processes in the resource group which do not belong to the new
runlevel will be killed.

Other resource groups will not be affected.

=cut

sub tell
{
  my ($self,$group,$runlevel)=@_;
  my $socket = $self->_open_socket() || die $!;
  print $socket "$group $runlevel";
  close($socket);
  1;
}

sub stopall
{
  my ($self)=@_;
  my $socket = $self->_open_socket() || die $!;
  print $socket "stopall";
  close($socket);
  1;
}


=head1 PRIVATE METHODS

These methods and functions are considered private and are intended for
internal use by this module. They are B<not> considered part of the public
interface and are described here for documentation purposes only.


=cut

=head2 _open_socket

=cut

sub _open_socket
{
  my $self=shift;
  my $client = new IO::Socket::UNIX (
      Peer => $self->{'socket'},
      Type => SOCK_STREAM
				    );
  return $client;
}

sub _start_daemon
{
  my $self=shift;

  my $child;
  unless ($child = fork())
  {
    while(1)
    {
      unlink $self->{'socket'};
      my $server = new IO::Socket::UNIX (
	  Local => $self->{'socket'},
	  Type => SOCK_STREAM,
	  Listen => SOMAXCONN
					) || die $!;
      while(my $client = $server->accept())
      {
	$SIG{CHLD} = 'IGNORE';
	# warn "reading\n";
	my $data=<$client>;
	$data="" unless $data;
	# warn "$data\n" if $data;
	# warn "done reading\n";
	$self->_stopall() if $data =~ /^stopall/;
	my ($group,$level) = split(' ',$data);
	$self->_spawn($group,$level);
	$self->_sigchld();
      }
      # warn "restarting socket";
    }
  }

  # warn "IS::Init daemon started as PID $child\n"; 

  sleep 1;
  return $child;
}

sub _stopall
{
  my $self=shift;
  for my $tag (keys %{$self->{'pid'}})
  {
    $self->_kill($tag);
  }
  exit(0);
}

sub _spawn
{
  my ($self,$newgroup,$newlevel)=@_;
  ($newgroup,$newlevel)=($self->{'group'},$self->{'level'})
    unless $newgroup && ($newlevel || (defined($newlevel) && $newlevel == 0));
  ($self->{'group'},$self->{'level'}) = ($newgroup,$newlevel);
  my @activetags;
  open(INITTAB,"<$self->{'config'}") || die $!;
  while(<INITTAB>)
  {
    next if /^#/;
    chomp;
    my ($group,$tag,$level,$mode,$cmd) = split(':',$_,5);
    $self->{'mode'}{$tag} = $mode;
    next if $mode eq "off";
    push @activetags, $tag;
    next unless $group eq $newgroup;

    if(
	($level =~ /,/ && $level =~ /(^|,)$newlevel(,|$)/) ||
	($level !~ /,/ && $level =~ /$newlevel/) 
      )
    {
      # start processes in new runlevel
      # warn "$level contains $newlevel";

      # bail if already started
      next if $self->{'pid'}{$tag};

      if ($mode eq "wait")
      {
	# set a placeholder to keep us from running $tag again
	$self->{'pid'}{$tag} = "wait";
	# warn "system($cmd)";
	system($cmd);
	next;
      }

      if ($mode eq "respawn")
      {
	$self->{'time'}{$tag}=time() unless $self->{'time'}{$tag};
	$self->{'counter'}{$tag}=0 unless $self->{'counter'}{$tag};
	if($self->{'time'}{$tag} < time() - 10)
	{
	  $self->{'time'}{$tag}=time(); 
	  $self->{'counter'}{$tag}=0;
	}
	next unless time() >= $self->{'time'}{$tag};
	if ($self->{'counter'}{$tag} >= 5)
	{
	  warn "$0: $tag respawning too rapidly -- sleeping 60 seconds\n";
	  $self->{'time'}{$tag}=time() + 60; 
	  $self->{'counter'}{$tag}=0;
	  next;
	}
	$self->{'counter'}{$tag}++;
      }

      if (my $pid = fork())
      {
	# parent
	# warn "$pid forked\n";
	# build index so we can find pid from tag
	$self->{'pid'}{$tag} = $pid;
	# build reverse index so we can find tag from pid
	$self->{'tag'}{$self->{'pid'}{$tag}}=$tag;
	next;
      }

      # child
      # sleep 1;
      # warn "exec $cmd";
      exec($cmd);
    }
    else
    {
      # stop processes in old runlevel 
      next unless $self->{'pid'}{$tag};
      $self->_kill($tag);
    }

  }
  close(INITTAB);

  # stop processes which are no longer in inittab
  for my $tag (keys %{$self->{'pid'}})
  {
    next if grep /^$tag$/, @activetags;
    $self->_kill($tag);
  }

}

sub _kill
{
  my $self = shift;
  my $tag = shift;
  if ($self->{'pid'}{$tag} eq "wait")
  {
    delete $self->{'pid'}{$tag};
    return;
  }
  return unless $self->{'pid'}{$tag};
  # warn "killing $self->{'pid'}{$tag}";
  kill(15,$self->{'pid'}{$tag});
  for(my $i=1;$i <= 16; $i*=2)
  {
    last unless $self->{'pid'}{$tag};
    last unless kill(0,$self->{'pid'}{$tag});
    sleep $i;
  }
  return unless $self->{'pid'}{$tag};
  kill(9,$self->{'pid'}{$tag});
  delete $self->{'pid'}{$tag};
}

sub _sigchld
{
  my $self=shift;
  my $pid = waitpid(-1, &WNOHANG);
  if ($pid == -1)
  {
    # nothing exited -- ignore
    $SIG{CHLD} = sub {$self->_sigchld()};
    return;
  }
  unless (kill(0,$pid) == 0)
  {
    # still running -- false alarm
    $SIG{CHLD} = sub {$self->_sigchld()};
    return;
  }
  # $pid exited
  # warn "$pid exited\n";
  my $tag = $self->{'tag'}{$pid};
  delete $self->{'pid'}{$tag} if $self->{'mode'}{$tag} eq 'respawn';
  $self->_spawn();
  $SIG{CHLD} = sub {$self->_sigchld()};
}

sub debug
{
  warn @_ if $debug;
}

=head1 BUGS

=head1 AUTHOR

	Steve Traugott
	CPAN ID: STEVEGT
	stevegt@TerraLuna.Org
	http://www.stevegt.com

=head1 COPYRIGHT

Copyright (c) 2001 Steve Traugott. All rights reserved.
This program is free software; you can redistribute
it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the
LICENSE file included with this module.

=head1 SEE ALSO

perl(1).

=cut

1; #this line is important and will help the module return a true value

__END__


