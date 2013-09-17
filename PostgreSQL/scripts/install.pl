#!/usr/bin/perl

# Install RecDB Server

use strict;
use warnings;

my $argc = (scalar @ARGV);

print "\nBeginning PostgreSQL installation.\n";

system "chmod", "0700", "configure";

print "Configuration in progress.\n";
if ($argc > 0) {
	system "./configure", "--prefix=$ARGV[0]", "--enable-debug";
} else {
	system "./configure", "--enable-debug";
}
my $rv = $? >> 8;
if ($rv > 0) {exit 1;}

print "Compiling PostgreSQL.\n";
my @makepg = `make`;
$rv = $? >> 8;
if ($rv > 0) {
	print "Compilation error.\n";
	exit 1;
} else {
	my $make_length = (scalar @makepg);
	print $makepg[($make_length - 1)];
}

print "Installing PostgreSQL.\n";
my @installpg = `make install`;
$rv = $? >> 8;
if ($rv > 0) {
	print "Installation error.\n";
	exit 1;
} else {
	my $install_length = (scalar @installpg);
	print $installpg[($install_length - 1)];
}

if ($argc > 0) {
	print "Creating vital directories.\n";
	system "mkdir", "data";

	print "Creating test database.\n";
	system "$ARGV[0]/bin/initdb", "-D", "data";

	# Writing out install path for later reference
	open FILE, ">", "install.properties" or die $!;
	print FILE $ARGV[0];
	close FILE or die $!;
} else {
	print "Creating vital directories.\n";
	system "mkdir", "data";

	print "Creating test database.\n";
	system "/usr/local/pgsql/bin/initdb", "-D", "data";

        # Writing out install path for later reference
        open FILE, ">", "install.properties" or die $!;
        print FILE "/usr/local/pgsql";
        close FILE or die $!;
}
